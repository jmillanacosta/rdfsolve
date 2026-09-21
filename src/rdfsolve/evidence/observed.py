"""Observed structural evidence derived from mined RDF instance data."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, computed_field

from rdfsolve.mining.query_fallbacks import query_with_bisect
from rdfsolve.sparql_helper import SparqlHelper

CompletionState = Literal["complete", "partial", "failed", "not_run"]


class MeasurementState(BaseModel):
    """Completion state of one measurement and why it ran."""

    status: CompletionState
    purpose: str
    failures: list[str] = Field(default_factory=list)


class ClassPopulationEvidence(BaseModel):
    """Class population used as the denominator for property-support evidence."""

    class_iri: str
    graph_scope: list[str] = Field(default_factory=list)
    scope_semantics: Literal["endpoint_default_graph", "rdf_merge_selected_graphs"]
    subject_count: int | None = Field(default=None, ge=0)
    count_status: Literal["available", "missing"]
    source_method: str = "class_entity_counts"


class PropertyUsageEvidence(BaseModel):
    """Class/property usage over one dataset graph scope.

    Counts are evaluated over the RDF merge of explicitly selected named graphs,
    or over the endpoint default graph when no graph scope is supplied.  This
    intentionally differs from graph-attributed pattern counts, where the same
    triple occurring in two named graphs can contribute once to each graph.
    """

    subject_class: str
    property_uri: str
    graph_scope: list[str] = Field(default_factory=list)
    scope_semantics: Literal["endpoint_default_graph", "rdf_merge_selected_graphs"]

    eligible_subjects: int | None = Field(default=None, ge=0)
    denominator_state: Literal["available", "missing"] = "missing"
    subjects_with_property: int | None = Field(default=None, ge=0)
    triple_count: int | None = Field(default=None, ge=0)
    distinct_objects: int | None = Field(default=None, ge=0)

    summary_state: MeasurementState
    node_kind_counts: dict[str, int] = Field(default_factory=dict)
    node_kind_state: MeasurementState | None = None
    datatype_counts: dict[str, int] = Field(default_factory=dict)
    language_counts: dict[str, int] = Field(default_factory=dict)
    datatype_state: MeasurementState | None = None
    value_count_histogram: dict[str, int] | None = None
    histogram_state: MeasurementState | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def support_fraction(self) -> float | None:
        """Return subjects with the property over eligible subjects, or None when unknown."""
        if self.eligible_subjects in (None, 0) or self.subjects_with_property is None:
            return None
        return self.subjects_with_property / self.eligible_subjects


class PropertyUsageCollection(BaseModel):
    """Property usage measured for one dataset over a stated graph scope."""

    dataset_id: str
    graph_scope: list[str] = Field(default_factory=list)
    scope_semantics: Literal["endpoint_default_graph", "rdf_merge_selected_graphs"]
    class_populations: list[ClassPopulationEvidence] = Field(default_factory=list)
    records: list[PropertyUsageEvidence] = Field(default_factory=list)
    batch_states: list[MeasurementState] = Field(default_factory=list)


def _values_block(classes: list[str]) -> str:
    return "VALUES ?class { " + " ".join(f"<{iri}>" for iri in classes) + " }"


def _dataset_clause(graph_uris: list[str] | None) -> str:
    if not graph_uris:
        return ""
    # SPARQL FROM constructs an RDF merge of the selected graphs. Duplicate
    # triples present in several named graphs therefore occur once in this
    # dataset-scope support measurement.
    return " ".join(f"FROM <{iri}>" for iri in graph_uris)


def build_property_usage_query(
    classes: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Build class/property support query over the selected dataset scope."""
    query = f"""\
SELECT ?class ?p
       (COUNT(*) AS ?triples)
       (COUNT(DISTINCT ?s) AS ?subjects)
       (COUNT(DISTINCT ?o) AS ?objects)
{_dataset_clause(graph_uris)}
WHERE {{
  {_values_block(classes)}
  ?s a ?class .
  ?s ?p ?o .
  FILTER(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
}}
GROUP BY ?class ?p"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(query)
    return query


def build_node_kind_query(
    classes: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Count IRI/literal/blank-node values per class/property."""
    query = f"""\
SELECT ?class ?p ?kind (COUNT(*) AS ?values)
{_dataset_clause(graph_uris)}
WHERE {{
  {_values_block(classes)}
  ?s a ?class .
  ?s ?p ?o .
  FILTER(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
  BIND(IF(isIRI(?o), "IRI", IF(isBlank(?o), "BlankNode", "Literal")) AS ?kind)
}}
GROUP BY ?class ?p ?kind"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(query)
    return query


def build_literal_profile_query(
    classes: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Count literal datatype and language-tag observations."""
    query = f"""\
SELECT ?class ?p ?datatype ?lang (COUNT(*) AS ?values)
{_dataset_clause(graph_uris)}
WHERE {{
  {_values_block(classes)}
  ?s a ?class .
  ?s ?p ?o .
  FILTER(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
  FILTER(isLiteral(?o))
  BIND(DATATYPE(?o) AS ?datatype)
  BIND(LANG(?o) AS ?lang)
}}
GROUP BY ?class ?p ?datatype ?lang"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(query)
    return query


def build_value_count_histogram_query(
    classes: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Count subjects by distinct value cardinality for each class/property."""
    query = f"""\
SELECT ?class ?p ?n (COUNT(*) AS ?subjects)
{_dataset_clause(graph_uris)}
WHERE {{
  {{
    SELECT ?class ?p ?s (COUNT(DISTINCT ?o) AS ?n)
    WHERE {{
      {_values_block(classes)}
      ?s a ?class .
      ?s ?p ?o .
      FILTER(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
    }}
    GROUP BY ?class ?p ?s
  }}
}}
GROUP BY ?class ?p ?n"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(query)
    return query


def _histogram_bucket(value_count: int) -> str:
    if value_count <= 5:
        return str(value_count)
    if value_count <= 10:
        return "6-10"
    if value_count <= 100:
        return "11-100"
    return "101+"


def _state_from_outcome(outcome: Any, purpose: str) -> MeasurementState:
    return MeasurementState(
        status=outcome.state,
        purpose=purpose,
        failures=[f"{item.category}: {item.message}" for item in outcome.failures],
    )


def _detail_outcome(
    *,
    batch: list[str],
    graph_scope: list[str],
    build_fn: Any,
    purpose: str,
    helper: SparqlHelper,
    chunk_size: int,
) -> Any:
    return query_with_bisect(
        batch,
        graph_scope or None,
        build_fn,
        purpose,
        helper,
        lambda q, p, size: _collect_pages(helper, q, p, size or chunk_size),
        chunk_size,
        False,
    )


def _collect_pages(
    helper: SparqlHelper, query: str, purpose: str, chunk_size: int
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for chunk in helper.select_chunked(
        query,
        chunk_size=chunk_size,
        purpose=purpose,
        pagination="offset",
    ):
        rows.extend(chunk)
    return rows


def collect_property_usage_evidence(
    *,
    dataset_id: str,
    classes: list[str],
    class_entity_counts: dict[str, int] | None,
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    batch_size: int = 10,
    chunk_size: int = 10_000,
    collect_node_kinds: bool = True,
    collect_datatypes: bool = True,
    collect_histograms: bool = False,
) -> PropertyUsageCollection:
    """Collect subject-level class/property support with bounded query fallback."""
    graph_scope = list(graph_uris or [])
    semantics: Literal["endpoint_default_graph", "rdf_merge_selected_graphs"] = (
        "rdf_merge_selected_graphs" if graph_scope else "endpoint_default_graph"
    )
    denominator = class_entity_counts or {}
    class_populations = [
        ClassPopulationEvidence(
            class_iri=class_iri,
            graph_scope=graph_scope,
            scope_semantics=semantics,
            subject_count=denominator.get(class_iri),
            count_status="available" if class_iri in denominator else "missing",
        )
        for class_iri in sorted(set(classes))
    ]
    records: list[PropertyUsageEvidence] = []
    states: list[MeasurementState] = []

    for offset in range(0, len(classes), max(1, batch_size)):
        batch = classes[offset : offset + max(1, batch_size)]
        outcome = query_with_bisect(
            batch,
            graph_scope or None,
            build_property_usage_query,
            "evidence/property-usage",
            helper,
            lambda q, purpose, size: _collect_pages(helper, q, purpose, size or chunk_size),
            chunk_size,
            False,
        )
        state = _state_from_outcome(outcome, "evidence/property-usage")
        states.append(state)
        batch_records: dict[tuple[str, str], PropertyUsageEvidence] = {}
        for row in outcome.rows:
            class_iri = row.get("class", {}).get("value")
            prop_iri = row.get("p", {}).get("value")
            if not class_iri or not prop_iri:
                continue
            try:
                triples = int(row["triples"]["value"])
                subjects = int(row["subjects"]["value"])
                objects = int(row["objects"]["value"])
            except (KeyError, TypeError, ValueError):
                continue
            eligible = denominator.get(class_iri)
            record = PropertyUsageEvidence(
                subject_class=class_iri,
                property_uri=prop_iri,
                graph_scope=graph_scope,
                scope_semantics=semantics,
                eligible_subjects=eligible,
                denominator_state="available" if eligible is not None else "missing",
                subjects_with_property=subjects,
                triple_count=triples,
                distinct_objects=objects,
                summary_state=state,
            )
            batch_records[(class_iri, prop_iri)] = record

        if collect_node_kinds and batch_records:
            detail = _detail_outcome(
                batch=batch,
                graph_scope=graph_scope,
                build_fn=build_node_kind_query,
                purpose="evidence/property-node-kind",
                helper=helper,
                chunk_size=chunk_size,
            )
            detail_state = _state_from_outcome(detail, "evidence/property-node-kind")
            states.append(detail_state)
            for record in batch_records.values():
                record.node_kind_state = detail_state.model_copy(deep=True)
            for row in detail.rows:
                key = (row.get("class", {}).get("value"), row.get("p", {}).get("value"))
                detail_record = batch_records.get(key)
                kind = row.get("kind", {}).get("value")
                try:
                    count = int(row["values"]["value"])
                except (KeyError, TypeError, ValueError):
                    continue
                if detail_record is not None and kind in {"IRI", "Literal", "BlankNode"}:
                    detail_record.node_kind_counts[kind] = count

        if collect_datatypes and batch_records:
            detail = _detail_outcome(
                batch=batch,
                graph_scope=graph_scope,
                build_fn=build_literal_profile_query,
                purpose="evidence/property-literal-profile",
                helper=helper,
                chunk_size=chunk_size,
            )
            detail_state = _state_from_outcome(detail, "evidence/property-literal-profile")
            states.append(detail_state)
            for record in batch_records.values():
                record.datatype_state = detail_state.model_copy(deep=True)
            for row in detail.rows:
                key = (row.get("class", {}).get("value"), row.get("p", {}).get("value"))
                detail_record = batch_records.get(key)
                if detail_record is None:
                    continue
                datatype = row.get("datatype", {}).get("value") or "untyped-literal"
                lang = row.get("lang", {}).get("value") or ""
                try:
                    count = int(row["values"]["value"])
                except (KeyError, TypeError, ValueError):
                    continue
                detail_record.datatype_counts[datatype] = (
                    detail_record.datatype_counts.get(datatype, 0) + count
                )
                if lang:
                    detail_record.language_counts[lang] = (
                        detail_record.language_counts.get(lang, 0) + count
                    )

        if collect_histograms and batch_records:
            detail = _detail_outcome(
                batch=batch,
                graph_scope=graph_scope,
                build_fn=build_value_count_histogram_query,
                purpose="evidence/property-value-count-histogram",
                helper=helper,
                chunk_size=chunk_size,
            )
            detail_state = _state_from_outcome(detail, "evidence/property-value-count-histogram")
            states.append(detail_state)
            exact: dict[tuple[str, str], dict[str, int]] = {}
            nonzero_subjects: dict[tuple[str, str], int] = {}
            for row in detail.rows:
                key = (row.get("class", {}).get("value"), row.get("p", {}).get("value"))
                if key not in batch_records:
                    continue
                try:
                    n = int(row["n"]["value"])
                    subject_count = int(row["subjects"]["value"])
                except (KeyError, TypeError, ValueError):
                    continue
                bucket = _histogram_bucket(n)
                exact.setdefault(key, {})[bucket] = (
                    exact.setdefault(key, {}).get(bucket, 0) + subject_count
                )
                nonzero_subjects[key] = nonzero_subjects.get(key, 0) + subject_count
            for key, record in batch_records.items():
                record.histogram_state = detail_state.model_copy(deep=True)
                histogram = dict(exact.get(key, {}))
                if detail_state.status == "complete" and record.eligible_subjects is not None:
                    zero = record.eligible_subjects - nonzero_subjects.get(key, 0)
                    if zero >= 0:
                        histogram["0"] = zero
                    else:
                        record.histogram_state.status = "partial"
                        record.histogram_state.failures.append(
                            "invalid_response: histogram subjects exceed class denominator"
                        )
                record.value_count_histogram = histogram or None

        records.extend(batch_records.values())

    records.sort(key=lambda row: (row.subject_class, row.property_uri))
    return PropertyUsageCollection(
        dataset_id=dataset_id,
        graph_scope=graph_scope,
        scope_semantics=semantics,
        class_populations=class_populations,
        records=records,
        batch_states=states,
    )


__all__ = [
    "ClassPopulationEvidence",
    "MeasurementState",
    "PropertyUsageCollection",
    "PropertyUsageEvidence",
    "build_literal_profile_query",
    "build_node_kind_query",
    "build_property_usage_query",
    "build_value_count_histogram_query",
    "collect_property_usage_evidence",
]
