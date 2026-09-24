"""Explore RDF data with small, named sets of typed records."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict
from functools import cached_property
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from typing import Literal as FormatLiteral

import pandas as pd
from pydantic import BaseModel
from rdflib import BNode, Graph, Literal, URIRef

from rdfsolve.client.exploration import DatasetClient
from rdfsolve.client.hydration import _iri, _term, class_iri, field_metadata
from rdfsolve.client.model_rdf import model_to_graph
from rdfsolve.client.query_log import QueryLog
from rdfsolve.client.registry import Registry
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import LABEL_PREDICATES, NAME_PREDICATES, SYNONYM_PREDICATES
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.sparql_helper import EndpointError, SparqlHelper

if TYPE_CHECKING:
    from rdfsolve.client.catalogue import Catalogue
    from rdfsolve.client.ontology import OntologyLookup
    from rdfsolve.client.query import QueryResult
    from rdfsolve.client.query_fragments import PreparedQuery, QueryPattern


def _key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.casefold())


def _name(name: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name).replace("_", " ").capitalize()


def _name_fields(model: type[BaseModel]) -> list[str]:
    priority = [*LABEL_PREDICATES[2:], *LABEL_PREDICATES[:2], *SYNONYM_PREDICATES]
    fields = {
        name: field_metadata(info).get("rdf_property_iri", "")
        for name, info in model.model_fields.items()
    }
    return sorted(
        (name for name, iri in fields.items() if iri in NAME_PREDICATES),
        key=lambda name: priority.index(fields[name]),
    )


def _title(record: BaseModel) -> str:
    for field in _name_fields(type(record)):
        iri = field_metadata(type(record).model_fields[field]).get("rdf_property_iri", "")
        if SYNONYM_PREDICATES.get(iri) in {"broad", "narrow", "related"}:
            continue
        value = getattr(record, field)
        if value:
            return str(value[0] if isinstance(value, list) else value)
    return str(vars(record)["uri"])


class Client(DatasetClient):
    """Find records and types, then explore their links."""

    def __init__(
        self,
        *args: Any,
        ontology_grounding: bool | OntologyLookup = False,
        source_id: str = "rdf",
        class_mappings: Iterable[Any] = (),
        related_registries: Iterable[Registry] = (),
        **kwargs: Any,
    ) -> None:
        """Open typed retrieval with optional, separately retained ontology evidence."""
        super().__init__(*args, **kwargs)
        from rdfsolve.client.ontology import OntologyLookup

        self.ontology: OntologyLookup | None
        if ontology_grounding is True:
            self.ontology = OntologyLookup()
        elif not ontology_grounding:
            self.ontology = None
        elif isinstance(ontology_grounding, OntologyLookup):
            self.ontology = ontology_grounding
        else:
            raise TypeError("ontology_grounding must be a boolean or OntologyLookup")
        self.vocabulary_evidence: dict[str, dict[str, Any] | None] = {}
        self.source_id = source_id
        self.class_mappings = tuple(class_mappings)
        self.related_registries = tuple(related_registries)
        self._prepared: dict[str, tuple[PreparedQuery, Any, tuple[str, ...]]] = {}

    @cached_property
    def catalogue(self) -> Catalogue:
        """Retain this client's schema, mappings, entities and executable paths."""
        from rdfsolve.client.catalogue import Catalogue

        return Catalogue(
            self,
            self.source_id,
            class_mappings=self.class_mappings,
            related_registries=self.related_registries,
        )

    def vocabulary(self, iri: str) -> dict[str, Any] | None:
        """Explain a local vocabulary IRI through the configured ontology provider.

        External labels, definitions and named parents remain an evidence overlay.
        The mined schema, generated fields and source scope remain authoritative.
        """
        if iri not in self.vocabulary_evidence and self.ontology:
            evidence = self.ontology.lookup(iri)
            if evidence is not None or self.ontology.events[-1]["status"] == "not_found":
                self.vocabulary_evidence[iri] = evidence
        return self.vocabulary_evidence.get(iri)

    def _vocabulary_names(self, iri: str) -> set[str]:
        term = self.vocabulary_evidence.get(iri)
        names = {v.casefold() for v in [term["label"], *term["synonyms"]]} if term else set()
        return names | {
            item.text.value.casefold()
            for item in self._schema.enrichment.labels
            if item.term_iri == iri
            and (
                item.predicate in LABEL_PREDICATES
                or SYNONYM_PREDICATES.get(item.predicate) in {"exact", "alternative"}
            )
        }

    def close(self) -> None:
        """Close source and ontology connections."""
        super().close()
        if self.ontology:
            self.ontology.close()

    def session_metadata(self, *, include_results: bool = True) -> dict[str, Any]:
        """Retain external evidence separately from the mined schema and source journal."""
        data = super().session_metadata(include_results=include_results)
        if self.ontology:
            data["ontology"] = {
                **self.ontology.diagnostics(),
                "events": self.ontology.events,
                "evidence": self.vocabulary_evidence,
                "queries": [dict(vars(q)) for q in self.ontology.helper.get_collected_queries()]
                if self.ontology.helper
                else [],
            }
        data["prepared_queries"] = {ref: asdict(q) for ref, (q, _, _) in self._prepared.items()}
        return data

    @classmethod
    def open(
        cls,
        schema: MinedSchema | str | Path,
        source: str | SparqlHelper | Graph | None = None,
        *,
        format: FormatLiteral["json", "shacl", "void"] | None = None,
        data_file: str | Path | None = None,
        **kwargs: Any,
    ) -> Client:
        """Open a saved schema without mining or making source requests.

        JSON uses the canonical or VoID JSON-LD reader. For Turtle, choose
        format="shacl" or "void". RDF imports retain only supported fields.
        The source defaults to the schema endpoint. data_file selects local RDF.
        """
        if isinstance(schema, MinedSchema):
            if format is not None:
                raise ValueError("format applies to schema files, not MinedSchema objects")
        else:
            path = Path(schema)
            if format is None:
                if path.suffix.lower() not in {".json", ".jsonld"}:
                    raise ValueError('For Turtle, set format="shacl" or format="void"')
                format = "json"
            if format == "json":
                schema = MinedSchema.from_json(path)
            elif format == "shacl":
                schema = MinedSchema.from_shacl(path.read_text(encoding="utf-8"))
            elif format == "void":
                schema = MinedSchema.from_void(path.read_text(encoding="utf-8"))
            else:
                raise ValueError("Use json, shacl, or void as the schema format")
        if not schema.get_classes():
            raise ValueError("The schema has no classes to query; metadata alone is not enough")
        if data_file is not None:
            if source is not None:
                raise ValueError("Choose source or data_file, not both")
            if kwargs.get("graph_uris"):
                raise ValueError("A single local RDF graph has no named graph scope")
            source = Graph().parse(data_file)
            kwargs["graph_uris"] = []
        return cls(schema, source, **kwargs)

    def registry(self, *, source_id: str) -> Registry:
        """Describe generated classes and source bindings from retained metadata."""
        from rdfsolve.client.registry import build_registry

        return build_registry(self, source_id)

    def describe(
        self, concept: str = "", *, owners: Iterable[str] = (), targets: Iterable[str] = ()
    ) -> pd.DataFrame:
        """Find relevant generated classes and fields without reading instances."""
        from rdfsolve.schema_models.exporters.paths import path_to_sparql

        index = self.catalogue
        return pd.DataFrame(
            [
                {
                    "Reference": ref,
                    "Kind": f.kind,
                    "Label": f.label,
                    "Class": f.iri or f.owner,
                    "Field": f.field_name,
                    "Path": path_to_sparql(f.path) if f.path else None,
                    "Targets": index.metadata[ref].get("targets", []),
                    "Description": f.description,
                    "Ontology evidence": index.metadata[ref].get("ontology", []),
                }
                for ref in index.search(concept, owners=owners, targets=targets)
                for f in [index.fragments[ref]]
            ]
        )

    def prepare(
        self,
        sparql: str,
        *,
        requirements: Mapping[str, Any] | Iterable[Any] = (),
        grounding: Mapping[str, dict[str, Any]] | None = None,
        output_variables: Sequence[str] = (),
    ) -> PreparedQuery:
        """Prepare a scoped retrieval query using this client's retained evidence.

        Class, field, path and entity references returned by discovery expand
        here. Optional requirements describe outputs, relationships and filters;
        all queries are checked for source scope, field ownership and joins.
        output_variables requires the specified column names. Preparation makes
        no source request. Pass the artifact to select().
        """
        from rdfsolve.client.retrieval import verify_query

        entries = (
            requirements.items()
            if isinstance(requirements, dict)
            else ((f"g{i}", value) for i, value in enumerate(requirements, 1))
        )
        goals = {key: self.catalogue.requirement(value, output_variables) for key, value in entries}
        if grounding and not set(grounding) <= goals.keys():
            raise ValueError("Grounding must refer to a supplied requirement")
        with self.step("Prepare retrieval query"):
            query = verify_query(
                sparql,
                goals,
                grounding or {},
                self.catalogue,
                output_variables=output_variables,
            )
        self._prepared[query.ref] = query, self.source, tuple(self.graph_uris)
        return query

    def navigation(self, *, max_hops: int = 6, observed_only: bool = False) -> pd.DataFrame:
        """Retrieve retained elongated routes and their snapshot support summaries."""
        from rdfsolve.client.query_fragments import Fragment

        summary = self._schema.navigation
        rows = []
        for route in summary.paths if summary else []:
            if len(route.steps) > max_hops or (
                observed_only and route.instance_support != "matched"
            ):
                continue
            fragment = Fragment(
                "path",
                route.label(),
                path=route.property_path(),
                basis="mined joined path",
            )
            for i, step in enumerate(route.steps):
                target: str | None = step.object_class
                if target in {"Literal", "Resource", "BlankNode"}:
                    fragment.term_kinds[i + 1] = (target, step.datatype)
                    target = None
                fragment.steps.append((step.subject_class, step.property_uri, target, False))
            ref = self.catalogue._put(fragment, route.signature())
            self.catalogue.metadata[ref] = {
                "status": route.instance_support,
                "source_count": route.source_count,
                "matched_sources": route.matched_sources,
                "observed_at": route.observed_at,
            }
            rows.append(
                {
                    "Reference": ref,
                    "Label": route.label(),
                    "Hops": len(route.steps),
                    "Source": route.steps[0].subject_class,
                    "Target": route.steps[-1].object_class,
                    "Support": route.instance_support,
                    "Sources": route.source_count,
                    "Matched": route.matched_sources,
                }
            )
        return pd.DataFrame(
            rows,
            columns=[
                "Reference",
                "Label",
                "Hops",
                "Source",
                "Target",
                "Support",
                "Sources",
                "Matched",
            ],
        )

    def prepare_network(
        self,
        patterns: Sequence[QueryPattern | dict[str, Any]],
        *,
        outputs: Sequence[str],
        values: Mapping[str, str] | None = None,
        text: Mapping[str, str] | None = None,
        distinct: bool = True,
        requirements: Mapping[str, Any] | Iterable[Any] = (),
        grounding: Mapping[str, dict[str, Any]] | None = None,
    ) -> PreparedQuery:
        """Compose connected class, field and path selections into one verified query.

        Each QueryPattern names retained evidence and its roles. Reuse a role to
        share a node; expose every path port to constrain an intermediate record.
        Optional descendants remain inside their parent's optional scope.
        """
        from rdfsolve.client.query_fragments import network_query

        query = network_query(
            self.catalogue, patterns, outputs, values=values, text=text, distinct=distinct
        )
        return self.prepare(
            query, requirements=requirements, grounding=grounding, output_variables=outputs
        )

    def prepare_path(
        self,
        path: str,
        *,
        source: str = "source",
        target: str = "target",
        fields: dict[str, list[str]] | None = None,
        requirements: Mapping[str, Any] | Iterable[Any] = (),
        grounding: Mapping[str, dict[str, Any]] | None = None,
        output_variables: Sequence[str] = (),
    ) -> PreparedQuery:
        """Prepare a retained route, with optional fields on either endpoint.

        source and target name the result columns. fields maps either column to
        generated field names or unique labels. Entity anchors and graph scope
        come from the selected route. Preparation makes no source request.
        """
        from rdfsolve.client.query_fragments import path_query

        text = path_query(self.catalogue, path, source, target, fields or {})
        return self.prepare(
            text,
            requirements=requirements,
            grounding=grounding,
            output_variables=output_variables,
        )

    def retrieve(
        self,
        path: str,
        *,
        source: str = "source",
        target: str = "target",
        fields: dict[str, list[str]] | None = None,
        limit: int | None = None,
    ) -> QueryResult:
        """Retrieve a retained route and its available endpoint fields.

        Return exact RDF cells with their row associations. Supply limit for a
        bounded sample; the default retrieves all rows through the shared helper.
        """
        query = self.prepare_path(path, source=source, target=target, fields=fields)
        return self.select(query, exhaustive=limit is None, limit=limit)

    def select(
        self, query: PreparedQuery | str, *, exhaustive: bool = False, limit: int | None = None
    ) -> QueryResult:
        """Execute SELECT through this client's shared helper and return typed cells.

        Accept a prepared artifact from this client, or ordinary SPARQL with
        its scope already present. The returned QueryResult preserves the query's row associations and RDF terms.
        exhaustive=True uses the helper's adaptive pager until an empty page.
        limit bounds a probe without changing the prepared artifact.
        Endpoint execution uses the existing SparqlHelper.select_with_fallback.
        """
        from time import perf_counter

        from rdflib.plugins.sparql import prepareQuery

        from rdfsolve.client.query import QueryResult, ResultCell
        from rdfsolve.client.query_fragments import PreparedQuery, identifier

        if isinstance(query, PreparedQuery):
            retained = self._prepared.get(query.ref)
            if (
                retained is None
                or retained[0] is not query
                or retained[1] is not self.source
                or retained[2] != tuple(self.graph_uris)
                or query.ref != identifier("q", query.sparql)
            ):
                raise ValueError(
                    "Prepare this query with the current client and source scope before execution"
                )
            query = query.sparql
        if limit is not None:
            if type(limit) is not int or limit < 1:
                raise ValueError("Use a positive integer probe limit")
            query += f"\nLIMIT {limit}"
        parsed = prepareQuery(query)
        if parsed.algebra.name != "SelectQuery":
            raise ValueError("Client.select requires a SELECT query")
        variables = [str(v) for v in parsed.algebra.PV]
        started = perf_counter()
        bindings = self._select(query, exhaustive=True) if exhaustive else self._select(query)
        rows: list[dict[str, ResultCell]] = []
        for binding in bindings:
            row: dict[str, ResultCell] = {}
            for name, cell in binding.items():
                term = _term(cell)
                row[name] = ResultCell(
                    value=term.value, type=term.kind, lang=term.language, datatype=term.datatype
                )
            rows.append(row)
        return QueryResult(
            query=query,
            endpoint="" if isinstance(self.source, Graph) else self.source.endpoint_url,
            variables=variables,
            rows=rows,
            row_count=len(rows),
            duration_ms=round((perf_counter() - started) * 1000),
        )

    def field_values(
        self, kind: str, field: str, *, text: str = "", limit: int = 8, offset: int = 0
    ) -> list[dict[str, Any]]:
        """Sample actual subject/value pairs through a generated field's full path.

        Request limit+1 to detect another page. Blank-node identifiers belong
        to the response that supplied them.
        """
        from rdfsolve.client.exploration import _path
        from rdfsolve.schema_models.exporters.paths import path_to_sparql

        if type(limit) is not int or not 1 <= limit <= self.max_rows or offset < 0:
            raise ValueError("Use a positive bounded limit and nonnegative offset")
        model = self.model(kind)
        name = self.field_name(model, field)
        path = _path(model, name)
        pattern = f"?subject a {_iri(class_iri(model))} ; {path_to_sparql(path)} ?value ."
        if text:
            pattern += f" FILTER(!isBlank(?value) && CONTAINS(LCASE(STR(?value)), LCASE({Literal(text).n3()})))"
        return self._select(
            f"SELECT DISTINCT ?subject ?value WHERE {{ {self._scope(pattern)} }} "
            f"ORDER BY ?subject ?value LIMIT {limit} OFFSET {offset}"
        )

    def paths_between(
        self,
        source: str | BaseModel | Results,
        target: str | BaseModel | Results | None = None,
        *,
        target_value: str | None = None,
        max_hops: int = 3,
        both_directions: bool = True,
        max_paths: int = 1000,
        allow_partial: bool = False,
        allow_repeated_classes: bool = False,
        meaning: str = "",
        via: tuple[str, ...] = (),
    ) -> pd.DataFrame:
        """Discover schema routes or evaluate paths for selected typed records.

        Two class names use retained model fields without querying. A record or
        Results on either side evaluates paths for those exact identities in each
        configured graph. target_value searches names with find first. Results
        preserve their whole selected scope; no example record is substituted.
        allow_partial returns bounded evidence with explicit coverage.
        """
        from rdfsolve.client.paths import class_paths
        from rdfsolve.client.value_paths import value_paths

        if (target is None) == (target_value is None):
            raise ValueError("Supply either a target class or target_value")
        if isinstance(source, str) and isinstance(target, str):
            table = class_paths(
                self,
                source,
                target,
                max_hops=max_hops,
                both_directions=both_directions,
                max_paths=max_paths,
                allow_partial=allow_partial,
                allow_repeated_classes=allow_repeated_classes,
                meaning=meaning,
                via=via,
            )
            self.catalogue.retain_paths(table)
            return table

        def endpoints(value: Any) -> dict[str, list[str] | None]:
            """Group class names, records or results by their RDF class."""
            if isinstance(value, str):
                return {class_iri(self.model(value)): None}
            records = Results(self, [value]) if isinstance(value, BaseModel) else value
            if not isinstance(records, Results) or records.client is not self:
                raise ValueError(
                    "Use a class name, a generated record, or Results from this Client"
                )
            groups: defaultdict[str, set[str]] = defaultdict(set)
            for record in records:
                if type(record) not in self.models.values():
                    raise ValueError("Use records generated by this Client")
                uri = str(vars(record)["uri"])
                _iri(uri)
                groups[class_iri(record)].add(uri)
            return {cls: sorted(iris) for cls, iris in sorted(groups.items())}

        sources = endpoints(source)
        target_records = self.find(target_value) if target_value is not None else target
        table = value_paths(
            self,
            sources,
            endpoints(target_records),
            max_hops=max_hops,
            both_directions=both_directions,
            max_paths=max_paths,
            allow_partial=allow_partial,
            allow_repeated_classes=allow_repeated_classes,
            meaning=meaning,
            via=via,
        )
        table.attrs["target_value"] = target_value
        selection_partial = any(
            isinstance(v, Results) and v.coverage.get("status") == "partial"
            for v in (source, target_records)
        )
        if selection_partial:
            table.attrs.update(status="partial", truncated=True)
            table.attrs["warnings"].append(
                "The input selection is partial; connections cover only its retained identities."
            )
        self.catalogue.retain_paths(table)
        return table

    def connections(
        self,
        source: str | BaseModel,
        target: str | BaseModel | None = None,
        *,
        max_hops: int = 3,
        both_directions: bool = True,
        max_paths: int | None = None,
    ) -> pd.DataFrame:
        """Find actual paths between records, or around one record.

        Show every intermediate resource and link. Paths do not repeat resources
        and stay in one graph. Requests run in sequence. By default, stop at
        the client's row budget and return a marked partial view with a warning.
        Set max_paths explicitly to require a strict limit and raise on overflow.
        Set both_directions=False to follow outgoing links only.
        Without a target, return linked resources within max_hops. Do not follow
        rdf:type links or literal values; show classes as node annotations.
        """
        from rdfsolve.client.paths import resource_paths

        return resource_paths(
            self,
            source,
            target,
            max_hops=max_hops,
            both_directions=both_directions,
            max_paths=max_paths,
        )

    def diagram(
        self,
        *kinds: str,
        paths: pd.DataFrame | None = None,
        path: int | None = None,
        instances: bool = True,
    ) -> str:
        """Draw selected models or the selected rows of a paths table, without queries."""
        from rdfsolve.client.diagram import model_diagram, path_diagram

        if paths is not None:
            if kinds:
                raise ValueError("Choose model names or a paths table, not both")
            return path_diagram(self, paths, path=path, instances=instances)
        if path is not None:
            raise ValueError("Supply a paths table to choose a path")
        return model_diagram(self, kinds)

    def query_log(self) -> QueryLog:
        """Show every session query and its retained response without running it again."""
        return QueryLog(self.session_metadata())

    @staticmethod
    def title(record: BaseModel) -> str:
        """Read a loaded title or label, falling back to the exact resource IRI."""
        return _title(record)

    def trace(self) -> dict[str, Any]:
        """Explain the investigation using named steps and retained query identifiers."""
        records = self._records()
        return {
            "source": "local RDF" if isinstance(self.source, Graph) else self.source.endpoint_url,
            "graphs": list(self.graph_uris),
            "source_queries": len(records),
            "endpoint_requests": sum(r.attempts for r in records),
            "steps": [dict(step) for step in self._steps],
            "ontology": self.ontology.diagnostics() if self.ontology else {"enabled": False},
        }

    def model(self, name_or_iri: str) -> type[BaseModel]:
        """Accept generated names, spaced names, or full class IRIs."""
        if name_or_iri in self.models:
            return self.models[name_or_iri]
        matches = [
            model
            for name, model in self.models.items()
            if _key(name_or_iri)
            in {
                _key(name),
                _key(re.split(r"[/#:]", getattr(model, "rdf_class_iri", ""))[-1]),
                _key(self.type_name(model)),
                "".join(w[0] for w in self.type_name(model).split()).casefold(),
            }
            or getattr(model, "rdf_class_iri", "") == name_or_iri
        ]
        if not matches:
            from rdfsolve.client.catalogue import words

            matches = [
                model
                for model in self.models.values()
                if words(name_or_iri) == words(self.type_name(model))
            ]
        if not matches:
            matches = [
                model
                for model in self.models.values()
                if name_or_iri.casefold() in self._vocabulary_names(class_iri(model))
            ]
        if len(matches) != 1:
            names = sorted(self.models)
            candidates = [name for name in names if _key(name_or_iri) in _key(name)] or names
            raise ValueError(
                f"Unknown or ambiguous class {name_or_iri!r}. Use an exact class name or IRI. "
                f"Available class names: {', '.join(candidates[:8])}"
            )
        return matches[0]

    def type_name(self, model: type[BaseModel]) -> str:
        """Display a source label without changing the generated type."""
        iri = getattr(model, "rdf_class_iri", "")
        labels = [
            item.text.value
            for item in self._schema.enrichment.labels
            if item.term_iri == iri
            and item.text.language in (None, "en")
            and item.predicate in LABEL_PREDICATES
        ]
        if labels:
            return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", min(labels))
        local = re.split(r"[/#:]", iri)[-1]
        return local if re.search(r"\d", local) else _name(local)

    def link_name(self, model: type[BaseModel], field: str) -> str:
        """Use a source label or readable predicate name for a link."""
        extra = model.model_fields[field].json_schema_extra
        if isinstance(extra, dict):
            iri = str(extra.get("rdf_property_iri", ""))
            labels = [
                item.text.value
                for item in self._schema.enrichment.labels
                if item.term_iri == iri
                and item.text.language in (None, "en")
                and item.predicate in LABEL_PREDICATES
            ]
            if labels:
                return min(labels)
            if iri:
                return _name(re.split(r"[/#:]", iri)[-1])
        return _name(field)

    def field_name(self, model: type[BaseModel], text: str) -> str:
        """Resolve a field by its Python name, source label or exact predicate IRI."""
        if text in model.model_fields:
            return text
        matches = [
            name
            for name, field in model.model_fields.items()
            if _key(text) in {_key(name), _key(self.link_name(model, name))}
            or (
                isinstance(field.json_schema_extra, dict)
                and field.json_schema_extra.get("rdf_property_iri") == text
            )
        ]
        if not matches:
            matches = [
                name
                for name, field in model.model_fields.items()
                if isinstance(field.json_schema_extra, dict)
                and text.casefold()
                in self._vocabulary_names(str(field_metadata(field).get("rdf_property_iri", "")))
            ]
        if len(matches) != 1:
            names = [
                name
                for name, field in model.model_fields.items()
                if isinstance(field.json_schema_extra, dict)
                and field.json_schema_extra.get("rdf_path")
            ]
            raise ValueError(
                f"Unknown or ambiguous field {text!r} for {self.type_name(model)}. "
                f"Available fields: {', '.join(names)}"
            )
        return matches[0]

    def create(
        self,
        kind: str,
        *,
        uri: str | BNode | None = None,
        blank_node_scope: str | None = None,
        **values: Any,
    ) -> BaseModel:
        """Create a typed record. Omit uri for a new blank node; pass RDF literals as values."""
        from rdfsolve.client.authoring import create_record

        model = self.model(kind)
        fields = {self.field_name(model, field): value for field, value in values.items()}
        return create_record(model, fields, uri=uri, blank_node_scope=blank_node_scope)

    def from_table(
        self,
        kind: str,
        table: pd.DataFrame,
        *,
        id_column: str | None = None,
        languages: Mapping[str, str] | None = None,
        datatypes: Mapping[str, str] | None = None,
        blank_node_scope: str | None = None,
        **columns: str,
    ) -> Results:
        """Create records from columns, with optional literal defaults keyed by field.

        Omit id_column to create distinct blank nodes. Explicit RDF terms in cells
        retain their language and datatype. Errors identify the input row.
        """
        from rdfsolve.client.authoring import create_record, table_literal

        model = self.model(kind)
        missing = ({id_column} if id_column is not None else set()) | set(columns.values())
        missing -= set(table.columns)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")
        fields = {self.field_name(model, field): column for field, column in columns.items()}
        languages = {self.field_name(model, k): v for k, v in (languages or {}).items()}
        datatypes = {self.field_name(model, k): v for k, v in (datatypes or {}).items()}
        if (languages.keys() | datatypes.keys()) - fields.keys():
            raise ValueError("Literal defaults require a mapped field")
        records = []
        for position, (index, row) in enumerate(table.iterrows()):
            try:
                identifier = row[id_column] if id_column is not None else None
                if id_column is not None and not isinstance(identifier, (str, BNode)):
                    raise ValueError("The identifier column must contain IRIs or blank nodes")
                values = {
                    field: table_literal(
                        row[column], language=languages.get(field), datatype=datatypes.get(field)
                    )
                    for field, column in fields.items()
                }
                records.append(
                    create_record(model, values, uri=identifier, blank_node_scope=blank_node_scope)
                )
            except (TypeError, ValueError) as error:
                raise ValueError(f"row {position} (index {index!r}): {error}") from error
        return Results(self, records)

    def types(self) -> pd.DataFrame:
        """List available record types without sending a query."""
        return pd.DataFrame(
            {"Class": sorted(self.type_name(model) for model in self.models.values())}
        )

    @classmethod
    def from_session(
        cls, path: str | Path, *, data_file: str | Path | None = None, **kwargs: Any
    ) -> Client:
        """Reuse a saved session's schema. Do not replay its queries."""
        session = json.loads(Path(path).read_text(encoding="utf-8"))
        if data_file is not None:
            kwargs["source"] = Graph().parse(data_file)
            kwargs["graph_uris"] = []
        return cls(MinedSchema.from_dict(session["schema"]), **kwargs)

    def find(
        self,
        text: str,
        *,
        kind: str | None = None,
        field: str | None = None,
        allow_partial: bool = False,
    ) -> Results:
        """Find names and identifiers, optionally retaining a marked partial selection."""
        from rdfsolve.client.search import search_records

        result = search_records(
            self,
            [text],
            kind,
            [field] if field else [],
            names_only=True,
            allow_partial=allow_partial,
        )
        if result or not self.ontology or field:
            return result
        import bioregistry

        from rdfsolve.client.ontology import term_key

        prefix = bioregistry.parse_iri(class_iri(self.model(kind)))[0] if kind else None
        ontology = bioregistry.get_ols_prefix(prefix) if prefix else None
        looked_up = [
            self.vocabulary(c["iri"]) for c in self.ontology.search(text, ontology=ontology)
        ]
        candidates = [
            c
            for c in looked_up
            if c is not None
            and text.casefold() in {v.casefold() for v in [c["label"], *c["synonyms"]]}
        ]
        aliases = list(dict.fromkeys(c.get("label", "") for c in candidates))
        aliases = [a for a in aliases if a and len(a) <= 200 and a.casefold() != text.casefold()]
        if not aliases:
            return result
        found = search_records(
            self, aliases[:12], kind, [], names_only=True, allow_partial=allow_partial
        )
        keys = {term_key(c["iri"]) for c in candidates}
        records = [r for r in found if term_key(str(vars(r)["uri"])) in keys]
        identities = {str(vars(r)["uri"]) for r in records}
        evidence = [e for e in found.evidence if e["id"] in identities]
        return Results(
            self,
            records,
            evidence=evidence,
            coverage={
                **found.coverage,
                "terms": [text],
                "ontology_candidates": candidates,
                "identity_check": "exact IRI or registered namespace and identifier",
            },
        )

    def search(
        self, terms: Sequence[str], *, kind: str | None = None, fields: Sequence[str] | None = None
    ) -> Results:
        """Find text candidates and keep matching passages in result.evidence.

        Search any phrase in names, identifiers and mined text fields. Supply fields
        to narrow the search. This does not infer synonyms or scientific relevance.
        """
        from rdfsolve.client.search import search_records

        return search_records(self, terms, kind, fields or [])

    def save(self, path: str | Path, *groups: Results) -> None:
        """Save selected records and direct links between them as RDF."""
        records = [record for group in groups for record in group]
        if any(group.client is not self for group in groups):
            raise ValueError("Save results from one client at a time")
        graph = Graph()
        self._schema.bind_prefixes(graph)
        ids = {str(vars(record)["uri"]) for record in records}
        for record in records:
            graph += model_to_graph(record)
        for link in self._matches:
            if link["source"] not in ids or link["target"] not in ids:
                continue
            path_model = PropertyPath.model_validate(link["path"])
            source, target = URIRef(link["source"]), URIRef(link["target"])
            if path_model.operator == "inverse":
                source, target = target, source
                path_model = path_model.items[0]
            if path_model.operator != "predicate" or path_model.iri is None:
                raise ValueError("Select the intermediate records before saving a multi-step link")
            graph.add((source, URIRef(path_model.iri), target))
        graph.serialize(destination=path, format="turtle")


class Results:
    """A reusable set of generated records. Display and completion do not query."""

    def __init__(
        self,
        client: Client,
        records: list[BaseModel],
        *,
        evidence: list[dict[str, Any]] | None = None,
        coverage: dict[str, Any] | None = None,
    ) -> None:
        """Keep the client and typed records together."""
        self.client = client
        self.records = records
        self.evidence = evidence or []
        self.coverage = coverage or {"status": "complete", "basis": "Retrieved records"}
        self.references = client.catalogue.retain_records(self)

    def __len__(self) -> int:
        """Return the number of typed records."""
        return len(self.records)

    def __iter__(self) -> Iterator[BaseModel]:
        """Iterate over the generated Pydantic objects."""
        return iter(self.records)

    def __getitem__(self, index: int) -> BaseModel:
        """Read a generated object, with its usual field completion."""
        return self.records[index]

    def __repr__(self) -> str:
        """Preview names without fetching more fields."""
        return f"{len(self)} matches\n" + self._table().head(20).to_string(index=False)

    def _repr_html_(self) -> str:
        note = " · showing the first 20" if len(self) > 20 else ""
        return f"<p>{len(self)} matches{note}</p>" + self._table().head(20).to_html(
            index=False, escape=True
        )

    def _table(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"Name": _title(record), "Class": self.client.type_name(type(record))}
                for record in self.records
            ],
            columns=["Name", "Class"],
        )

    def summary(self) -> dict[str, Any]:
        """Describe the selected set and loaded fields without exposing record values."""
        counts: defaultdict[str, int] = defaultdict(int)
        fields: defaultdict[str, set[str]] = defaultdict(set)
        for record in self.records:
            kind = self.client.type_name(type(record))
            counts[kind] += 1
            fields[kind].update(vars(record).get("rdf_loaded_fields", []))
        return {
            "records": len(self),
            "types": [
                {"class": kind, "records": count, "loaded_fields": sorted(fields[kind])}
                for kind, count in counts.items()
            ],
            "coverage": {
                k: self.coverage[k]
                for k in ("status", "basis", "query_ids", "limit_reached")
                if k in self.coverage
            },
            "scope": "All records in this retained selection",
        }

    def paths_between(self, target: Any, **kwargs: Any) -> pd.DataFrame:
        """Evaluate paths from every selected record through this set's Client."""
        return self.client.paths_between(self, target, **kwargs)

    def types(self) -> pd.DataFrame:
        """List the types found and their record counts."""
        groups: dict[type[BaseModel], list[BaseModel]] = defaultdict(list)
        for record in self.records:
            groups[type(record)].append(record)
        return pd.DataFrame(
            [
                {
                    "Class": self.client.type_name(model),
                    "Matches": len(records),
                    "Example": _title(records[0]),
                }
                for model, records in groups.items()
            ],
            columns=["Class", "Matches", "Example"],
        )

    def of_type(self, kind: str) -> Results:
        """Keep matches of one type without sending a query."""
        model = self.client.model(kind)
        return Results(
            self.client,
            [r for r in self.records if type(r) is model],
            evidence=self.evidence,
            coverage=self.coverage,
        )

    def without(self, other: Results) -> Results:
        """Remove records with IRIs already present in another result set."""
        ids = {vars(record)["uri"] for record in other}
        return Results(
            self.client,
            [r for r in self.records if vars(r)["uri"] not in ids],
            evidence=self.evidence,
            coverage=self.coverage,
        )

    @property
    def fields(self) -> SimpleNamespace:
        """Offer field names for tab completion, without fetching their values."""
        names = {
            name
            for record in self.records
            for name, info in type(record).model_fields.items()
            if isinstance(info.json_schema_extra, dict) and info.json_schema_extra.get("rdf_path")
        }
        aliases = {name: name for name in sorted(names)}
        for record in self.records:
            for name in names & type(record).model_fields.keys():
                label = re.sub(
                    r"\W+", "_", self.client.link_name(type(record), name).lower()
                ).strip("_")
                if label.isidentifier() and label not in aliases:
                    aliases[label] = name
        return SimpleNamespace(**aliases)

    def _routes(self, incoming: bool) -> list[tuple[type[BaseModel], str, type[BaseModel]]]:
        routes = []
        models = {type(record) for record in self.records}
        for source in self.client.models.values() if incoming else models:
            for row in self.client.links(source).itertuples(index=False):
                target = self.client.model(str(row.target))
                if not incoming or target in models:
                    routes.append(
                        (target, str(row.field), source)
                        if incoming
                        else (source, str(row.field), target)
                    )
        return routes

    def paths(self, *, incoming: bool = False) -> pd.DataFrame:
        """Show the types and named links available from these records."""
        return pd.DataFrame(
            [
                {
                    "From": self.client.type_name(source),
                    "Link": self.client.link_name(target if incoming else source, field),
                    "To": self.client.type_name(target),
                }
                for source, field, target in self._routes(incoming)
            ],
            columns=["From", "Link", "To"],
        ).drop_duplicates()

    def related(
        self,
        kind: str | None = None,
        *,
        via: str | None = None,
        value: str | None = None,
        incoming: bool = False,
    ) -> Results:
        """Follow a link or an intermediate class, optionally matching a name or identifier.

        A class in via means two hops. A link name selects one direct link.
        Text is a case-insensitive substring, tested only on connected targets.
        """
        if kind is None and value is None:
            raise ValueError("Choose kind or value")
        if value is not None and not value.strip():
            raise ValueError("Enter a word or name to find")
        if via is not None:
            try:
                intermediate = self.client.model(via)
            except ValueError:
                intermediate = None
            if intermediate is not None:
                middle = self.related(intermediate.__name__, incoming=incoming)
                return middle.related(kind, value=value, incoming=incoming)
        if kind is None:
            targets = sorted({target.__name__ for _, _, target in self._routes(incoming)})
            records: list[BaseModel] = []
            for name in targets:
                records.extend(self.related(name, via=via, value=value, incoming=incoming))
            return Results(self.client, records)
        target = self.client.model(kind)
        routes = [
            (source, field)
            for source, field, dest in self._routes(incoming)
            if dest is target
            and (
                via is None
                or _key(via)
                in {_key(field), _key(self.client.link_name(dest if incoming else source, field))}
            )
        ]
        by_source: dict[type[BaseModel], set[str]] = defaultdict(set)
        for source, field in routes:
            by_source[source].add(field)
        if value is None and any(len(fields) > 1 for fields in by_source.values()):
            choices = sorted(
                {
                    self.client.link_name(target if incoming else source, field)
                    for source, field in routes
                }
            )
            raise ValueError(f"Choose one link with via= from: {choices}")
        if self.records and not routes:
            raise ValueError("No matching link. Use paths() to see where these records can lead.")
        found: dict[str, BaseModel] = {}
        with self.client.step(
            f"Read related {self.client.type_name(target)}"
            + (f" matching {value}" if value is not None else "")
        ):
            for source, fields in by_source.items():
                records = [record for record in self.records if type(record) is source]
                for field in sorted(fields):
                    for record in self.client.follow(
                        records,
                        field,
                        target,
                        inverse=incoming,
                        fields=_name_fields(target),
                        value=value,
                    ):
                        found[str(vars(record)["uri"])] = record
        return Results(
            self.client,
            list(found.values()),
            evidence=[
                link
                for link in self.client._matches
                if link["query_id"] in self.client._steps[-1]["query_ids"]
            ],
            coverage={
                "status": self.coverage.get("status", "complete"),
                "basis": "Related resources from the whole selected set",
                "source_records": len(self),
                "via": via,
                "query_ids": self.client._steps[-1]["query_ids"],
            },
        )

    def _load(self, *fields: str) -> None:
        fields = tuple(field for field in fields if field != "uri")
        if not fields:
            return
        models = {type(record) for record in self.records}
        names = {
            model: {self.client.field_name(model, field) for field in fields} for model in models
        }
        with self.client.step(f"Read {', '.join(fields)}"):
            for model, requested in names.items():
                group = [
                    r
                    for r in self.records
                    if type(r) is model
                    and requested - set(vars(r).get("rdf_loaded_fields", [])) - r.model_fields_set
                ]
                if not group:
                    continue
                selected = set(_name_fields(model)) | requested
                selected.update(
                    field for r in group for field in vars(r).get("rdf_loaded_fields", [])
                )
                loaded = self.client.get_many(
                    model, [str(vars(r)["uri"]) for r in group], fields=sorted(selected)
                )
                replacements = {vars(r)["uri"]: r for r in loaded}
                self.records = [
                    replacements.get(vars(r)["uri"], r) if type(r) is model else r
                    for r in self.records
                ]

    def show(self, *fields: str) -> pd.DataFrame:
        """Show names and requested fields, retrieving those fields if needed."""
        self._load(*fields)
        rows = []
        for record in self.records:
            row: dict[str, Any] = {
                "Name": _title(record),
                "Class": self.client.type_name(type(record)),
            }
            for field in fields:
                name = self.client.field_name(type(record), field)
                value = getattr(record, name)
                row[_name(field)] = (
                    " | ".join(map(str, value)) if isinstance(value, list) else value
                )
            rows.append(row)
        return pd.DataFrame(rows, columns=["Name", "Class", *(_name(f) for f in fields)])

    def table(self, *fields: str) -> pd.DataFrame:
        """Return RDF term lists and typed records; load requested fields if needed.

        With no fields, use only what has been read. NA means unread or inapplicable;
        an empty list means the field was read but no value was returned.
        attrs retains records, session queries with original bindings, and links.
        """
        from rdfsolve.client.table import record_table

        self._load(*fields)
        models = {type(record) for record in self.records}
        names = sorted(
            {self.client.field_name(model, field) for model in models for field in fields}
        )
        metadata = self.client.session_metadata()
        return record_table(
            self.records,
            labels={
                str(getattr(model, "rdf_class_iri", "")): self.client.type_name(model)
                for model in models
            },
            fields=names if fields else None,
            context={
                "queries": metadata["queries"],
                "links": metadata["links"],
                "evidence": self.evidence,
                "coverage": self.coverage,
            },
        )

    def values(self, field: str) -> pd.DataFrame:
        """List values of one field across the selected records."""
        self._load(field)
        values: dict[str, set[str]] = defaultdict(set)
        for record in self.records:
            name = self.client.field_name(type(record), field)
            terms = vars(record).get("rdf_terms", {}).get(name)
            if terms is None:
                from rdfsolve.schema_models.enrichment import RdfTerm

                extra = type(record).model_fields[name].json_schema_extra
                if not isinstance(extra, dict) or not extra.get("rdf_property_iri"):
                    raise ValueError("No direct RDF field definition")
                graph = model_to_graph(record, fields=[name])
                terms = [
                    RdfTerm.from_rdf(value).model_dump(mode="json")
                    for value in graph.objects(
                        URIRef(vars(record)["uri"]), URIRef(str(extra["rdf_property_iri"]))
                    )
                ]
            for term in terms:
                values[json.dumps(term, sort_keys=True)].add(str(vars(record)["uri"]))
        rows = []
        for raw, subjects in values.items():
            term = json.loads(raw)
            rows.append(
                {
                    "Value": term["value"],
                    "Records": len(subjects),
                    "Language": term.get("language"),
                    "Datatype": term.get("datatype"),
                }
            )
        result = pd.DataFrame(rows, columns=["Value", "Records", "Language"])
        result.attrs["rdf_terms"] = [json.loads(raw) for raw in values]
        return result.dropna(axis=1, how="all")


def explore(endpoint: str, *, graph: str | None = None, timeout: float = 30) -> Client:
    """Read an endpoint's schema and open a query-recording client."""
    from rdfsolve.mining.miner import SchemaMiner

    miner = SchemaMiner(
        endpoint,
        graph_uris=[graph] if graph else None,
        counts=False,
        enrich=True,
        examples_per_pattern=0,
        timeout=timeout,
    )
    miner.helper.enable_query_collection(include_results=True)
    try:
        schema = miner.mine()
        if miner.last_report is None or miner.last_report.completion_state != "complete":
            raise EndpointError("Could not finish reading the available fields")
        client = Client(schema, miner.helper, max_subjects=500)
        client._owns_helper = True
        return client
    except BaseException:
        miner.close()
        raise
