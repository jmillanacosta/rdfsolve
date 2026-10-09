"""Property-level patterns of the IRI subjects that have no type, mined through an endpoint.

Class-based mining starts from the members of classes, so data without rdf:type has no
pattern at all (STRING's main graph: 453 M triples, no type). Here each property is asked
for the edges whose subject is an IRI without a type, with the count queries of the typed
patterns (query_builders, UntypedSubjects in place of the class): the patterns
(rdfs:Resource, p, class / Resource / Literal datatype / BlankNode) have subject_binding
"untyped" and the same counts as typed patterns (triples, distinct subjects, distinct objects,
by edge graph). A subject with any type, in the data or type context graphs, is never one of
them. Blank-node subjects are left out: they are reached from the patterns of the nodes that
point to them (blank_node_predicates) and from the structural patterns.

The properties are chosen without a query of their own when the structural census counted
the triples of untyped subjects of each property (StructuralStrategy, which runs before): a
property with none is skipped, and a property whose triples are all of untyped subjects is
counted without the test of each subject (only FILTER(isIRI(?s))). Otherwise the properties
are listed and each is probed for one untyped subject under a short time limit; the probes of
an endpoint that runs past the limit five times in a row stop (QueryCuts), and each property
not probed is a measurement gap. A count refused for a property that has untyped subjects is
asked over a sample of its edges (rdfsolve.mining.sampling): the rows of the sample are kept,
flagged sampled, with counts that are lower bounds. Only a count whose samples are refused too
is a failure: the schema then misses rows that exist.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from rdfsolve._outcomes import FailureCategory, QueryFailure, QueryOutcome
from rdfsolve.mining.query_builders import (
    MEMBERSHIP,
    UntypedSubjects,
    _build_batched_blank_node_count_query,
    _build_batched_blank_node_query,
    _build_batched_literal_count_query,
    _build_batched_literal_objects_query,
    _build_batched_typed_count_query,
    _build_batched_untyped_count_query,
    _graph_scope,
    _type_pattern,
)
from rdfsolve.schema_models._constants import UNTYPED_SUBJECT
from rdfsolve.schema_models.pattern import PatternType, SchemaPattern
from rdfsolve.sparql_helper import (
    EndpointTimeoutError,
    QueryCuts,
    SparqlHelper,
    SparqlHelperError,
)

if TYPE_CHECKING:
    from rdfsolve.mining.strategy import MiningContext

logger = logging.getLogger(__name__)

__all__ = ["PROBE_SECONDS", "mine_untyped_subjects", "untyped_census"]

# The time limit of the probe of one property (as the light steps of VoID-first mining).
PROBE_SECONDS = 30.0


def untyped_census(context: MiningContext) -> dict[str, dict[str, int] | None]:
    """Return what the structural census counted for each property: its triples and the
    triples of its untyped subjects, summed over the graphs; None for a property whose
    untyped triples were not counted (a refused census). A property whose census was refused
    but whose sample showed untyped subjects is marked "sampled" (it has some, not every
    subject is known to be untyped). Empty without a census.
    """
    coverage = context.report.report.config.get("structural_coverage") or []
    found: dict[str, dict[str, int]] = {}
    refused: set[str] = set()
    sampled: set[str] = set()
    for entry in coverage:
        for predicate, counts in (entry.get("census_properties") or {}).items():
            if "untypedTriples" not in counts and (counts.get("sampled") or {}).get(
                "untypedTriples"
            ):
                # Untyped subjects seen in a sample of a refused census: they exist.
                sampled.add(predicate)
                continue
            if "untypedTriples" not in counts:
                refused.add(predicate)
                continue
            total = found.setdefault(predicate, {"triples": 0, "untyped": 0})
            total["triples"] += int(counts.get("triples") or 0)
            total["untyped"] += int(counts["untypedTriples"])
    seen = {"triples": 0, "untyped": 0, "sampled": 1}
    return {
        p: None if p in refused else seen if p in sampled - found.keys() else found.get(p)
        for p in found.keys() | refused | sampled
    }


def _listed_properties(context: MiningContext) -> list[str] | None:
    """List the properties of the selected data; None when the endpoint refuses."""
    dataset, opening, closing = _graph_scope(context.graph_uris, context.type_context_graph_uris)
    query = SparqlHelper.prepare_paginated_query(
        f"SELECT DISTINCT ?p {dataset} WHERE {{ {opening} ?s ?p ?o . {closing} }}"
    )
    try:
        rows = context.collect_bindings(query, "untyped-subjects/properties", context.chunk_size)
    except SparqlHelperError as error:
        failure = QueryFailure(
            "endpoint", str(error)[:300], "untyped-subjects/properties", [], context.graph_uris
        )
        context.report.record_outcome(QueryOutcome(state="complete", gaps=[failure]))
        return None
    return sorted({r["p"]["value"] for r in rows if r.get("p", {}).get("type") == "uri"})


def _probe(
    context: MiningContext,
    predicate: str,
    cuts: QueryCuts,
    seconds: float,
    gaps: list[QueryFailure],
) -> bool | None:
    """Ask for one untyped IRI subject of *predicate*; None when not answered."""
    purpose = "untyped-subjects/probe"
    if cuts.skip(purpose):
        gaps.append(
            QueryFailure(
                "timeout",
                f"{predicate}: untyped subjects not checked ({cuts.stopped(purpose)})",
                purpose,
                [UNTYPED_SUBJECT],
                context.graph_uris,
            )
        )
        return None
    dataset, opening, closing = _graph_scope(context.graph_uris, context.type_context_graph_uris)
    test = _type_pattern(
        "?s", UntypedSubjects(edge=f"?s <{predicate}> ?o ."), context.type_context_graph_uris
    )
    query = (
        f"SELECT ?s {dataset} WHERE {{ {opening} ?s <{predicate}> ?o . {closing} {test} }} LIMIT 1"
    )
    started = time.monotonic()
    try:
        with context.helper.budget(seconds):
            rows = context.helper.select(query, purpose=purpose)["results"]["bindings"]
    except (SparqlHelperError, KeyError) as error:
        context.report.record_query(purpose, time.monotonic() - started, success=False)
        if isinstance(error, SparqlHelperError):
            cuts.failed(purpose, error)
        category: FailureCategory = (
            "timeout" if isinstance(error, EndpointTimeoutError) else "endpoint"
        )
        gaps.append(
            QueryFailure(
                category,
                f"{predicate}: untyped subjects not checked: {str(error)[:200]}",
                purpose,
                [UNTYPED_SUBJECT],
                context.graph_uris,
            )
        )
        return None
    context.report.record_query(purpose, time.monotonic() - started)
    cuts.answered(purpose)
    return bool(rows)


def _candidates(
    context: MiningContext, seconds: float, record: dict[str, Any], gaps: list[QueryFailure]
) -> list[tuple[str, UntypedSubjects]]:
    """Return (property, marker) for each property with untyped subjects."""
    membership = set(MEMBERSHIP.get())
    census = untyped_census(context)
    listed = sorted(census) if census else _listed_properties(context)
    if listed is None:
        record["state"] = "not_checked"
        return []
    chosen: list[tuple[str, UntypedSubjects]] = []
    cuts = QueryCuts(client_timeouts=True)
    probed = 0
    for predicate in listed:
        if predicate in membership:
            continue
        counted = census.get(predicate)
        if counted is not None and counted.get("sampled"):
            chosen.append((predicate, UntypedSubjects()))
            continue
        if counted is not None:
            if counted["untyped"]:
                every = counted["untyped"] == counted["triples"]
                chosen.append((predicate, UntypedSubjects(every=every)))
            continue
        probed += 1
        if _probe(context, predicate, cuts, seconds, gaps):
            chosen.append((predicate, UntypedSubjects()))
    record.update(
        properties=len(listed),
        selected_by="structural census" if census else "probe",
        probed=probed,
        with_untyped_subjects=[p for p, _ in chosen],
    )
    stopped = cuts.record()
    if stopped:
        record["stopped"] = stopped
    return chosen


def _count_property(
    context: MiningContext, predicate: str, marker: UntypedSubjects
) -> tuple[list[SchemaPattern], QueryOutcome]:
    """Count the patterns of the untyped subjects of one property."""
    from rdfsolve.mining.pattern_enrichment import (
        _count_from_binding,
        _PatternCount,
        apply_counts,
    )
    from rdfsolve.mining.property_queries import query_by_property
    from rdfsolve.mining.sampling import flag, sample_of, sampled_select

    graphs, ctx = context.graph_uris, context.type_context_graph_uris
    counts: dict[tuple[str, str | None], dict[str, _PatternCount]] = defaultdict(dict)
    outcome = QueryOutcome()

    def run(builder: Any, purpose: str) -> list[dict[str, Any]]:
        """Run one count builder for the property, with the fallbacks of query_by_property."""
        nonlocal outcome
        started = time.monotonic()
        found = query_by_property(
            marker,
            graphs,
            builder,
            f"untyped-subjects/{purpose}",
            context.helper,
            context.collect_bindings,
            context.chunk_size,
            ctx,
            properties=[predicate],
        )
        context.report.record_query(
            f"untyped-subjects/{purpose}",
            time.monotonic() - started,
            success=found.state == "complete",
        )
        outcome = outcome.merge(found)
        return found.rows

    def keep(rows: list[dict[str, Any]], kind: str | None) -> None:
        """Keep the counts of rows of one object kind (None: the object class of each row)."""
        for row in rows:
            metric = _count_from_binding(row)
            # A grouped count without solutions can answer one row of zeros (RDFLib).
            if metric is None or not metric.triples:
                continue
            if kind is None:
                obj = row.get("oc", {})
                if obj.get("type") != "uri":
                    continue
                key: tuple[str, str | None] = (obj["value"], None)
            elif kind == "Literal":
                key = ("Literal", row.get("dt", {}).get("value"))
            else:
                key = (kind, None)
            counts[key][row.get("_g", {}).get("value", "")] = metric

    keep(run(_build_batched_typed_count_query, "typed-object"), None)
    keep(run(_build_batched_literal_count_query, "literal"), "Literal")
    keep(run(_build_batched_untyped_count_query, "untyped-uri"), "Resource")
    keep(run(_build_batched_blank_node_count_query, "blank-node"), "BlankNode")
    if any(obj == "Literal" for obj, _ in counts):
        for row in run(_build_batched_literal_objects_query, "literal-objects"):
            key = ("Literal", row.get("dt", {}).get("value"))
            graph = row.get("_g", {}).get("value", "")
            value = row.get("objects", {}).get("value")
            metric = counts.get(key, {}).get(graph)
            # Distinct objects of a sample are not given to a count of all edges.
            if metric is not None and value is not None and (metric.sample or not sample_of(row)):
                counts[key][graph] = replace(metric, distinct_objects=int(value))
    blank: list[str] | None = None
    if any(obj == "BlankNode" for obj, _ in counts):
        query = _build_batched_blank_node_query(
            [marker], graphs, type_context_graph_uris=ctx, property_uri=predicate
        )
        from rdfsolve.mining.query_fallbacks import select_outcome

        found = select_outcome(
            query, "untyped-subjects/blank-node-predicates", context.helper, [marker], graphs
        )
        found = sampled_select(
            lambda size: _build_batched_blank_node_query(
                [marker], graphs, type_context_graph_uris=ctx, property_uri=predicate, sample=size
            ),
            "untyped-subjects/blank-node-predicates",
            context.helper,
            found,
            unit="edges",
            classes=[marker],
            graph_uris=graphs,
            property_uri=predicate,
        )
        outcome = outcome.merge(found)
        blank = sorted({r["bnPred"]["value"] for r in found.rows if "bnPred" in r}) or None
    patterns = []
    for (obj, datatype), per_graph in sorted(
        counts.items(), key=lambda i: (i[0][0], i[0][1] or "")
    ):
        seed = SchemaPattern(
            subject_class=UNTYPED_SUBJECT,
            subject_binding="untyped",
            property_uri=predicate,
            object_class=obj,
            datatype=datatype,
            blank_node_predicates=blank if obj == "BlankNode" else None,
            pattern_type={
                "Literal": PatternType.DATATYPE_PROPERTY,
                "BlankNode": PatternType.BLANK_NODE_PROPERTY,
            }.get(obj, PatternType.OBJECT_PROPERTY),
        )
        pattern = apply_counts(seed, per_graph, graphs)
        if pattern.sampled is not None:
            # The count queries of untyped subjects are their discovery too: the sample decided
            # the rows of the property as well as their counts.
            flag(pattern, pattern.sampled.model_dump(), ["patterns", "counts"])
        patterns.append(pattern)
    return patterns, outcome


def mine_untyped_subjects(
    context: MiningContext, *, probe_seconds: float = PROBE_SECONDS
) -> list[SchemaPattern]:
    """Return the patterns of the IRI subjects without a type, with their counts.

    The choice of properties, the probes, the patterns and the gaps are recorded in the
    report (config "untyped_subjects").
    """
    phase = context.report.start_phase("untyped-subjects")
    record: dict[str, Any] = {"state": "complete"}
    context.report.report.config["untyped_subjects"] = record
    gaps: list[QueryFailure] = []
    patterns: list[SchemaPattern] = []
    try:
        for predicate, marker in _candidates(context, probe_seconds, record, gaps):
            logger.info("Untyped subjects: counting %s", predicate)
            found, outcome = _count_property(context, predicate, marker)
            if outcome.state != "complete":
                record["state"] = "partial"
            if outcome.samples:
                record.setdefault("sampled", []).append(predicate)
            if not found and outcome.state == "complete":
                # Untyped subjects that are all blank nodes, which these patterns leave out.
                record.setdefault("blank_subjects_only", []).append(predicate)
            context.report.record_outcome(outcome)
            patterns.extend(found)
    except Exception as error:
        context.report.finish_phase(phase, error=str(error))
        raise
    if gaps:
        record["not_checked"] = len(gaps)
        if record["state"] == "complete":
            record["state"] = "partial"
        context.report.record_outcome(QueryOutcome(state="complete", gaps=gaps))
    record["patterns"] = len(patterns)
    context.report.finish_phase(phase, items=len(patterns))
    return patterns
