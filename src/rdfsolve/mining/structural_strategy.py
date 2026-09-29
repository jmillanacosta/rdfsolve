"""Mine graph-local shapes for edges absent from typed profiles."""

from __future__ import annotations

import json
import logging
import time
from collections import Counter, defaultdict
from collections.abc import Sequence
from typing import Any

from rdflib import Literal, URIRef

from rdfsolve._outcomes import QueryFailure, QueryOutcome
from rdfsolve.mining.local_graph import LocalGraphHelper
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

logger = logging.getLogger(__name__)
from rdfsolve.mining.typed_coverage import typed_match, uncovered_filter
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.structural import StructuralPattern
from rdfsolve.sparql_helper import EndpointTimeoutError


def _dataset(graph: str | None, type_graphs: list[str] | None = None) -> str:
    if graph is None:
        return ""
    return " ".join(
        ([f"FROM <{graph}>"] if graph else []) + [f"FROM NAMED <{g}>" for g in type_graphs or []]
    )


def _types(graphs: list[str] | None) -> str:
    if not graphs:
        return "?s a ?_type ."
    values = " ".join(f"<{g}>" for g in graphs)
    return f"VALUES ?_typeGraph {{ {values} }} GRAPH ?_typeGraph {{ ?s a ?_type }}"


def _untyped(graphs: list[str] | None) -> str:
    return f"FILTER NOT EXISTS {{ {_types(graphs)} }}"


def _discovery_query(
    graph: str | None,
    named_graphs: list[str],
    residual: str,
    *,
    include_edges: bool = False,
) -> str:
    edges = "?s ?o " if include_edges else ""
    edge_pattern = f"?s ?p ?o . {residual}"
    if include_edges:
        edge_pattern = f"{{ SELECT ?s ?p ?o WHERE {{ {edge_pattern} }} }}"
    return f"""SELECT DISTINCT {edges}?ss ?os ?p ?sk ?ok ?dt ?lang
{_dataset(graph, named_graphs)} WHERE {{
  {edge_pattern}
  {{ SELECT ?s (GROUP_CONCAT(DISTINCT STR(?sp); SEPARATOR=" ") AS ?ss)
     WHERE {{ ?s ?sp ?sv }} GROUP BY ?s }}
  OPTIONAL {{
    {{ SELECT ?o (GROUP_CONCAT(DISTINCT STR(?op); SEPARATOR=" ") AS ?os)
       WHERE {{ ?o ?op ?ov }} GROUP BY ?o }}
  }}
  BIND(IF(isBlank(?s), "BlankNode", "IRI") AS ?sk)
  BIND(IF(isBlank(?o), "BlankNode", IF(isLiteral(?o), "Literal", "IRI")) AS ?ok)
  BIND(DATATYPE(?o) AS ?dt)
  BIND(LANG(?o) AS ?lang)
}}"""


def _shape(node: str, properties: list[str], kind: str, *, exact: bool = True) -> str:
    test = {"IRI": "isIRI", "BlankNode": "isBlank", "Literal": "isLiteral"}[kind]
    clauses = [f"FILTER({test}({node}))"]
    if kind == "Literal" or not exact:
        return " ".join(clauses)
    for prop in properties:
        clauses.append(f"FILTER EXISTS {{ {node} <{prop}> ?_value }}")
    exclusion = (
        "FILTER(?_property NOT IN (" + ", ".join(f"<{p}>" for p in properties) + "))"
        if properties
        else ""
    )
    clauses.append(f"FILTER NOT EXISTS {{ {node} ?_property ?_value . {exclusion} }}")
    return " ".join(clauses)


def structural_queries(pattern: StructuralPattern) -> tuple[str, str]:
    """Build an exact recount and witness for the recorded profile and scope."""
    exact = pattern.shape_semantics == "exact_property_sets"
    clauses = [
        f"VALUES ?p {{ <{pattern.property_uri}> }} ?s ?p ?o .",
        _shape("?s", pattern.subject_properties, pattern.subject_kind, exact=exact),
        _shape("?o", pattern.object_properties, pattern.object_kind, exact=exact),
    ]
    if pattern.subject_selection == "untyped":
        clauses.append(_untyped(pattern.type_graph_uris))
    if pattern.subject_selection == "uncovered":
        keys = [(s, pattern.property_uri, o, dt) for s, o, dt in pattern.covered_types]
        clauses.append(
            uncovered_filter(keys, pattern.type_graph_uris, pattern.object_type_graph_uris)
        )
    if pattern.datatype:
        clauses.append(f"FILTER(DATATYPE(?o) = <{pattern.datatype}>)")
    if pattern.language is not None:
        clauses.append(f"FILTER(LANG(?o) = {Literal(pattern.language).n3()})")
    named = list(dict.fromkeys(pattern.type_graph_uris + pattern.object_type_graph_uris))
    body = f"{_dataset(pattern.graph_uri, named)} WHERE {{ {' '.join(clauses)} }}"
    return (
        f"SELECT ?s ?o {body} LIMIT 1",
        (
            "SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?s) AS ?subjects) "
            f"(COUNT(DISTINCT ?o) AS ?objects) {body}"
        ),
    )


def _select(
    context: MiningContext, query: str, purpose: str, *, paged: bool = True
) -> list[dict[str, Any]]:
    """Run a SELECT and record it; *paged* allows the helper to page a refused query."""
    started = time.monotonic()
    try:
        if paged:
            response = context.helper.select_with_fallback(query, purpose=purpose)
        else:
            response = context.helper.select(query, purpose=purpose)
        rows: list[dict[str, Any]] = response["results"]["bindings"]
    except Exception:
        context.report.record_query(purpose, time.monotonic() - started, success=False)
        raise
    context.report.record_query(purpose, time.monotonic() - started)
    return rows


# Triples of one property in one census batch, when the census of the property is refused.
CENSUS_BATCH_TRIPLES = 20_000_000
# Fewest triples per object for a census in batches of objects: with fewer, a batch names about
# as many objects as it has triples (SIBiLS pattern#contains: one triple per object).
CENSUS_MIN_TRIPLES_PER_OBJECT = 10


class CensusRefusedError(Exception):
    """The census of a property was refused and cannot be split into batches of objects."""

    def __init__(self, triples: int | None, reason: str) -> None:
        """Keep the triple count of the property, when it is known."""
        super().__init__(reason)
        self.triples = triples


def _census_queries(
    graph: str | None,
    named: list[str],
    match: str,
    local: bool,
    predicate: str | None = None,
    restriction: str = "",
) -> list[str]:
    """Count all triples, the triples of untyped subjects and, remotely, the uncovered triples.

    Each count filters the edges. No count is grouped by a value that BIND(EXISTS ...) sets:
    Virtuoso gives wrong counts for that form (AOP-Wiki prov:used: 1 of 2 edges covered),
    whereas the same test in a FILTER counts 2 of 2, as a query of each edge confirms. For one
    property, the typed test repeats the edge and its restriction: QLever evaluates the group
    of EXISTS on its own before the join, and a group of only ``?s a ?_type`` reads every type
    triple of the graph for every batch (Bgee: 455.7 GB for each batch of RO_0002206).
    """
    edge = f"?s <{predicate}> ?o . {restriction}" if predicate else "?s ?p ?o ."
    typed = f"{edge} {_types(named)}" if predicate else _types(named)
    tests = {"triples": "", "untypedTriples": f"FILTER NOT EXISTS {{ {typed} }}"}
    # The uncovered edges are counted with the negated test, the filter of the discovery, and the
    # covered edges follow by subtraction: Rhea counts 550,753 covered rdf:type edges of 550,634
    # with FILTER(test) (duplicates), and 0 uncovered with FILTER(!test). Without typed profiles
    # every edge is uncovered and no filter is sent, because Rhea answers COUNT(*) with
    # FILTER(false) with no row.
    if not local:
        tests["uncoveredTriples"] = "" if match == "false" else f"FILTER(!{match})"
    return [
        f"SELECT (COUNT(*) AS ?{name}) {_dataset(graph, named)} WHERE {{ {edge} {test} }}"
        for name, test in tests.items()
    ]


def _count(context: MiningContext, queries: list[str]) -> Counter[str]:
    """Run the census queries; each returns one count."""
    counts: Counter[str] = Counter()
    for query in queries:
        (row,) = _select(context, query, "structural/coverage", paged=False)
        counts.update({name: int(binding["value"]) for name, binding in row.items()})
    return counts


def _census(
    context: MiningContext,
    graph: str | None,
    named: list[str],
    keys: list[tuple[str, str, str, str | None]],
    local: bool,
    entry: dict[str, Any],
) -> Counter[str]:
    """Count a local graph at once, and a graph behind an endpoint one property at a time.

    Through an endpoint only the one-property test is used, the form that was checked against a
    query of each edge (Virtuoso) and against the earlier nested form (QLever). Virtuoso evaluates
    the whole-graph test, with ?p a variable, wrongly and without an error (AOP-Wiki: the
    whole-graph discovery gave 9 rows for 8 properties, the discovery of virtrdf:item alone 41),
    and the whole-graph test of a large graph joins every triple with every typed profile (Bgee:
    no answer in 2 h). The counts of each property are recorded; the discovery of uncovered
    edges uses them. A census is not paged: each query returns one count.
    """
    if local:
        match = typed_match(keys, context.graph_uris, context.type_context_graph_uris)
        counts = _count(context, _census_queries(graph, named, match, local))
        entry["census"] = "whole_graph"
        return counts
    listing = f"SELECT DISTINCT ?p {_dataset(graph, named)} WHERE {{ ?s ?p ?o }}"
    predicates = sorted(r["p"]["value"] for r in _select(context, listing, "structural/properties"))
    logger.info("Census: counting %d properties one at a time", len(predicates))
    counts = Counter[str]()
    per_property: dict[str, dict[str, Any]] = {}
    for number, predicate in enumerate(predicates, start=1):
        logger.info("Census: property %d/%d %s", number, len(predicates), predicate)
        own = [key for key in keys if key[1] == predicate]
        try:
            found = _property_census(context, graph, named, own, local, predicate, entry)
        except CensusRefusedError as refused:
            logger.warning("Census: %s not counted: %s", predicate, refused)
            failure = QueryFailure("timeout", f"{predicate}: {refused}", "structural/coverage")
            context.report.record_outcome(QueryOutcome(state="partial", failures=[failure]))
            per_property[predicate] = {"triples": refused.triples, "refused": str(refused)}
            counts.update(triples=refused.triples or 0, uncheckedTriples=refused.triples or 0)
            continue
        per_property[predicate] = {
            name: found[name] for name in ("triples", "untypedTriples", "uncoveredTriples")
        }
        counts.update(found)
    entry["census"] = "per_property"
    entry["census_properties"] = per_property
    return counts


def _object_term(binding: dict[str, Any]) -> str:
    """Write an IRI or literal of a SPARQL JSON result as a SPARQL term."""
    if binding["type"] == "uri":
        return URIRef(binding["value"]).n3()
    language, datatype = binding.get("xml:lang"), binding.get("datatype")
    return Literal(binding["value"], lang=language, datatype=datatype).n3()


def _property_census(
    context: MiningContext,
    graph: str | None,
    named: list[str],
    keys: list[tuple[str, str, str, str | None]],
    local: bool,
    predicate: str,
    entry: dict[str, Any],
) -> Counter[str]:
    """Count one property; when the endpoint refuses, count it in batches of its objects.

    The objects are grouped into batches of about CENSUS_BATCH_TRIPLES triples, and a batch
    that the endpoint refuses is split in two. FILTER(?o IN ...) restricts the query and its
    coverage group, so that each reads only the edges of the batch (Bgee RO_0002206:
    813,735,712 triples, 127,021 objects; one query needs more memory than a node has). The
    batch is not a VALUES: QLever evaluates an EXISTS group with a VALUES of 768 objects wrongly
    (all 19,999,769 edges untyped; 0 with FILTER IN). Blank-node objects cannot be listed and
    are counted together. The counts of the batches add up to the counts of the property.
    """

    def count(restriction: str) -> Counter[str]:
        """Count the edges of the property that *restriction* selects."""
        match = typed_match(
            keys, context.graph_uris, context.type_context_graph_uris, predicate, restriction
        )
        return _count(context, _census_queries(graph, named, match, local, predicate, restriction))

    try:
        return count("")
    except EndpointTimeoutError:
        pass
    size_query = (
        f"SELECT (COUNT(*) AS ?triples) (COUNT(DISTINCT ?o) AS ?objects) {_dataset(graph, named)} "
        f"WHERE {{ ?s <{predicate}> ?o }}"
    )
    try:
        (row,) = _select(context, size_query, "structural/objects", paged=False)
    except EndpointTimeoutError as error:
        raise CensusRefusedError(None, f"census and size refused: {error}") from error
    triples, distinct = int(row["triples"]["value"]), int(row["objects"]["value"])
    if triples < CENSUS_MIN_TRIPLES_PER_OBJECT * distinct:
        raise CensusRefusedError(
            triples,
            f"census refused; {triples} triples over {distinct} objects are too few per object"
            " for batches of objects",
        )
    listing = (
        f"SELECT ?o (COUNT(*) AS ?n) {_dataset(graph, named)} "
        f"WHERE {{ ?s <{predicate}> ?o }} GROUP BY ?o"
    )
    objects, blank = [], False
    for row in _select(context, listing, "structural/objects"):
        if row["o"]["type"] == "bnode":
            blank = True
        else:
            objects.append((_object_term(row["o"]), int(row["n"]["value"])))
    pending: list[list[str]] = [[]]
    size = 0
    for term, triples in sorted(objects):
        if pending[-1] and size + triples > CENSUS_BATCH_TRIPLES:
            pending.append([])
            size = 0
        pending[-1].append(term)
        size += triples
    pending = [batch for batch in pending if batch]
    logger.info(
        "Census: %s refused; counting %d objects in %d batches",
        predicate,
        len(objects),
        len(pending),
    )
    counts: Counter[str] = Counter()
    batches = 0
    while pending:
        batch = pending.pop()
        try:
            counts.update(count(f"FILTER(?o IN ({', '.join(batch)}))"))
            batches += 1
        except EndpointTimeoutError:
            if len(batch) == 1:
                raise
            logger.info(
                "Census: %s batch of %d objects refused; split in two", predicate, len(batch)
            )
            pending += [batch[len(batch) // 2 :], batch[: len(batch) // 2]]
    if blank:
        counts.update(count("FILTER(isBlank(?o))"))
        batches += 1
    entry.setdefault("census_batches", {})[predicate] = batches
    return counts


class StructuralStrategy(MiningStrategy):
    """Add exact structural evidence only for edges absent from typed profiles."""

    def __init__(self, patterns: list[SchemaPattern] | None = None) -> None:
        """Use supplied observations or discover typed profiles first."""
        self.patterns = patterns

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "structural"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Measure typed coverage and mine only uncovered edges."""
        patterns = self.patterns if self.patterns is not None else TwoPhaseStrategy().mine(context)
        if context.report.report.abort_reason or context.report.report.query_failures:
            context.report.report.config["structural_coverage"] = [
                {"state": "not_checked", "reason": "typed_mining_incomplete"}
            ]
            return patterns
        keys = sorted(
            {
                (p.subject_class, p.property_uri, p.object_class, p.datatype)
                for p in patterns
                if p.subject_binding == p.object_binding == "type" and p.evidence_source == "mined"
            },
            key=str,
        )
        local = isinstance(context.helper, LocalGraphHelper)
        context.report.report.config["structural_execution"] = (
            "local_bulk" if local else "per_profile_queries"
        )
        observations: dict[str | None, list[dict[str, Any]]] = {}
        phase = context.report.start_phase("graph-census" if local else "typed-coverage")
        coverage: list[dict[str, Any]] = []
        context.report.report.config["structural_coverage"] = coverage
        graphs: Sequence[str | None] = (
            sorted(set(context.graph_uris)) if context.graph_uris else [None]
        )
        named = list(
            dict.fromkeys((context.graph_uris or []) + (context.type_context_graph_uris or []))
        )
        match = typed_match(keys, context.graph_uris, context.type_context_graph_uris)
        for graph in graphs:
            entry: dict[str, Any] = {"graph_uri": graph, "state": "checking"}
            coverage.append(entry)
            context.report.flush()
            try:
                counts = _census(context, graph, named, keys, local, entry)
                total, untyped = counts["triples"], counts["untypedTriples"]
                if local:
                    entry.update(triple_count=total, untyped_subject_triples=untyped)
                    continue
                missing, unchecked = counts["uncoveredTriples"], counts["uncheckedTriples"]
                covered = total - missing - unchecked
                entry.update(
                    triple_count=total,
                    untyped_subject_triples=untyped,
                    excluded_subject_triples=0,
                    covered_triples=covered,
                    uncovered_triples=missing,
                    unchecked_triples=unchecked,
                    type_graph_uris=named,
                    subject_selection="uncovered",
                    state="needed" if missing else ("partial" if unchecked else "not_needed"),
                )
                if unchecked:
                    entry["census_refused"] = sorted(
                        prop for prop, n in entry["census_properties"].items() if "refused" in n
                    )
            except Exception:
                entry["state"] = "failed"
                raise
        context.report.finish_phase(phase, items=len(coverage))
        if local:
            phase = context.report.start_phase("structural-discovery")
            for entry in coverage:
                graph = entry["graph_uri"]
                try:
                    needed = entry["untyped_subject_triples"] > 0 or bool(
                        _select(
                            context,
                            f"SELECT ?s {_dataset(graph, named)} WHERE {{ "
                            f"?s ?p ?o . FILTER(!{match}) }} LIMIT 1",
                            "structural/check",
                        )
                    )
                    observations[graph] = (
                        _select(
                            context,
                            _discovery_query(graph, named, f"FILTER(!{match})", include_edges=True),
                            "structural/discovery",
                        )
                        if needed
                        else []
                    )
                    missing = len(observations[graph])
                    covered = entry["triple_count"] - missing
                    if covered < 0:
                        raise ValueError("Structural bindings exceed the graph triple count")
                    entry.update(
                        excluded_subject_triples=0,
                        covered_triples=covered,
                        uncovered_triples=missing,
                        type_graph_uris=named,
                        subject_selection="uncovered",
                        state="needed" if missing else "not_needed",
                    )
                except Exception:
                    entry["state"] = "failed"
                    raise
            context.report.finish_phase(
                phase, items=sum(len(rows) for rows in observations.values())
            )
        if keys and not any(entry["covered_triples"] for entry in coverage):
            for entry in coverage:
                entry.update(state="failed", reason="typed_coverage_mismatch")
            raise ValueError("Typed observations have zero edge coverage")
        if not any(entry["uncovered_triples"] for entry in coverage):
            return patterns
        phase = context.report.start_phase("structural-patterns")
        for entry in coverage:
            if not entry["uncovered_triples"]:
                continue
            try:
                self._mine_graph(context, entry, keys, named, observations.get(entry["graph_uri"]))
            except Exception:
                entry["state"] = "failed"
                raise
        context.report.report.config["structural_pattern_count"] = len(context.structural_patterns)
        context.report.finish_phase(phase, items=len(context.structural_patterns))
        return patterns

    def _mine_graph(
        self,
        context: MiningContext,
        entry: dict[str, Any],
        keys: list[tuple[str, str, str, str | None]],
        named: list[str],
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        graph = entry["graph_uri"]
        bulk = rows is not None
        if rows is None:
            # One query for each property with uncovered edges, with the one-property test of
            # the census and the recount (see _census).
            rows = []
            for predicate, n in sorted(entry.get("census_properties", {}).items()):
                if not n.get("uncoveredTriples"):
                    continue
                own = [key for key in keys if key[1] == predicate]
                residual = f"VALUES ?p {{ <{predicate}> }} " + uncovered_filter(
                    own, context.graph_uris, context.type_context_graph_uris
                )
                rows += _select(
                    context, _discovery_query(graph, named, residual), "structural/discovery"
                )
        candidates: dict[str, StructuralPattern] = {}
        bindings: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            predicate = row["p"]["value"]
            candidate = StructuralPattern(
                subject_properties=row["ss"]["value"].split(),
                object_properties=row.get("os", {}).get("value", "").split(),
                subject_kind=row["sk"]["value"],
                object_kind=row["ok"]["value"],
                property_uri=predicate,
                datatype=row.get("dt", {}).get("value"),
                language=row.get("lang", {}).get("value"),
                graph_uri=graph,
                type_graph_uris=named,
                object_type_graph_uris=context.type_context_graph_uris or [],
                covered_types=[(s, o, dt) for s, p, o, dt in keys if p == predicate],
                subject_selection="uncovered",
                count=0,
                distinct_subjects=0,
                distinct_objects=0,
                witness_query="",
                recount_query="",
            )
            key = json.dumps(candidate.model_dump(), sort_keys=True)
            candidates[key] = candidate
            if bulk:
                bindings[key].append(row)
        structural = []
        for key in sorted(candidates):
            pattern = candidates[key]
            pattern.witness_query, pattern.recount_query = structural_queries(pattern)
            if bulk:
                pairs = {
                    (json.dumps(row["s"], sort_keys=True), json.dumps(row["o"], sort_keys=True))
                    for row in bindings[key]
                }
                pattern.count = len(pairs)
                pattern.distinct_subjects = len({subject for subject, _ in pairs})
                pattern.distinct_objects = len({obj for _, obj in pairs})
                pattern.examples = [{name: bindings[key][0][name] for name in ("s", "o")}]
            else:
                counts = _select(context, pattern.recount_query, "structural/count")
                if len(counts) != 1:
                    raise ValueError("Expected one structural count row")
                pattern.count = int(counts[0]["n"]["value"])
                pattern.distinct_subjects = int(counts[0]["subjects"]["value"])
                pattern.distinct_objects = int(counts[0]["objects"]["value"])
                pattern.examples = _select(context, pattern.witness_query, "structural/witness")
            if pattern.count < 1 or len(pattern.examples) != 1:
                raise ValueError("Structural discovery and witnesses disagree")
            structural.append(pattern)
        if sum(p.count for p in structural) != entry["uncovered_triples"]:
            raise ValueError("Structural patterns do not account for the uncovered edges")
        context.structural_patterns.extend(structural)
        entry.update(
            state="partial" if entry.get("unchecked_triples") else "complete",
            pattern_count=len(structural),
            representation="exact_property_sets",
        )
