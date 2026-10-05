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
from rdfsolve.mining.query_builders import MEMBERSHIP, membership_path
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

logger = logging.getLogger(__name__)
from rdfsolve.mining.typed_coverage import typed_match, uncovered_filter
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.structural import StructuralPattern
from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError, SparqlHelperError


def _dataset(graph: str | None, type_graphs: list[str] | None = None) -> str:
    if graph is None:
        return ""
    return " ".join(
        ([f"FROM <{graph}>"] if graph else []) + [f"FROM NAMED <{g}>" for g in type_graphs or []]
    )


def _types(graphs: list[str] | None) -> str:
    if not graphs:
        return f"?s {membership_path()} ?_type ."
    values = " ".join(f"<{g}>" for g in graphs)
    return (
        f"VALUES ?_typeGraph {{ {values} }} GRAPH ?_typeGraph {{ ?s {membership_path()} ?_type }}"
    )


def _untyped(graphs: list[str] | None, predicate: str) -> str:
    """Test that the subject of an edge of *predicate* has no type.

    The group repeats the edge, as the census does: QLever evaluates the group of EXISTS on
    its own, and ``?s a ?_type`` alone reads every type triple of the graph.
    """
    return f"FILTER NOT EXISTS {{ ?s <{predicate}> ?o . {_types(graphs)} }}"


def _discovery_query(
    graph: str | None,
    named_graphs: list[str],
    residual: str,
    *,
    include_edges: bool = False,
) -> str:
    """Return the query of the property sets of the subjects and objects of uncovered edges.

    The property sets are grouped only for the subjects and objects of the uncovered edges
    (the edge pattern with *residual*): an engine that does not push the join into the
    subqueries (QLever) grouped every subject and object of the graph, or of the property,
    for each property (UberGraph: 47 properties refused after 10 min each; IAO_0000115 with
    720,078 edges and 5 uncovered took 263 s grouped by property, structural-discovery-20261002).
    """
    edges = "?s ?o " if include_edges else ""
    edge_pattern = f"?s ?p ?o . {residual}"
    if include_edges:
        edge_pattern = f"{{ SELECT ?s ?p ?o WHERE {{ {edge_pattern} }} }}"
    return f"""SELECT DISTINCT {edges}?ss ?os ?p ?sk ?ok ?dt ?lang
{_dataset(graph, named_graphs)} WHERE {{
  {edge_pattern}
  {{ SELECT ?s (GROUP_CONCAT(DISTINCT STR(?sp); SEPARATOR=">") AS ?ss)
     WHERE {{ {{ SELECT DISTINCT ?s WHERE {{ ?s ?p ?o . {residual} }} }} ?s ?sp ?sv }} GROUP BY ?s }}
  OPTIONAL {{
    {{ SELECT ?o (GROUP_CONCAT(DISTINCT STR(?op); SEPARATOR=">") AS ?os)
       WHERE {{ {{ SELECT DISTINCT ?o WHERE {{ ?s ?p ?o . {residual} }} }} ?o ?op ?ov }} GROUP BY ?o }}
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
        clauses.append(_untyped(pattern.type_graph_uris, pattern.property_uri))
    if pattern.subject_selection == "uncovered":
        keys = [(s, pattern.property_uri, o, dt) for s, o, dt in pattern.covered_types]
        clauses.append(
            uncovered_filter(keys, pattern.type_graph_uris, pattern.object_type_graph_uris)
        )
    if pattern.datatype:
        clauses.append(f"FILTER(DATATYPE(?o) = <{pattern.datatype}>)")
    # A literal with a datatype other than rdf:langString has no language tag. Virtuoso answers
    # 0 for DATATYPE(?o) = xsd:integer with LANG(?o) = "" (SIBiLS dc:extent; 771 for either).
    if pattern.language is not None and (pattern.language or not pattern.datatype):
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


QLEVER_PREFIX = "PREFIX ql: <http://qlever.cs.uni-freiburg.de/builtin-functions/>\n"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _untyped_subject() -> str:
    """Keep the subjects without a membership property (read from QLever's property sets)."""
    return " ".join(f"MINUS {{ ?s ql:has-predicate <{p}> }}" for p in MEMBERSHIP.get())


def _typed_edges_covered(context: MiningContext) -> bool:
    """Return whether every edge of a typed subject is in a typed profile: typed mining mined
    every discovered class and skipped no type value.
    """
    batches = context.class_batches or []
    mined = {str(c) for batch in batches for c in batch}
    mined |= {m for batch in batches for c in batch for m in getattr(c, "members", ())}
    discovered = context.discovered_classes
    return not (discovered is None or context.skipped_type_values or not set(discovered) <= mined)


def _patterns_census(
    context: MiningContext, graph: str | None, named: list[str], entry: dict[str, Any]
) -> Counter[str] | None:
    """Count the census with the precomputed patterns of QLever, when they decide it exactly.

    QLever stores the set of properties of each subject; ?s ql:has-predicate rdf:type tells a
    typed subject without reading the type triples. The triples and the untyped triples of every
    property are two grouped queries (Bgee: 106 s and 148 s, against about 10 h for the test of
    each edge). When typed mining mined every discovered class and skipped no type value, every
    edge of a typed subject is in a typed profile, so the uncovered edges are the edges of
    untyped subjects. The patterns cover the whole index, so the scope must hold every graph of
    it. Otherwise None, and the exact census runs.
    """
    if str(getattr(context.helper, "sparql_engine", "")).lower() != "qlever":
        return None
    if not _typed_edges_covered(context):
        return None
    dataset = _dataset(graph, named)
    scope = list(dict.fromkeys(([graph] if graph else []) + named))
    try:
        probe = QLEVER_PREFIX + "SELECT ?s WHERE { ?s ql:has-predicate ?p } LIMIT 1"
        _select(context, probe, "structural/patterns", paged=False)
        if scope:
            listed = ", ".join(URIRef(g).n3() for g in scope)
            outside = (
                f"SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH ?_g {{ ?s ?p ?o }} "
                f"FILTER(?_g NOT IN ({listed})) }}"
            )
            (row,) = _select(context, outside, "structural/patterns", paged=False)
            if int(row["n"]["value"]):
                return None
        triples = _select(
            context,
            f"SELECT ?p (COUNT(*) AS ?n) {dataset} WHERE {{ ?s ?p ?o }} GROUP BY ?p",
            "structural/coverage",
            paged=False,
        )
        untyped = _select(
            context,
            QLEVER_PREFIX + f"SELECT ?p (COUNT(*) AS ?n) {dataset} "
            f"WHERE {{ ?s ?p ?o . {_untyped_subject()} }} GROUP BY ?p",
            "structural/coverage",
            paged=False,
        )
    except EndpointError:
        return None
    by_property = {r["p"]["value"]: int(r["n"]["value"]) for r in untyped}
    counts = Counter[str]()
    per_property: dict[str, dict[str, Any]] = {}
    for row in triples:
        predicate, total = row["p"]["value"], int(row["n"]["value"])
        missing = by_property.get(predicate, 0)
        per_property[predicate] = {
            "triples": total,
            "untypedTriples": missing,
            "uncoveredTriples": missing,
        }
        counts.update(triples=total, untypedTriples=missing, uncoveredTriples=missing)
    entry["census"] = "qlever_patterns"
    entry["census_properties"] = per_property
    return counts


def _patterns_query(
    graph: str | None, named: list[str], predicate: str, part: Sequence[str] = ()
) -> str:
    """Return the query of the patterns of the edges of *predicate* whose subject is untyped and
    is in *part* (clauses on the subject's properties, see _split_property).
    """
    prop = f"<{predicate}>"
    subject = " ".join([_untyped_subject(), *part])
    return (
        QLEVER_PREFIX
        + f"""SELECT ?p ?ss ?os ?sk ?ok ?dt ?lang (COUNT(*) AS ?n)
  (COUNT(DISTINCT ?s) AS ?subjects) (COUNT(DISTINCT ?o) AS ?objects)
  (SAMPLE(CONCAT(IF(isBlank(?s), "", STR(?s)), ">", IF(isBlank(?o), "", STR(?o)))) AS ?witness)
{_dataset(graph, named)} WHERE {{ {{ SELECT DISTINCT ?s ?o ?ss ?os ?p ?sk ?ok ?dt ?lang WHERE {{
  ?s {prop} ?o . {subject}
  {{ SELECT ?s (GROUP_CONCAT(DISTINCT STR(?sp); SEPARATOR=">") AS ?ss)
     WHERE {{ ?s {prop} ?_o . {subject} ?s ql:has-predicate ?sp }} GROUP BY ?s }}
  OPTIONAL {{
    {{ SELECT ?o (GROUP_CONCAT(DISTINCT STR(?op); SEPARATOR=">") AS ?os)
       WHERE {{ ?s {prop} ?o . {subject} ?o ql:has-predicate ?op }} GROUP BY ?o }}
  }}
  BIND({prop} AS ?p)
  BIND(IF(isBlank(?s), "BlankNode", "IRI") AS ?sk)
  BIND(IF(isBlank(?o), "BlankNode", IF(isLiteral(?o), "Literal", "IRI")) AS ?ok)
  BIND(DATATYPE(?o) AS ?dt)
  BIND(LANG(?o) AS ?lang)
}} }} }}
GROUP BY ?p ?ss ?os ?sk ?ok ?dt ?lang"""
    )


def _split_property(
    context: MiningContext,
    graph: str | None,
    named: list[str],
    predicate: str,
    part: Sequence[str],
    triples: int,
) -> tuple[str, int] | None:
    """Choose a property of the subjects of *part* that holds about half of its *triples*.

    Return the property and the triples of the subjects that have it; None when no property
    divides the part or the endpoint refuses. The subject's property set is a key of the
    groups of _patterns_query, so the groups of the subjects with the property and of those
    without it are disjoint and the rows of both parts are those of the whole.
    """
    subject = " ".join([_untyped_subject(), *part])
    query = (
        QLEVER_PREFIX + f"SELECT ?sp (COUNT(*) AS ?n) {_dataset(graph, named)} WHERE {{ "
        f"?s <{predicate}> ?_o . {subject} ?s ql:has-predicate ?sp }} GROUP BY ?sp"
    )
    try:
        rows = _select(context, query, "structural/discovery-split", paged=False)
    except (SparqlHelperError, ValueError) as error:
        logger.warning("Discovery: %s not split: %s", predicate, str(error)[:200])
        return None
    sizes = {row["sp"]["value"]: int(row["n"]["value"]) for row in rows}
    dividing = [(abs(2 * n - triples), p, n) for p, n in sizes.items() if 0 < n < triples]
    if not dividing:
        return None
    _, chosen, inside = min(dividing)
    return chosen, inside


def _discover_part(
    context: MiningContext,
    graph: str | None,
    named: list[str],
    predicate: str,
    part: list[str],
    triples: int,
    refused: list[tuple[int, Exception]],
) -> list[dict[str, Any]]:
    """Discover the patterns of a part, splitting it by a property of its subjects when the
    endpoint refuses it; a part that cannot be split is added to *refused* with its triples.
    """
    try:
        return _select(
            context, _patterns_query(graph, named, predicate, part), "structural/discovery"
        )
    except (SparqlHelperError, ValueError) as error:
        split = _split_property(context, graph, named, predicate, part, triples)
        if split is None:
            refused.append((triples, error))
            return []
        chosen, inside = split
        logger.info(
            "Discovery: %s split by %s (%d of %d triples)", predicate, chosen, inside, triples
        )
        has = f"?s ql:has-predicate <{chosen}> ."
        lacks = f"MINUS {{ ?s ql:has-predicate <{chosen}> }}"
        return _discover_part(
            context, graph, named, predicate, [*part, has], inside, refused
        ) + _discover_part(
            context, graph, named, predicate, [*part, lacks], triples - inside, refused
        )


def _patterns_discovery(
    context: MiningContext, graph: str | None, named: list[str], entry: dict[str, Any]
) -> list[dict[str, Any]]:
    """Count the patterns of the edges of untyped subjects by the property sets of their nodes.

    The property sets come from ql:has-predicate, which reads one row per property of a node
    instead of every triple of it. The answer has one row for each pattern of a property, with
    its triples, distinct subjects and distinct objects: one row for each edge, with the
    property sets of both nodes, exceeded the response budget of 64 MiB for WikiPathways
    gpml:hasDataNode (137,699 edges), and a query with GROUP_CONCAT is not paged. The witness
    of a pattern is one edge of its group: the kinds, datatype and language of the object are
    keys of the group, so the text of the subject and object gives the edge.

    A refused property is split by the properties of its subjects (_split_property): OMA
    dcterms:identifier, with 49,916,389 edges of untyped subjects, timed out after 600 s in
    the final GROUP BY (corpus-local-4a3affdf-8). The parts that cannot be split further are
    recorded with their triples.
    """
    rows: list[dict[str, Any]] = []
    for predicate, n in sorted(entry["census_properties"].items()):
        if not n.get("uncoveredTriples"):
            continue
        refused: list[tuple[int, Exception]] = []
        rows += _discover_part(context, graph, named, predicate, [], n["uncoveredTriples"], refused)
        if refused:
            _refused(context, n, predicate, refused[0][1], sum(t for t, _ in refused))
    return rows


def _refused(
    context: MiningContext,
    n: dict[str, Any],
    predicate: str,
    error: Exception,
    triples: int | None = None,
) -> None:
    """Record a property whose discovery was refused, for all its uncovered triples or for
    *triples* of them; the source is then partial.

    The discovery query groups with GROUP_CONCAT and cannot be read in pages (UberGraph:
    IAO_0000115).
    """
    logger.warning("Discovery: %s not discovered: %s", predicate, str(error)[:200])
    n["discovery_refused"] = str(error)[:500]
    if triples is not None and triples != n.get("uncoveredTriples"):
        n["undiscoveredTriples"] = triples
    failure = QueryFailure(
        "timeout", f"{predicate}: structural discovery refused", "structural/discovery"
    )
    context.report.record_outcome(QueryOutcome(state="partial", failures=[failure]))


def _property_discovery(
    context: MiningContext,
    graph: str | None,
    named: list[str],
    keys: list[tuple[str, str, str, str | None]],
    entry: dict[str, Any],
) -> tuple[list[dict[str, Any]], set[str]]:
    """Discover the uncovered edges with one query for each property.

    Return the rows and the properties whose uncovered edges are those of untyped subjects.
    Each query has the one-property test of the census and the recount (see _census). When
    the uncovered edges of a property are as many as the edges of its untyped subjects, they
    are the same edges (an untyped subject has no typed profile), and the test of an untyped
    subject is used without the typed keys, whose filter Virtuoso refused (SIBiLS
    pattern#contains: SQ200, stack overflow in cost model).
    """
    rows: list[dict[str, Any]] = []
    untyped: set[str] = set()
    for predicate, n in sorted(entry.get("census_properties", {}).items()):
        if not n.get("uncoveredTriples"):
            continue
        own = [key for key in keys if key[1] == predicate]
        if n.get("untypedTriples") == n["uncoveredTriples"]:
            untyped.add(predicate)
            test = _untyped(named, predicate)
        else:
            test = uncovered_filter(own, context.graph_uris, context.type_context_graph_uris)
        residual = f"VALUES ?p {{ <{predicate}> }} " + test
        query = _discovery_query(graph, named, residual)
        try:
            rows += _select(context, query, "structural/discovery")
        except (SparqlHelperError, ValueError) as error:
            _refused(context, n, predicate, error)
    return rows, untyped


def _undiscovered_triples(entry: dict[str, Any]) -> int:
    """Return the triples of the properties, or of the parts of them, whose discovery was refused."""
    return sum(
        int(n.get("undiscoveredTriples", n.get("uncoveredTriples")) or 0)
        for n in (entry.get("census_properties") or {}).values()
        if "discovery_refused" in n
    )


def _witness(row: dict[str, Any]) -> dict[str, Any]:
    """Return the edge that a grouped discovery row samples, as SPARQL JSON bindings.

    A blank node has no label that another query can use; it is given as a blank node only.
    """
    # No IRI holds ">" (one that would cannot be written in a query); a literal object may.
    subject, value = row["witness"]["value"].split(">", 1)
    edge: dict[str, Any] = {
        "s": {"type": "uri", "value": subject}
        if row["sk"]["value"] == "IRI"
        else {"type": "bnode", "value": "witness"}
    }
    kind = row["ok"]["value"]
    if kind == "IRI":
        edge["o"] = {"type": "uri", "value": value}
    elif kind == "Literal":
        edge["o"] = {"type": "literal", "value": value}
        language = row.get("lang", {}).get("value")
        if language:
            edge["o"]["xml:lang"] = language
        elif row.get("dt", {}).get("value"):
            edge["o"]["datatype"] = row["dt"]["value"]
    else:
        edge["o"] = {"type": "bnode", "value": "witness"}
    return edge


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
    """Run the census queries; each returns one count.

    The counts are kept in the checkpoint of the run, keyed by the queries, and a resumed run
    takes them from there (the census of Bgee RO_0002206 takes about 6.5 h).
    """
    import hashlib

    key = ("census|" + hashlib.sha256("\n".join(queries).encode()).hexdigest(),)
    resumed = getattr(context, "resumed", None) or {}
    if key in resumed:
        counts: Counter[str] = Counter(resumed[key][0])
    else:
        counts = Counter()
        for query in queries:
            (row,) = _select(context, query, "structural/coverage", paged=False)
            counts.update({name: int(binding["value"]) for name, binding in row.items()})
    context.report.checkpoint("census", list(key), [dict(counts)])
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
        # The triples and the untyped triples of each property, which decide the subject
        # selection of the patterns as through an endpoint (see _mine_graph). RDFLib answers a
        # grouped count without solutions with one empty row.
        grouped: dict[str, dict[str, Any]] = defaultdict(dict)
        for name, test in (
            ("triples", ""),
            ("untypedTriples", f"FILTER NOT EXISTS {{ {_types(named)} }}"),
        ):
            query = (
                f"SELECT ?p (COUNT(?o) AS ?n) {_dataset(graph, named)} "
                f"WHERE {{ ?s ?p ?o . {test} }} GROUP BY ?p"
            )
            for row in _select(context, query, "structural/coverage", paged=False):
                if "p" in row:
                    grouped[row["p"]["value"]][name] = int(row["n"]["value"])
        entry["census_properties"] = {
            prop: {"triples": n.get("triples", 0), "untypedTriples": n.get("untypedTriples", 0)}
            for prop, n in sorted(grouped.items())
        }
        return counts
    listing = f"SELECT DISTINCT ?p {_dataset(graph, named)} WHERE {{ ?s ?p ?o }}"
    predicates = sorted(r["p"]["value"] for r in _select(context, listing, "structural/properties"))
    logger.info("Census: counting %d properties one at a time", len(predicates))
    counts = Counter[str]()
    per_property: dict[str, dict[str, Any]] = {}
    # After the endpoint has refused the test of each edge of one property, and when typed
    # mining mined every discovered class and skipped no type value, the test is not sent for
    # the next properties: their uncovered edges are the edges of their untyped subjects, the
    # counts that a refused test falls back to (pharmgkb: 600 s refused for each of 7
    # properties). The skipped properties are recorded. A graph whose tests are all answered is
    # counted by the test of each edge, which also finds typed profiles that cover no edge.
    skipped: dict[str, Any] | None = None
    for number, predicate in enumerate(predicates, start=1):
        logger.info("Census: property %d/%d %s", number, len(predicates), predicate)
        own = [key for key in keys if key[1] == predicate]
        untyped = _untyped_census(context, graph, named, predicate) if skipped else None
        if skipped and untyped is not None:
            logger.info("Census: %s counted from its untyped subjects", predicate)
            skipped["properties"].append(predicate)
            per_property[predicate] = {**untyped, "census": "untyped subjects"}
            counts.update(untyped)
            continue
        try:
            found = _property_census(context, graph, named, own, local, predicate, entry)
        except CensusRefusedError as refused:
            untyped = (
                _untyped_census(context, graph, named, predicate)
                if not skipped and _typed_edges_covered(context)
                else None
            )
            if untyped is not None:
                # Every edge of a typed subject is in a typed profile, so the uncovered edges are
                # the edges of untyped subjects; the test of each edge is not needed.
                logger.info("Census: %s counted from its untyped subjects (%s)", predicate, refused)
                per_property[predicate] = {**untyped, "census": "untyped subjects"}
                counts.update(untyped)
                skipped = entry["census_edge_test"] = {
                    "state": "skipped",
                    "reason": "typed_edges_covered",
                    "refused": predicate,
                    "properties": [],
                }
                continue
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


def _properties(joined: str) -> list[str]:
    """Split a property set joined by ">", which no IRI holds (a property IRI may hold a space;
    Virtuoso returns the escape of a newline in a SPARQL string undecoded).
    """
    return [p for p in joined.split(">") if p]


def _object_term(binding: dict[str, Any]) -> str:
    """Write an IRI or literal of a SPARQL JSON result as a SPARQL term."""
    if binding["type"] == "uri":
        return f"<{binding['value']}>"
    language, datatype = binding.get("xml:lang"), binding.get("datatype")
    return Literal(binding["value"], lang=language, datatype=datatype).n3()


def _untyped_census(
    context: MiningContext, graph: str | None, named: list[str], predicate: str
) -> dict[str, int] | None:
    """Count the triples of a property and those of its untyped subjects, which are its uncovered
    triples when typed edges are covered; None when the endpoint refuses.
    """
    queries = _census_queries(graph, named, "false", True, predicate)
    try:
        counts = _count(context, queries)
    except EndpointTimeoutError:
        return None
    return {
        "triples": counts["triples"],
        "untypedTriples": counts["untypedTriples"],
        "uncoveredTriples": counts["untypedTriples"],
    }


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
    total, distinct = int(row["triples"]["value"]), int(row["objects"]["value"])
    if total < CENSUS_MIN_TRIPLES_PER_OBJECT * distinct:
        raise CensusRefusedError(
            total,
            f"census refused; {total} triples over {distinct} objects are too few per object"
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
        except EndpointTimeoutError as error:
            if len(batch) == 1:
                raise CensusRefusedError(
                    total, f"census refused for a batch of one object {batch[0]}: {error}"
                ) from error
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
                counts = None if local else _patterns_census(context, graph, named, entry)
                if counts is None:
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
                rows = observations.get(entry["graph_uri"])
                if rows is None and entry.get("census") == "qlever_patterns":
                    rows = _patterns_discovery(context, entry["graph_uri"], named, entry)
                self._mine_graph(context, entry, keys, named, rows)
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
        untyped: set[str] = set()
        if rows is None:
            rows, untyped = _property_discovery(context, graph, named, keys, entry)
        if bulk:
            census = entry.get("census_properties", {})
            found: Counter[str] = Counter()
            for row in rows:  # A grouped row counts its edges (_patterns_discovery).
                found[row["p"]["value"]] += int(row["n"]["value"]) if "n" in row else 1
            untyped = {
                p
                for p, n in found.items()
                if census.get(p, {}).get("untypedTriples")
                == n + census.get(p, {}).get("undiscoveredTriples", 0)
            }
        candidates: dict[str, StructuralPattern] = {}
        bindings: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            predicate = row["p"]["value"]
            candidate = StructuralPattern(
                subject_properties=_properties(row["ss"]["value"]),
                object_properties=_properties(row.get("os", {}).get("value", "")),
                subject_kind=row["sk"]["value"],
                object_kind=row["ok"]["value"],
                property_uri=predicate,
                datatype=row.get("dt", {}).get("value"),
                language=row.get("lang", {}).get("value"),
                graph_uri=graph,
                type_graph_uris=named,
                object_type_graph_uris=context.type_context_graph_uris or [],
                covered_types=[(s, o, dt) for s, p, o, dt in keys if p == predicate],
                subject_selection="untyped" if predicate in untyped else "uncovered",
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
            if bulk and "n" in bindings[key][0]:
                # Rows of patterns (_patterns_discovery). Rows of one pattern with two spellings of
                # a property set are recounted: their distinct objects do not add up.
                group = bindings[key]
                pattern.count = sum(int(row["n"]["value"]) for row in group)
                if len(group) == 1:
                    pattern.distinct_subjects = int(group[0]["subjects"]["value"])
                    pattern.distinct_objects = int(group[0]["objects"]["value"])
                else:
                    (recount,) = _select(context, pattern.recount_query, "structural/count")
                    pattern.count = int(recount["n"]["value"])
                    pattern.distinct_subjects = int(recount["subjects"]["value"])
                    pattern.distinct_objects = int(recount["objects"]["value"])
                pattern.examples = [_witness(group[0])]
            elif bulk:
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
        undiscovered = _undiscovered_triples(entry)
        if sum(p.count for p in structural) != entry["uncovered_triples"] - undiscovered:
            raise ValueError("Structural patterns do not account for the uncovered edges")
        context.structural_patterns.extend(structural)
        entry.update(
            undiscovered_triples=undiscovered,
            state="partial" if entry.get("unchecked_triples") or undiscovered else "complete",
            pattern_count=len(structural),
            representation="exact_property_sets",
        )
