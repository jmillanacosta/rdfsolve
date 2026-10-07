"""Mine graph-local shapes for edges absent from typed profiles."""

from __future__ import annotations

import json
import logging
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from typing import Any

from rdflib import Literal, URIRef

from rdfsolve._outcomes import FailureCategory, QueryFailure, QueryOutcome, QuerySample
from rdfsolve.mining.local_graph import LocalGraphHelper
from rdfsolve.mining.query_builders import MEMBERSHIP, membership_path
from rdfsolve.mining.sampling import flag, sample_of
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

logger = logging.getLogger(__name__)
from rdfsolve.mining.typed_coverage import typed_match, uncovered_filter
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.structural import StructuralPattern
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointRateLimitError,
    EndpointTimeoutError,
    QueryParserLimitError,
    SparqlHelperError,
)


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
    sample: int | None = None,
) -> str:
    """Return the query of the property sets of the subjects and objects of uncovered edges.

    The property sets are grouped only for the subjects and objects of the uncovered edges
    (the edge pattern with *residual*): an engine that does not push the join into the
    subqueries (QLever) grouped every subject and object of the graph, or of the property,
    for each property (UberGraph: 47 properties refused after 10 min each; IAO_0000115 with
    720,078 edges and 5 uncovered took 263 s grouped by property, structural-discovery-20261002).

    With *sample*, only the first *sample* uncovered edges, and the property sets of their nodes,
    are read (a refused discovery, rdfsolve.mining.sampling).
    """
    edges = "?s ?o " if include_edges else ""
    source = f"?s ?p ?o . {residual}"
    if sample:
        source = f"{{ SELECT ?s ?p ?o WHERE {{ {source} }} LIMIT {sample} }}"
    edge_pattern = source
    if include_edges:
        edge_pattern = f"{{ SELECT ?s ?p ?o WHERE {{ {edge_pattern} }} }}"
    return f"""SELECT DISTINCT {edges}?ss ?os ?p ?sk ?ok ?dt ?lang
{_dataset(graph, named_graphs)} WHERE {{
  {edge_pattern}
  {{ SELECT ?s (GROUP_CONCAT(DISTINCT STR(?sp); SEPARATOR=">") AS ?ss)
     WHERE {{ {{ SELECT DISTINCT ?s WHERE {{ {source} }} }} ?s ?sp ?sv }} GROUP BY ?s }}
  OPTIONAL {{
    {{ SELECT ?o (GROUP_CONCAT(DISTINCT STR(?op); SEPARATOR=">") AS ?os)
       WHERE {{ {{ SELECT DISTINCT ?o WHERE {{ {source} }} }} ?o ?op ?ov }} GROUP BY ?o }}
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


def _sample_rows(
    context: MiningContext,
    build: Callable[[int], str],
    purpose: str,
    error: Exception,
    *,
    unit: str,
    predicate: str,
    graph: str | None,
) -> list[dict[str, Any]] | None:
    """Ask a refused structural query again over samples (rdfsolve.mining.sampling).

    Return the rows of the first sample that the endpoint answers, each with the provenance of
    the sample, and record the sample in the report (not a failure); None when *error* is not
    a refusal or every sample is refused.
    """
    from rdfsolve.mining.sampling import sample_sizes, with_sample
    from rdfsolve.sparql_helper import PaginationTruncatedError

    if not isinstance(error, EndpointTimeoutError):
        return None
    for size in sample_sizes():
        try:
            rows = _select(context, build(size), f"{purpose}/sample", paged=False)
        except EndpointTimeoutError:
            continue
        except (SparqlHelperError, ValueError):
            return None
        sample = QuerySample(
            purpose,
            size,
            unit,
            "truncated" if isinstance(error, PaginationTruncatedError) else "timeout",
            str(error)[:500],
            [],
            predicate,
            [graph] if graph else None,
        )
        context.report.record_outcome(QueryOutcome(samples=[sample]))
        logger.info("%s: %s refused; answered over %d %s", purpose, predicate, size, unit)
        return with_sample(rows, sample)
    return None


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
    graph: str | None,
    named: list[str],
    predicate: str,
    part: Sequence[str] = (),
    *,
    witness: bool = True,
    sample: int | None = None,
) -> str:
    """Return the query of the patterns of the edges of *predicate* whose subject is untyped and
    is in *part* (clauses on the subject's properties, see _split_property).

    The property set of a subject is grouped from one row for each of its properties, not for
    each of its edges: MedGen dcterms:references (95,419,320 edges of 125,005 subjects) took
    338 s grouped by edge and 3.5 s by subject (corpus-local-4a3affdf-2).

    Without *witness*, the rows have no sampled edge: the text of the edge is built for each
    edge before it is sampled, which took the MedGen query past 600 s (273 s without it).
    With *sample*, only the first *sample* edges of the part are grouped (a refused discovery,
    rdfsolve.mining.sampling): the counts of the rows are lower bounds.
    """
    prop = f"<{predicate}>"
    subject = " ".join([_untyped_subject(), *part])
    source = f"?s {prop} ?o . {subject}"
    if sample:
        source = f"{{ SELECT ?s ?o WHERE {{ {source} }} LIMIT {sample} }}"
    witnessed = (
        '\n  (SAMPLE(CONCAT(IF(isBlank(?s), "", STR(?s)), ">", IF(isBlank(?o), "", STR(?o)))) '
        "AS ?witness)"
        if witness
        else ""
    )
    return (
        QLEVER_PREFIX
        + f"""SELECT ?p ?ss ?os ?sk ?ok ?dt ?lang (COUNT(*) AS ?n)
  (COUNT(DISTINCT ?s) AS ?subjects) (COUNT(DISTINCT ?o) AS ?objects){witnessed}
{_dataset(graph, named)} WHERE {{ {{ SELECT DISTINCT ?s ?o ?ss ?os ?p ?sk ?ok ?dt ?lang WHERE {{
  {source}
  {{ SELECT ?s (GROUP_CONCAT(DISTINCT STR(?sp); SEPARATOR=">") AS ?ss)
     WHERE {{ ?s ql:has-predicate {prop} . {subject} ?s ql:has-predicate ?sp }} GROUP BY ?s }}
  OPTIONAL {{
    {{ SELECT ?o (GROUP_CONCAT(DISTINCT STR(?op); SEPARATOR=">") AS ?os)
       WHERE {{ {source} ?o ql:has-predicate ?op }} GROUP BY ?o }}
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
    refused: list[tuple[int, Exception, list[str]]],
) -> list[dict[str, Any]]:
    """Discover the patterns of a part, splitting it by a property of its subjects when the
    endpoint refuses it. A part that cannot be split is asked again without witnesses, which
    are then read for each pattern (structural_queries); one still refused is added to
    *refused* with its triples. MedGen dcterms:references cannot be split: its 125,005 subjects
    have one property set (corpus-local-4a3affdf-2).
    """
    try:
        return _select(
            context, _patterns_query(graph, named, predicate, part), "structural/discovery"
        )
    except (SparqlHelperError, ValueError) as error:
        split = _split_property(context, graph, named, predicate, part, triples)
        if split is None:
            logger.info("Discovery: %s asked again without witnesses", predicate)
            try:
                return _select(
                    context,
                    _patterns_query(graph, named, predicate, part, witness=False),
                    "structural/discovery",
                )
            except (SparqlHelperError, ValueError):
                refused.append((triples, error, part))
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
    asked over a sample of their edges (the property is then discovery_sampled), and recorded
    with their triples when the samples are refused too.
    """
    rows: list[dict[str, Any]] = []
    for predicate, n in sorted(entry["census_properties"].items()):
        if not n.get("uncoveredTriples"):
            continue
        refused: list[tuple[int, Exception, list[str]]] = []
        rows += _discover_part(context, graph, named, predicate, [], n["uncoveredTriples"], refused)
        sampled: list[dict[str, Any]] = []
        for _, error, part in refused:

            def part_sample(size: int, part: list[str] = part, predicate: str = predicate) -> str:
                """Group the patterns of the first *size* edges of the part."""
                return _patterns_query(graph, named, predicate, part, sample=size)

            found = _sample_rows(
                context,
                part_sample,
                "structural/discovery",
                error,
                unit="edges",
                predicate=predicate,
                graph=graph,
            )
            if found is None:
                sampled = []
                break
            sampled += found
        if refused and sampled:
            # The refused parts answered over samples: their rows stand, flagged sampled.
            rows += sampled
            n["discovery_sampled"] = _provenance(sampled)
        elif refused:
            _refused(context, n, predicate, refused[0][1], sum(t for t, _, _ in refused))
    return rows


def _provenance(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the provenance of the first sampled row."""
    from rdfsolve.mining.sampling import sample_of

    return next(p for row in rows if (p := sample_of(row)) is not None)


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
    residuals: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Discover the uncovered edges with one query for each property.

    Return the rows and the properties whose uncovered edges are those of untyped subjects.
    Each query has the one-property test of the census and the recount (see _census). When
    the uncovered edges of a property are as many as the edges of its untyped subjects, they
    are the same edges (an untyped subject has no typed profile), and the test of an untyped
    subject is used without the typed keys, whose filter Virtuoso refused (SIBiLS
    pattern#contains: SQ200, stack overflow in cost model). A property whose discovery stays
    refused is discovered over a sample of its edges (discovery_sampled; the rows hold the
    edges, which give the counts when the recount is refused too).
    """
    rows: list[dict[str, Any]] = []
    untyped: set[str] = set()
    for predicate, n in sorted(entry.get("census_properties", {}).items()):
        # A property whose census was refused and answered over a sample (_sampled_census)
        # is discovered over a sample of its edges at once.
        counted = n if n.get("uncoveredTriples") else n.get("sampled") or {}
        if not counted.get("uncoveredTriples"):
            continue
        own = [key for key in keys if key[1] == predicate]
        if counted.get("untypedTriples") == counted["uncoveredTriples"]:
            untyped.add(predicate)
            test = _untyped(named, predicate)
        else:
            test = uncovered_filter(own, context.graph_uris, context.type_context_graph_uris)
        residual = f"VALUES ?p {{ <{predicate}> }} " + test
        if residuals is not None:
            residuals[predicate] = residual
        query = _discovery_query(graph, named, residual)
        found: list[dict[str, Any]] = []
        missing: int | None = None
        if counted is not n:
            error: Exception = EndpointTimeoutError(n.get("refused") or "census refused")
            last: list[Exception] = [error]
        else:
            try:
                rows += _select(context, query, "structural/discovery")
                continue
            except (SparqlHelperError, ValueError) as refused:
                error = refused
            logger.info("Discovery: %s refused; discovering it in batches of subjects", predicate)
            last = [error]
            found, missing = _discover_by_subjects(context, graph, named, residual, last)
        rows += found
        if missing is None or missing:

            def edges(size: int, residual: str = residual) -> str:
                """Discover the property over its first *size* edges."""
                return _discovery_query(graph, named, residual, include_edges=True, sample=size)

            sampled = _sample_rows(
                context,
                edges,
                "structural/discovery",
                error,
                unit="edges",
                predicate=predicate,
                graph=graph,
            )
            if sampled:
                rows += sampled
                n["discovery_sampled"] = _provenance(sampled)
            else:
                # The reason is the last refusal, not the first one (the refusal of the
                # whole property, or of reading it in pages).
                _refused(context, n, predicate, last[-1], missing)
    return rows, untyped


def _discover_pairs(
    context: MiningContext, graph: str | None, named: list[str], residual: str
) -> list[dict[str, Any]]:
    """Discover the patterns of the edges that *residual* selects without GROUP_CONCAT.

    The rows are those of _discovery_query: the edges with the kinds, datatype and language of
    their objects, and the property sets of their subjects and objects. Here the edges and the
    (node, property) pairs are read with queries without aggregates, which the helper can read
    in pages in a fixed order (ORDER BY of every projected variable); the property sets are
    built from the pairs (sorted, as StructuralPattern keeps them). GROUP_CONCAT has no fixed
    order, so the helper does not page a query with it (IDEAL author and WikiPathways
    gpml:hasDataNode, jobs 115329 and 115327: subjects refused one by one at the time limit).
    A blank node has no name that holds from one request to the next, so the edges whose subject
    or object is a blank node are discovered with the grouped query, as before. Raise
    SparqlHelperError or ValueError when a query is refused.
    """
    dataset = _dataset(graph, named)
    binds = (
        'BIND(IF(isBlank(?s), "BlankNode", "IRI") AS ?sk) '
        'BIND(IF(isBlank(?o), "BlankNode", IF(isLiteral(?o), "Literal", "IRI")) AS ?ok) '
        "BIND(DATATYPE(?o) AS ?dt) BIND(LANG(?o) AS ?lang)"
    )
    named_nodes = "FILTER(!isBlank(?s) && !isBlank(?o))"
    edges = _select(
        context,
        f"SELECT DISTINCT ?s ?o ?p ?sk ?ok ?dt ?lang {dataset} WHERE {{ "
        f"?s ?p ?o . {residual} {named_nodes} {binds} }}",
        "structural/discovery-pairs",
    )
    subject_pairs = _select(
        context,
        f"SELECT DISTINCT ?s ?sp {dataset} WHERE {{ {{ SELECT DISTINCT ?s WHERE {{ "
        f"?s ?p ?o . {residual} {named_nodes} }} }} ?s ?sp ?sv }}",
        "structural/discovery-pairs",
    )
    object_pairs = _select(
        context,
        f"SELECT DISTINCT ?o ?op {dataset} WHERE {{ {{ SELECT DISTINCT ?o WHERE {{ "
        f"?s ?p ?o . {residual} {named_nodes} FILTER(isIRI(?o)) }} }} ?o ?op ?ov }}",
        "structural/discovery-pairs",
    )
    sets: dict[str, dict[str, set[str]]] = {"s": defaultdict(set), "o": defaultdict(set)}
    for node, prop, pairs in (("s", "sp", subject_pairs), ("o", "op", object_pairs)):
        for row in pairs:
            sets[node][row[node]["value"]].add(row[prop]["value"])
    rows: dict[str, dict[str, Any]] = {}
    for edge in edges:
        row = {name: edge[name] for name in ("sk", "ok", "dt", "lang") if name in edge}
        row["p"] = edge["p"]
        row["ss"] = {"type": "literal", "value": ">".join(sorted(sets["s"][edge["s"]["value"]]))}
        if edge["o"]["type"] == "uri" and sets["o"].get(edge["o"]["value"]):
            row["os"] = {
                "type": "literal",
                "value": ">".join(sorted(sets["o"][edge["o"]["value"]])),
            }
        rows[json.dumps(row, sort_keys=True)] = row
    blank = f"{residual} FILTER(isBlank(?s) || isBlank(?o))"
    if _select(
        context,
        f"SELECT ?s {dataset} WHERE {{ ?s ?p ?o . {blank} }} LIMIT 1",
        "structural/discovery-pairs",
        paged=False,
    ):
        query = _discovery_query(graph, named, blank)
        for row in _select(context, query, "structural/discovery", paged=False):
            rows[json.dumps(row, sort_keys=True)] = row
    return list(rows.values())


# Subjects of one discovery query when the discovery of a property is refused as a whole. The
# cost grows faster than the batch (WikiPathways dc:creator: 50 subjects 6.4 s, 100 23.6 s, 200
# refused at the ANYTIME limit).
DISCOVERY_SUBJECT_BATCH = 50


def _discover_by_subjects(
    context: MiningContext,
    graph: str | None,
    named: list[str],
    residual: str,
    errors: list[Exception] | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    """Discover the patterns of the uncovered edges of one property in batches of subjects.

    The discovery query groups with GROUP_CONCAT, which the helper does not read in pages
    (the order of a group's values is not stable between requests). Its rows are distinct
    patterns, so the rows of disjoint sets of subjects together are the rows of the property:
    the uncovered subjects are listed with a query that can be paged, and the discovery runs for
    each batch of them, FILTER(?s IN ...) inside each of its subqueries, then for the blank-node
    subjects together. A refused batch is split in two. Return the rows and the uncovered
    triples of the subjects that were still refused; None when the subjects cannot be listed or
    those triples cannot be counted, and the whole property stays undiscovered (WikiPathways
    dc:creator, 8,095 edges: "estimated execution time 1115 s exceeds the limit of 400 s").
    A subject refused alone is discovered from its edges and the pairs of its nodes and their
    properties (_discover_pairs), which need no GROUP_CONCAT. The refusals are added to
    *errors*, when given.
    """
    dataset = _dataset(graph, named)
    try:
        listed = _select(
            context,
            f"SELECT DISTINCT ?s {dataset} WHERE {{ ?s ?p ?o . {residual} }}",
            "structural/discovery-subjects",
        )
    except (SparqlHelperError, ValueError) as error:
        if errors is not None:
            errors.append(error)
        logger.warning("Discovery: subjects not listed: %s", str(error)[:200])
        return [], None
    subjects = sorted({_object_term(r["s"]) for r in listed if r["s"]["type"] == "uri"})
    blank = any(r["s"]["type"] == "bnode" for r in listed)
    pending: list[list[str]] = [
        subjects[i : i + DISCOVERY_SUBJECT_BATCH]
        for i in range(0, len(subjects), DISCOVERY_SUBJECT_BATCH)
    ]
    rows: dict[str, dict[str, Any]] = {}
    refused: list[str] = []

    def discover(restriction: str) -> list[dict[str, Any]]:
        """Read the property sets of the subjects that match one restriction."""
        query = _discovery_query(graph, named, f"{residual} {restriction}")
        return _select(context, query, "structural/discovery", paged=False)

    while pending:
        batch = pending.pop()
        try:
            found = discover(f"FILTER(?s IN ({', '.join(batch)}))")
        except (SparqlHelperError, ValueError) as error:
            if errors is not None:
                errors.append(error)
            if len(batch) == 1:
                restriction = f"FILTER(?s IN ({batch[0]}))"
                try:
                    found = _discover_pairs(context, graph, named, f"{residual} {restriction}")
                except (SparqlHelperError, ValueError) as pairs_error:
                    if errors is not None:
                        errors.append(pairs_error)
                    refused.append(restriction)
                    continue
                logger.info("Discovery: subject %s discovered from pairs", batch[0])
                rows.update((json.dumps(row, sort_keys=True), row) for row in found)
                continue
            pending += [batch[len(batch) // 2 :], batch[: len(batch) // 2]]
            continue
        rows.update((json.dumps(row, sort_keys=True), row) for row in found)
    if blank:
        try:
            found = discover("FILTER(isBlank(?s))")
            rows.update((json.dumps(row, sort_keys=True), row) for row in found)
        except (SparqlHelperError, ValueError) as error:
            if errors is not None:
                errors.append(error)
            refused.append("FILTER(isBlank(?s))")
    missing = 0
    for restriction in refused:
        try:
            (row,) = _select(
                context,
                f"SELECT (COUNT(*) AS ?n) {dataset} WHERE {{ ?s ?p ?o . {residual} {restriction} }}",
                "structural/discovery-subjects",
                paged=False,
            )
            missing += int(row["n"]["value"])
        except (SparqlHelperError, ValueError):
            return list(rows.values()), None
    logger.info(
        "Discovery: %d subjects in batches, %d patterns, %d triples of refused subjects",
        len(subjects),
        len(rows),
        missing,
    )
    return list(rows.values()), missing


def _undiscovered_triples(entry: dict[str, Any]) -> int:
    """Return the triples of the properties, or of the parts of them, whose discovery was refused."""
    return sum(
        int(n.get("undiscoveredTriples", n.get("uncoveredTriples")) or 0)
        for n in (entry.get("census_properties") or {}).values()
        if "discovery_refused" in n
    )


def _to_discover(entry: dict[str, Any]) -> bool:
    """Return whether a graph has uncovered edges, counted or seen in a census sample."""
    return bool(entry.get("uncovered_triples") or entry.get("sampled_uncovered_triples"))


def _sample_counts(pattern: StructuralPattern, rows: list[dict[str, Any]]) -> None:
    """Count a pattern from the edges of a sample (lower bounds) and take one as its example."""
    pairs = {
        (json.dumps(row["s"], sort_keys=True), json.dumps(row["o"], sort_keys=True)) for row in rows
    }
    pattern.count = len(pairs)
    pattern.distinct_subjects = len({subject for subject, _ in pairs})
    pattern.distinct_objects = len({obj for _, obj in pairs})
    pattern.examples = [{name: rows[0][name] for name in ("s", "o")}]


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
# Properties counted in one census request through an endpoint (see _census_batch), and the
# longest such request. A refused or too slow request is split in halves; one property is counted
# by itself, as before. 1 turns batching off.
CENSUS_PROPERTIES_PER_QUERY = 16
CENSUS_QUERY_CHARS = 100_000
# Censuses of 2 properties (the smallest batched form) in a row that the parser of an endpoint
# refuses (QueryParserLimitError: Virtuoso SQ200, SQ074), after which the census of that endpoint
# counts one property at a time for the rest of the source. Job 115589: Virtuoso refused the
# batches of 8, 4 and 2 properties with "SQ200: Stack Overflow in cost model" before the single
# properties answered, so every batch of 16 cost 4 refused requests. Refusals of larger batches
# do not count: an endpoint that answers batches of 4 keeps batching. The count is kept on the
# helper of the endpoint (SparqlHelper.census_parser_refusals); an answered batch resets it.
CENSUS_PARSER_REFUSALS_BEFORE_SINGLE = 2
# Branches of one UNION group in a batched census; longer unions are nested in groups of this
# size, so that the depth of the query stays bounded (Virtuoso SQ074 counts open groups).
CENSUS_UNION_GROUP = 16


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
    sample: int | None = None,
) -> list[str]:
    """Count all triples, the triples of untyped subjects and, remotely, the uncovered triples.

    With *sample* (and a predicate), the counts are of the first *sample* edges of the property
    (a refused census, rdfsolve.mining.sampling): the tests apply to each edge of the sample.

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
    source = edge
    if sample and predicate:
        source = f"{{ SELECT ?s ?o WHERE {{ {edge} }} LIMIT {sample} }}"
    return [
        f"SELECT (COUNT(*) AS ?{name}) {_dataset(graph, named)} WHERE {{ {source} {test} }}"
        for name, test in tests.items()
    ]


def _census_key(context: MiningContext, queries: list[str]) -> tuple[str]:
    """Return the checkpoint key of a census: its queries and the graphs the helper leaves out."""
    import hashlib

    # The graphs that the helper leaves out are part of the queries that it sends.
    # The key names the pragma (default graph only), so that counts read with
    # input:named-graph-exclude as well (dbpedia: emptied) are not resumed.
    excluded = list(getattr(getattr(context, "helper", None), "excluded_graphs", None) or [])
    sent = [*(["exclude-default " + " ".join(excluded)] if excluded else []), *queries]
    return ("census|" + hashlib.sha256("\n".join(sent).encode()).hexdigest(),)


def _union(branches: list[str]) -> str:
    """Join group patterns with UNION, nested in groups of CENSUS_UNION_GROUP."""
    while len(branches) > CENSUS_UNION_GROUP:
        branches = [
            "{ " + " UNION ".join(branches[i : i + CENSUS_UNION_GROUP]) + " }"
            for i in range(0, len(branches), CENSUS_UNION_GROUP)
        ]
    return " UNION ".join(branches)


def _batched_census_query(
    graph: str | None, named: list[str], queries: dict[str, list[str]]
) -> tuple[str, dict[str, tuple[str, str]]]:
    """Return one query that answers the census queries of several properties, and its keys.

    Each branch is one census query of one property (_census_queries, unchanged) as a subquery,
    tagged with a key: the engine evaluates the same pattern as when the query is sent alone, with
    the constant property that the typed test needs (a VALUES ?p with GROUP BY ?p would make the
    property a variable inside the EXISTS group, which QLever evaluates over every edge of the
    graph and Virtuoso has counted wrongly; see typed_match and _census). The dataset clause is
    the outer query's, as for each query alone.
    """
    dataset = _dataset(graph, named)
    branches: list[str] = []
    keys: dict[str, tuple[str, str]] = {}
    for predicate, sent in queries.items():
        for query in sent:
            head, body = query.split(" WHERE ", 1)
            name = head.split("AS ?", 1)[1].split(")", 1)[0]
            key = f"{len(keys)}"
            keys[key] = (predicate, name)
            branches.append(
                f'{{ {{ SELECT (COUNT(*) AS ?_n) WHERE {body} }} BIND("{key}" AS ?_census) }}'
            )
    return f"SELECT ?_census ?_n {dataset} WHERE {{ {_union(branches)} }}", keys


def _census_batch(
    context: MiningContext,
    graph: str | None,
    named: list[str],
    keys: list[tuple[str, str, str, str | None]],
    predicates: list[str],
    entry: dict[str, Any],
) -> dict[str, Counter[str]]:
    """Count the census of several properties in few requests; return the counted properties.

    The census queries of each property are those of _property_census without a restriction,
    joined into one request (_batched_census_query). A request that the endpoint refuses, or
    that runs past its limits, is split in halves; a property left alone is not sent here, and
    _property_census counts it as before (with batches of its objects when refused). A request
    whose answer misses a count leaves its properties to _property_census too, so every count is
    the count of the query of one property (Rhea answers some empty counts with no row). An
    endpoint that rejects the batched form turns batching off for the rest of the census. The
    counts of each property are kept in the checkpoint under the key of its own queries, so a
    resumed run, batched or not, takes them again.
    """
    batching = context.report.report.config.setdefault(
        "census_batching",
        {
            "state": "on",
            "properties_per_query": CENSUS_PROPERTIES_PER_QUERY,
            "requests": 0,
            "split": 0,
            "properties": 0,
        },
    )
    helper = context.helper
    if batching["state"] == "on" and (
        getattr(helper, "census_parser_refusals", 0) >= CENSUS_PARSER_REFUSALS_BEFORE_SINGLE
    ):
        batching.update(state="single", reason="the endpoint's parser refused batched censuses")
    if batching["state"] != "on" or CENSUS_PROPERTIES_PER_QUERY < 2:
        return {}
    resumed = getattr(context, "resumed", None) or {}
    queries: dict[str, list[str]] = {}
    length = 0
    for predicate in predicates:
        if len(queries) >= CENSUS_PROPERTIES_PER_QUERY:
            break
        own = [key for key in keys if key[1] == predicate]
        match = typed_match(own, context.graph_uris, context.type_context_graph_uris, predicate)
        sent = _census_queries(graph, named, match, False, predicate)
        if _census_key(context, sent) in resumed:
            continue  # _count takes it from the checkpoint
        size = sum(len(q) for q in sent)
        if queries and length + size > CENSUS_QUERY_CHARS:
            break
        queries[predicate] = sent
        length += size
    counted: dict[str, Counter[str]] = {}
    pending = [list(queries)] if len(queries) > 1 else []
    while pending and batching["state"] == "on":
        batch = pending.pop()
        if len(batch) < 2:
            continue  # counted alone by _property_census
        query, tags = _batched_census_query(graph, named, {p: queries[p] for p in batch})
        try:
            rows = _select(context, query, "structural/coverage", paged=False)
        except EndpointRateLimitError:
            raise
        except EndpointTimeoutError as error:
            logger.info("Census: %d properties in one query refused (%s)", len(batch), error)
            batching["split"] += 1
            if (
                isinstance(error, QueryParserLimitError)
                and len(batch) == 2
                and _parser_refused(helper)
            ):
                logger.info(
                    "Census: the parser of %s refused %d censuses of 2 properties in a row; counting"
                    " one property at a time for the rest of the source",
                    getattr(helper, "endpoint_url", "the endpoint"),
                    CENSUS_PARSER_REFUSALS_BEFORE_SINGLE,
                )
                batching.update(
                    state="single", reason=f"the endpoint's parser refused: {str(error)[:300]}"
                )
                break
            if len(batch) > 2:
                half = len(batch) // 2
                pending += [batch[half:], batch[:half]]
            continue
        except EndpointError as error:
            logger.warning("Census: the batched census was rejected; one property at a time")
            batching.update(state="off", reason=str(error)[:300])
            break
        batching["requests"] += 1
        if hasattr(helper, "census_parser_refusals"):
            helper.census_parser_refusals = 0
        found: dict[str, Counter[str]] = defaultdict(Counter)
        seen: set[str] = set()
        for row in rows:
            tag = row.get("_census", {}).get("value")
            if tag in tags and "_n" in row and tag not in seen:
                seen.add(tag)
                predicate, name = tags[tag]
                found[predicate][name] = int(row["_n"]["value"])
        for predicate in batch:
            if len(found[predicate]) != len(queries[predicate]):
                continue  # a count is missing: counted alone
            counted[predicate] = found[predicate]
            context.report.checkpoint(
                "census", list(_census_key(context, queries[predicate])), [dict(found[predicate])]
            )
    batching["properties"] += len(counted)
    return counted


def _parser_refused(helper: Any) -> bool:
    """Count a batched census that the parser of the endpoint refused; return whether the
    census of the endpoint now counts one property at a time.
    """
    try:
        refusals = int(getattr(helper, "census_parser_refusals", 0)) + 1
        helper.census_parser_refusals = refusals
    except (AttributeError, TypeError, ValueError):
        return False
    return refusals >= CENSUS_PARSER_REFUSALS_BEFORE_SINGLE


def _count(context: MiningContext, queries: list[str]) -> Counter[str]:
    """Run the census queries; each returns one count.

    The counts are kept in the checkpoint of the run, keyed by the queries, and a resumed run
    takes them from there (the census of Bgee RO_0002206 takes about 6.5 h).
    """
    key = _census_key(context, queries)
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
    # Properties counted together (_census_batch), and those already tried in a batch.
    batched: dict[str, Counter[str]] = {}
    tried: set[str] = set()
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
        if not skipped and predicate not in tried:
            following = [p for p in predicates[number - 1 :] if p not in tried]
            batched.update(_census_batch(context, graph, named, keys, following, entry))
            tried.update(following[:CENSUS_PROPERTIES_PER_QUERY])
        try:
            if predicate in batched:
                found = batched.pop(predicate)
            else:
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
            per_property[predicate] = {"triples": refused.triples, "refused": str(refused)}
            counts.update(triples=refused.triples or 0, uncheckedTriples=refused.triples or 0)
            sampled = _sampled_census(context, graph, named, own, local, predicate, refused)
            if sampled is not None:
                # The triples stay unchecked in the totals; the sample's counts (lower bounds)
                # choose the discovery of the property and the untyped subjects pass.
                per_property[predicate]["sampled"] = sampled
                continue
            logger.warning("Census: %s not counted: %s", predicate, refused)
            failure = QueryFailure("timeout", f"{predicate}: {refused}", "structural/coverage")
            context.report.record_outcome(QueryOutcome(state="partial", failures=[failure]))
            continue
        per_property[predicate] = {
            name: found[name] for name in ("triples", "untypedTriples", "uncoveredTriples")
        }
        counts.update(found)
    entry["census"] = "per_property"
    entry["census_properties"] = per_property
    return counts


def _sampled_census(
    context: MiningContext,
    graph: str | None,
    named: list[str],
    keys: list[tuple[str, str, str, str | None]],
    local: bool,
    predicate: str,
    refused: CensusRefusedError,
) -> dict[str, Any] | None:
    """Count a property whose census was refused over a sample of its edges.

    Return the counts of the sample (triples, untypedTriples, uncoveredTriples of the sample,
    each a lower bound of its property) with the size, unit and refusal, and record the sample
    in the report; None when every sample is refused.
    """
    from rdfsolve.mining.sampling import sample_sizes

    match = typed_match(keys, context.graph_uris, context.type_context_graph_uris, predicate)
    for size in sample_sizes():
        queries = _census_queries(graph, named, match, local, predicate, sample=size)
        try:
            found = _count(context, queries)
        except EndpointTimeoutError:
            continue
        except (SparqlHelperError, ValueError):
            return None
        sample = QuerySample(
            "structural/coverage",
            size,
            "edges",
            "timeout",
            str(refused)[:500],
            [],
            predicate,
            [graph] if graph else None,
        )
        context.report.record_outcome(QueryOutcome(samples=[sample]))
        logger.info("Census: %s refused; counted over a sample of %d edges", predicate, size)
        return {**dict(found), **sample.provenance()}
    return None


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
    except QueryParserLimitError as error:
        # Batches of objects keep the typed test of the property, which is what the parser
        # refused; the property is recorded as not checked.
        raise CensusRefusedError(None, f"census refused by the parser: {error}") from error
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


def _structural_gap(
    context: MiningContext, entry: dict[str, Any], purpose: str, error: SparqlHelperError
) -> None:
    """Record that the endpoint refused the structural census or discovery of one graph.

    The structural layer adds evidence for edges that no typed profile covers; the typed
    patterns, mined before it, do not depend on it. So a refusal leaves this graph unchecked,
    recorded as a measurement gap (the source ends partial), and the source keeps its typed
    patterns (GlyCoNAVI, job 115329: one census query refused with Virtuoso SQ074 failed the
    whole source after 1025 s). A host that stayed busy through the waits of the helper
    (EndpointRateLimitError) is the same: the endpoint answered the typed mining before, so the
    graph is left unchecked and the source goes on (STRING, job 115591: one census query
    failed the whole source after its busy-host waits).
    """
    graph = entry.get("graph_uri")
    logger.warning("Structural %s of %s not completed: %s", purpose, graph or "the data", error)
    entry.update(state="failed", reason=f"{purpose}: {str(error)[:300]}")
    category: FailureCategory = (
        "rate_limited"
        if isinstance(error, EndpointRateLimitError)
        else "timeout"
        if isinstance(error, EndpointTimeoutError)
        else "endpoint"
    )
    failure = QueryFailure(
        category,
        f"structural evidence of {graph or 'the default graph'} not checked: {str(error)[:300]}",
        purpose,
    )
    context.report.record_outcome(QueryOutcome(state="partial", failures=[failure]))


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
                # Uncovered edges seen in samples of refused censuses: discovered over samples.
                seen = sum(
                    int((n.get("sampled") or {}).get("uncoveredTriples") or 0)
                    for n in entry["census_properties"].values()
                )
                if seen:
                    entry.update(sampled_uncovered_triples=seen, state="needed")
            except SparqlHelperError as error:
                _structural_gap(context, entry, "structural/coverage", error)
            except Exception:
                entry["state"] = "failed"
                raise
        context.report.finish_phase(phase, items=len(coverage))
        if local:
            phase = context.report.start_phase("structural-discovery")
            for entry in coverage:
                graph = entry["graph_uri"]
                if entry["state"] == "failed":
                    continue
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
        counted = [entry for entry in coverage if "covered_triples" in entry]
        if keys and counted and not any(entry["covered_triples"] for entry in counted):
            for entry in counted:
                entry.update(state="failed", reason="typed_coverage_mismatch")
            raise ValueError("Typed observations have zero edge coverage")
        if not any(_to_discover(entry) for entry in coverage):
            return patterns
        phase = context.report.start_phase("structural-patterns")
        for entry in coverage:
            if not _to_discover(entry):
                continue
            try:
                rows = observations.get(entry["graph_uri"])
                if rows is None and entry.get("census") == "qlever_patterns":
                    rows = _patterns_discovery(context, entry["graph_uri"], named, entry)
                self._mine_graph(context, entry, keys, named, rows)
            except SparqlHelperError as error:
                _structural_gap(context, entry, "structural/discovery", error)
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
        residuals: dict[str, str] = {}
        if rows is None:
            rows, untyped = _property_discovery(context, graph, named, keys, entry, residuals)
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

        def confirm(
            rows: list[dict[str, Any]],
        ) -> tuple[list[StructuralPattern], list[dict[str, Any]]]:
            """Build the patterns of discovered rows, and recount and witness each one."""
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
                if bulk or sample_of(row) is not None:
                    bindings[key].append(row)
            structural = []
            inconsistent: list[dict[str, Any]] = []
            for key in sorted(candidates):
                pattern = candidates[key]
                refused_check: str | None = None
                pattern.witness_query, pattern.recount_query = structural_queries(pattern)
                provenance = next(
                    (p for row in bindings[key] if (p := sample_of(row)) is not None), None
                )
                if provenance is not None:
                    # Found in a sample of a refused discovery: other shapes of the property may
                    # exist. Counted below exactly when the recount answers, else from the sample.
                    flag(pattern, provenance, ["patterns"])
                if provenance is not None and "n" in bindings[key][0] and len(bindings[key]) > 1:
                    # Rows of a sample of grouped patterns: the counts of the sample, lower bounds.
                    group = bindings[key]
                    pattern.count = sum(int(row["n"]["value"]) for row in group)
                    pattern.distinct_subjects = max(int(row["subjects"]["value"]) for row in group)
                    pattern.distinct_objects = max(int(row["objects"]["value"]) for row in group)
                    flag(pattern, provenance, ["counts"])
                    sampled_witness = [row for row in group if "witness" in row]
                    pattern.examples = [_witness(sampled_witness[0])] if sampled_witness else []
                elif bulk and "n" in bindings[key][0]:
                    # Rows of patterns (_patterns_discovery). Rows of one pattern with two spellings of
                    # a property set are recounted: their distinct objects do not add up.
                    group = bindings[key]
                    pattern.count = sum(int(row["n"]["value"]) for row in group)
                    if len(group) == 1:
                        pattern.distinct_subjects = int(group[0]["subjects"]["value"])
                        pattern.distinct_objects = int(group[0]["objects"]["value"])
                        if provenance is not None:
                            flag(pattern, provenance, ["counts"])
                    else:
                        (recount,) = _select(context, pattern.recount_query, "structural/count")
                        pattern.count = int(recount["n"]["value"])
                        pattern.distinct_subjects = int(recount["subjects"]["value"])
                        pattern.distinct_objects = int(recount["objects"]["value"])
                    sampled = [row for row in group if "witness" in row]
                    pattern.examples = (
                        [_witness(sampled[0])]
                        if sampled
                        else _select(context, pattern.witness_query, "structural/witness")
                    )
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
                    try:
                        counts = _select(context, pattern.recount_query, "structural/count")
                        if len(counts) != 1:
                            raise ValueError("Expected one structural count row")
                        pattern.count = int(counts[0]["n"]["value"])
                        pattern.distinct_subjects = int(counts[0]["subjects"]["value"])
                        pattern.distinct_objects = int(counts[0]["objects"]["value"])
                        pattern.examples = _select(
                            context, pattern.witness_query, "structural/witness"
                        )
                    except (SparqlHelperError, ValueError) as error:
                        # A refused recount leaves the pattern unconfirmed, as a recount of 0 does;
                        # a pattern seen in the edges of a sample keeps the counts of the sample.
                        refused_check = str(error)[:300]
                        if provenance is not None and "s" in bindings[key][0]:
                            _sample_counts(pattern, bindings[key])
                            flag(pattern, provenance, ["counts"])
                            refused_check = None
                    else:
                        refused_check = None
                if refused_check or pattern.count < 1 or len(pattern.examples) != 1:
                    # The discovered property sets are not confirmed by the recount or the witness
                    # (Virtuoso: one subject in two groups of a GROUP BY). The pattern is left out
                    # and recorded; the other patterns and the typed schema stand.
                    inconsistent.append(
                        {
                            "property": pattern.property_uri,
                            "subject_kind": pattern.subject_kind,
                            "subject_properties": pattern.subject_properties,
                            "object_kind": pattern.object_kind,
                            "object_properties": pattern.object_properties,
                            "datatype": pattern.datatype,
                            "language": pattern.language,
                            "recount": pattern.count,
                            "witnesses": len(pattern.examples),
                            **({"refused": refused_check} if refused_check else {}),
                        }
                    )
                    continue
                structural.append(pattern)
            return structural, inconsistent

        structural, inconsistent = confirm(rows)
        redo = sorted({i["property"] for i in inconsistent if i["property"] in residuals})
        if redo:
            # Virtuoso can give a subject a wrong property set in a grouped discovery of many
            # subjects (PlantMetWiki rdfs:seeAlso, 2,478 subjects: PC857 grouped with 5 of its 8
            # properties, alone with all of them; job 115327, 681 patterns that recount to 0).
            # The properties with unconfirmed patterns are discovered again from their edges and
            # the (node, property) pairs, without GROUP_CONCAT (_discover_pairs).
            again: dict[str, dict[str, int]] = {}
            for predicate in redo:
                before = sum(1 for i in inconsistent if i["property"] == predicate)
                try:
                    rediscovered = _discover_pairs(context, graph, named, residuals[predicate])
                except (SparqlHelperError, ValueError) as error:
                    logger.warning("Discovery: %s not discovered again: %s", predicate, error)
                    continue
                kept, unconfirmed = confirm(rediscovered)
                structural = [p for p in structural if p.property_uri != predicate] + kept
                inconsistent = [i for i in inconsistent if i["property"] != predicate]
                inconsistent += unconfirmed
                again[predicate] = {
                    "unconfirmed_before": before,
                    "unconfirmed_after": len(unconfirmed),
                }
            if again:
                entry["discovered_again_from_pairs"] = again
        undiscovered = _undiscovered_triples(entry)
        # The properties discovered over a sample are left out of the account: their patterns
        # may not cover their uncovered triples, by design (rdfsolve.mining.sampling).
        census = entry.get("census_properties") or {}
        sampled_props = {p for p, n in census.items() if "discovery_sampled" in n or "sampled" in n}
        expected = (
            entry["uncovered_triples"]
            - undiscovered
            - sum(
                int(census[p].get("uncoveredTriples") or 0)
                for p in sampled_props
                if "discovery_refused" not in census[p]
            )
        )
        recounted = sum(p.count for p in structural if p.property_uri not in sampled_props)
        gaps = []
        if inconsistent:
            entry["inconsistent_patterns"] = inconsistent
            gaps.append(
                QueryFailure(
                    "invalid_response",
                    f"{len(inconsistent)} discovered structural patterns not confirmed by their "
                    "recount and witness (properties: "
                    + ", ".join(sorted({i["property"] for i in inconsistent}))
                    + ")",
                    "structural/count",
                    graph_uris=[graph] if graph else None,
                )
            )
        # A recount counts every edge of its pattern, also the edges of subjects whose discovery
        # was refused when they have a discovered pattern (PlantMetWiki, job 115327: 1,855,293
        # recounted of 1,855,296 uncovered, 354 of them not discovered). So the recounts are
        # consistent between the discovered triples and all uncovered triples.
        total = expected + undiscovered
        if undiscovered:
            entry["undiscovered_triples_in_patterns"] = max(0, recounted - expected)
        if not expected <= recounted <= total:
            entry["unaccounted_triples"] = (
                expected - recounted if recounted < expected else total - recounted
            )
            gaps.append(
                QueryFailure(
                    "invalid_response",
                    f"Structural patterns account for {recounted} of {total} uncovered triples"
                    + (
                        f" ({undiscovered} of them in subjects whose discovery was refused, so "
                        f"between {expected} and {total} are expected)"
                        if undiscovered
                        else ""
                    ),
                    "structural/count",
                    graph_uris=[graph] if graph else None,
                )
            )
        if gaps:
            for gap in gaps:
                logger.warning("Structural consistency: %s", gap.message)
            context.report.record_outcome(QueryOutcome(state="complete", gaps=gaps))
        context.structural_patterns.extend(structural)
        # Unchecked triples whose census a sample answered do not make the graph partial.
        unchecked = entry.get("unchecked_triples") and any(
            "refused" in n and "sampled" not in n for n in census.values()
        )
        entry.update(
            undiscovered_triples=undiscovered,
            state="partial"
            if unchecked or undiscovered or gaps
            else "sampled"
            if sampled_props
            else "complete",
            pattern_count=len(structural),
            representation="exact_property_sets",
        )
