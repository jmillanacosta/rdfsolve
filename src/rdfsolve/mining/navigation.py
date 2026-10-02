"""Compose bounded navigation routes from an existing class graph."""

from __future__ import annotations

import logging
import time
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal

from rdfsolve.mining.query_builders import _graph_scope, _subject_type_pattern, _type_pattern
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary
from rdfsolve.schema_models.pattern import SchemaPattern

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)


def discover_paths_with_fallback(
    schema: MinedSchema,
    *,
    max_hops: int = 5,
    min_hops: int = 3,
    max_paths_per_length: int = 100,
    helper: SparqlHelper | None = None,
    probe_limit: int = 0,
) -> NavigationSummary:
    """Compose the longest routes the schema supports, stepping down to *min_hops*.

    Routes of a given length only exist when the mined edges chain that far, so
    each attempt is checked for a route of its own length and the hop bound is
    lowered when none was found. The last attempt is returned even when it is
    empty, so the caller always learns how deep the schema goes.
    """
    if not 2 <= min_hops <= max_hops <= 6:
        raise ValueError("Use 2..6 hops with min_hops no greater than max_hops")

    def compose(hops: int) -> NavigationSummary:
        """Compose bounded routes for one schema."""
        return discover_paths(
            schema,
            max_hops=hops,
            max_paths_per_length=max_paths_per_length,
            helper=helper,
            probe_limit=probe_limit,
        )

    hops = max_hops
    summary = compose(hops)
    while hops > min_hops and not any(len(route.steps) == hops for route in summary.paths):
        logger.info("No %d-hop routes composed; retrying with %d hops", hops, hops - 1)
        hops -= 1
        summary = compose(hops)
    if not any(len(route.steps) == hops for route in summary.paths):
        logger.info("No %d-hop routes composed at the lowest bound", hops)
    return summary


def _schema_graph(
    schema: MinedSchema, max_hops: int
) -> tuple[
    dict[tuple[str, str, str, str], SchemaPattern],
    dict[str, list[SchemaPattern]],
    list[dict[str, int]],
]:
    """Return the distinct edges of a schema, the edges from each class, and the walk counts.

    suffix[h][class] counts all length-h walks from a class in the schema graph.
    """
    unique: dict[tuple[str, str, str, str], SchemaPattern] = {}
    for pattern in schema.patterns:
        if pattern.count == 0:
            continue
        key = (
            pattern.subject_class,
            pattern.property_uri,
            pattern.object_class,
            pattern.datatype or "",
        )
        if key in unique and unique[key].count != pattern.count:
            # Conflicting duplicate counts cannot be added or chosen safely.
            unique[key] = pattern.model_copy(
                update={"count": None, "distinct_subjects": None, "distinct_objects": None}
            )
        else:
            unique.setdefault(key, pattern)
    outgoing: dict[str, list[SchemaPattern]] = defaultdict(list)
    for key in sorted(unique):
        outgoing[key[0]].append(unique[key])

    # suffix[h][class] counts all length-h walks from this class.
    # Reuse these counts to prune branches that cannot reach the requested length.
    suffix: list[dict[str, int]] = [{}]
    for hops in range(1, max_hops + 1):
        suffix.append(
            {
                start: sum(
                    1 if hops == 1 else suffix[hops - 1].get(edge.object_class, 0) for edge in edges
                )
                for start, edges in outgoing.items()
            }
        )
    return unique, outgoing, suffix


def discover_paths(
    schema: MinedSchema,
    *,
    max_hops: int = 3,
    max_paths_per_length: int = 100,
    helper: SparqlHelper | None = None,
    probe_limit: int = 0,
) -> NavigationSummary:
    """Count schema walks with dynamic programming; retain a bounded sample.

    Optional helper probes measure bounded candidates. Cycles are allowed up to max_hops.
    A shared class is a possible join, not evidence of shared entities.
    Counts are exact for the supplied schema graph, not the source dataset.
    Samples rotate across starting classes. Each step prefers predicates and
    classes not yet used in that route; ties use lexical order.
    They are bounded coverage samples, not frequency estimates.
    """
    if probe_limit < 0 or (probe_limit and helper is None):
        raise ValueError("Path probes require a helper and a nonnegative probe_limit")
    if not 2 <= max_hops <= 6 or max_paths_per_length < 0:
        raise ValueError("Use 2..6 hops and a nonnegative path limit")
    unique, outgoing, suffix = _schema_graph(schema, max_hops)

    def walks(
        start: str, remaining: int, prefix: tuple[SchemaPattern, ...]
    ) -> Iterator[NavigationPath]:
        """Extend a class route to the requested length."""
        predicates = {edge.property_uri for edge in prefix}
        classes = {edge.subject_class for edge in prefix} | {start}
        edges = sorted(
            outgoing.get(start, []),
            key=lambda edge: (
                edge.property_uri in predicates,
                edge.object_class in classes,
                edge.property_uri,
                edge.object_class,
                edge.datatype or "",
            ),
        )
        for edge in edges:
            if remaining == 1:
                yield NavigationPath(steps=[*prefix, edge])
            elif suffix[remaining - 1].get(edge.object_class, 0):
                yield from walks(edge.object_class, remaining - 1, (*prefix, edge))

    totals = {hops: sum(suffix[hops].values()) for hops in range(1, max_hops + 1)}
    paths: list[NavigationPath] = []
    omitted: dict[int, dict[str, int]] = {}
    for hops in range(2, max_hops + 1):
        pending = deque(walks(start, hops, ()) for start in sorted(outgoing) if suffix[hops][start])
        selected: Counter[str] = Counter()
        while pending and sum(selected.values()) < max_paths_per_length:
            iterator = pending.popleft()
            route = next(iterator, None)
            if route is not None:
                paths.append(route)
                selected[route.steps[0].subject_class] += 1
                pending.append(iterator)
        omitted[hops] = {
            start: count - selected[start]
            for start, count in suffix[hops].items()
            if count > selected[start]
        }
    if helper is not None:
        for route in paths[:probe_limit]:
            observe_path(
                route,
                helper,
                schema.about.graph_uris or [],
                type_context_graph_uris=(schema.about.type_graph_uris or [])
                + (schema.about.type_context_graph_uris or []),
            )
    return NavigationSummary(
        max_hops=max_hops,
        max_paths_per_length=max_paths_per_length,
        edge_count=len(unique),
        walk_counts=totals,
        paths=paths,
        omitted_by_class=omitted,
        truncated_lengths=[
            hops for hops in range(2, max_hops + 1) if totals[hops] > max_paths_per_length
        ],
        probe_limit=probe_limit,
        probe_selection="retained_prefix",
    )


def probe_paths(
    schema: MinedSchema,
    paths: Sequence[NavigationPath],
    *,
    helper: SparqlHelper,
) -> list[NavigationPath]:
    """Probe selected retained routes and update their stored observations."""
    if schema.navigation is None:
        raise ValueError("Discover candidate paths before selecting probes")
    retained = {route.signature(): route for route in schema.navigation.paths}
    signatures = list(dict.fromkeys(route.signature() for route in paths))
    if any(signature not in retained for signature in signatures):
        raise ValueError("Choose paths retained in schema.navigation")
    selected = [retained[signature] for signature in signatures]
    for route in selected:
        observe_path(
            route,
            helper,
            schema.about.graph_uris or [],
            type_context_graph_uris=(schema.about.type_graph_uris or [])
            + (schema.about.type_context_graph_uris or []),
        )
    schema.navigation.probe_selection = "explicit"
    schema.navigation.probe_limit = len(selected)
    return selected


def _path_pattern(
    route: NavigationPath,
    graphs: list[str],
    type_context_graph_uris: list[str] | None,
    *,
    include_unmatched: bool = True,
) -> tuple[str, str]:
    """Return dataset clauses and the typed path pattern."""
    from rdfsolve.schema_models.paths import absolute_iri

    def iri(value: str) -> str:
        """Serialize an absolute IRI as a SPARQL term."""
        return "<" + absolute_iri(value) + ">"

    for graph in [*graphs, *(type_context_graph_uris or [])]:
        absolute_iri(graph)
    dataset, _, _ = _graph_scope(graphs, type_context_graph_uris)
    body = []
    for i, step in enumerate(route.steps):
        node = f"?n{i + 1}"
        body.append(f"?n{i} {iri(step.property_uri)} {node} .")
        if step.object_class == "Literal":
            condition = f"isLiteral({node})"
            if step.datatype:
                condition += f" && datatype({node}) = {iri(step.datatype)}"
            body.append(f"FILTER({condition})")
        elif step.object_class in {"Resource", "BlankNode"}:
            body.append(
                f"FILTER({'isIRI' if step.object_class == 'Resource' else 'isBlank'}({node}))"
            )
        else:
            body.append(_type_pattern(node, iri(step.object_class), type_context_graph_uris))
    joined = " ".join(body)
    if include_unmatched:
        joined = f"OPTIONAL {{ {joined} }}"
    root = _subject_type_pattern("?n0", iri(route.steps[0].subject_class), type_context_graph_uris)
    return dataset, f"{root} {joined}"


def path_query(
    route: NavigationPath,
    graphs: list[str],
    *,
    type_context_graph_uris: list[str] | None = None,
    include_unmatched: bool = True,
) -> str:
    """Select source and intermediate RDF terms for one typed route."""
    dataset, body = _path_pattern(
        route, graphs, type_context_graph_uris, include_unmatched=include_unmatched
    )
    nodes = " ".join(f"?n{i}" for i in range(len(route.steps) + 1))
    return f"SELECT DISTINCT {nodes} {dataset} WHERE {{ {body} }} ORDER BY {nodes}"


def observe_path(
    route: NavigationPath,
    helper: SparqlHelper,
    graphs: list[str],
    *,
    type_context_graph_uris: list[str] | None = None,
) -> None:
    """Measure whole-route support including focus nodes with no matching endpoint."""
    from datetime import datetime, timezone

    dataset, scoped = _path_pattern(route, graphs, type_context_graph_uris)
    query = (
        "SELECT (COUNT(*) AS ?sources) (SUM(IF(?degree > 0, 1, 0)) AS ?matched) "
        f"(MIN(?degree) AS ?minimum) (MAX(?degree) AS ?maximum) {dataset} WHERE {{ "
        f"{{ SELECT ?n0 (COUNT(DISTINCT ?n{len(route.steps)}) AS ?degree) "
        f"WHERE {{ {scoped} }} GROUP BY ?n0 }} }}"
    )
    route.source_count = route.matched_sources = route.min_count = route.max_count = None
    route.error = None
    route.graph_uris = list(graphs)
    route.type_context_graph_uris = list(type_context_graph_uris or [])
    route.query = query
    route.observed_at = datetime.now(timezone.utc).isoformat()
    try:
        result = helper.select_with_fallback(query, purpose="mine joined path support")
        rows = result.get("results", {}).get("bindings", [])
        if len(rows) != 1 or "sources" not in rows[0]:
            raise ValueError("Path statistics returned no aggregate row")
        row = rows[0]
        route.source_count = int(row["sources"]["value"])
        if not route.source_count:
            route.matched_sources = 0
            route.instance_support = "no_sources"
        else:
            route.matched_sources = int(row["matched"]["value"])
            route.min_count = int(row["minimum"]["value"])
            route.max_count = int(row["maximum"]["value"])
            route.instance_support = "matched" if route.matched_sources else "no_match"
    except Exception as exc:
        route.source_count = route.matched_sources = route.min_count = route.max_count = None
        route.instance_support = "timeout" if "timeout" in type(exc).__name__.lower() else "error"
        route.error = f"{type(exc).__name__}: {exc}"
        import logging

        logging.getLogger(__name__).warning("Path support probe failed: %s", route.error)


# Integer and decimal types that QLever reports as xsd:int and xsd:double: a restored datatype
# of the schema (for example xsd:integer) is the same step as the reported one.
_XSD = "http://www.w3.org/2001/XMLSchema#"
_INTEGERS = {
    _XSD + t
    for t in [
        "int",
        "integer",
        "long",
        "short",
        "byte",
        "nonNegativeInteger",
        "positiveInteger",
        "nonPositiveInteger",
        "negativeInteger",
        "unsignedLong",
        "unsignedInt",
        "unsignedShort",
        "unsignedByte",
    ]
}
_DECIMALS = {_XSD + t for t in ("decimal", "double", "float")}


def _same_datatype(schema_type: str | None, found: str) -> bool:
    """Tell whether a literal of the found datatype is a value of a step with *schema_type*."""
    if not schema_type or schema_type == found:
        return True
    return any(schema_type in family and found in family for family in (_INTEGERS, _DECIMALS))


def _typed(
    node: str,
    cls: str,
    context: list[str] | None,
    members: Mapping[str, Sequence[str]],
    subject: bool = False,
) -> str:
    """Match a node of a class, or of any member term of a group of ontology terms."""
    from rdfsolve.schema_models.paths import absolute_iri

    terms = members.get(cls)
    if terms:
        variable = f"?_t{node[1:]}"
        values = " ".join("<" + absolute_iri(term) + ">" for term in terms)
        pattern = (_subject_type_pattern if subject else _type_pattern)(node, variable, context)
        return f"VALUES {variable} {{ {values} }} {pattern}"
    iri = "<" + absolute_iri(cls) + ">"
    return (_subject_type_pattern if subject else _type_pattern)(node, iri, context)


def _prefix_pattern(
    steps: Sequence[SchemaPattern],
    context: list[str] | None,
    members: Mapping[str, Sequence[str]],
    root: bool = True,
) -> str:
    """Return the pattern of the start class (with *root*) and the steps of a path.

    A step to a class types its value; a step to a literal tests the datatype (with the
    integer and decimal families of _same_datatype); a step to an IRI without a type, or to a
    blank node, tests the node kind, blank nodes first.
    """
    from rdfsolve.schema_models.paths import absolute_iri

    body = [_typed("?n0", steps[0].subject_class, context, members, subject=True)] if root else []
    for i, step in enumerate(steps):
        node = f"?n{i + 1}"
        body.append(f"?n{i} <{absolute_iri(step.property_uri)}> {node} .")
        if step.object_class == "Literal":
            family = next((f for f in (_INTEGERS, _DECIMALS) if step.datatype in f), None)
            types = sorted(family) if family else [step.datatype] if step.datatype else []
            test = f"isLiteral({node})"
            if types:
                test += f" && DATATYPE({node}) IN ({', '.join(f'<{t}>' for t in types)})"
            body.append(f"FILTER({test})")
        elif step.object_class == "BlankNode":
            body.append(f"FILTER(isBlank({node}))")
        elif step.object_class == "Resource":
            body.append(f"FILTER(!isBlank({node}) && isIRI({node}))")
            body.append(f"FILTER NOT EXISTS {{ {_type_pattern(node, '?_anyType', context)} }}")
        else:
            body.append(_typed(node, step.object_class, context, members))
    return " ".join(body)


def _support_query(
    steps: Sequence[SchemaPattern],
    dataset: str,
    context: list[str] | None,
    members: Mapping[str, Sequence[str]],
) -> str:
    """Count the start instances of a path and those that follow the whole path (one row)."""
    root = _typed("?n0", steps[0].subject_class, context, members, subject=True)
    rest = _prefix_pattern(steps, context, members, root=False)
    end = f"?n{len(steps)}"
    return (
        "SELECT (COUNT(*) AS ?sources) (SUM(IF(?degree > 0, 1, 0)) AS ?matched) "
        f"{dataset} WHERE {{ {{ SELECT ?n0 (COUNT(DISTINCT {end}) AS ?degree) "
        f"WHERE {{ {root} OPTIONAL {{ {rest} }} }} GROUP BY ?n0 }} }}"
    )


def _extension_query(
    prefix: Sequence[SchemaPattern],
    candidates: Sequence[SchemaPattern],
    dataset: str,
    context: list[str] | None,
    members: Mapping[str, Sequence[str]],
) -> str:
    """Count the start instances that follow a path and then each candidate property.

    One row for each property and value type: the class of the value (?t), or the node kind or
    datatype of a value without a type (?kind). Blank nodes are tested with isBlank first,
    because Virtuoso reads its blank nodes as IRIs with isIRI in a BIND.
    """
    from rdfsolve.schema_models.paths import absolute_iri

    end = f"?n{len(prefix)}"
    properties = " ".join(sorted({"<" + absolute_iri(e.property_uri) + ">" for e in candidates}))
    return (
        f"SELECT ?p ?t ?kind (COUNT(DISTINCT ?n0) AS ?starts) {dataset} WHERE {{ "
        f"{_prefix_pattern(prefix, context, members)} "
        f"VALUES ?p {{ {properties} }} {end} ?p ?x . "
        f"OPTIONAL {{ {_type_pattern('?x', '?t', context)} }} "
        'BIND(IF(isBlank(?x), "blank", IF(isLiteral(?x), STR(DATATYPE(?x)), "iri")) AS ?kind) '
        "} GROUP BY ?p ?t ?kind"
    )


def _matched_starts(
    edge: SchemaPattern, rows: Sequence[Mapping[str, Any]], members: Mapping[str, Sequence[str]]
) -> int:
    """Return the most start instances that follow a candidate step in the answer rows."""
    best = 0
    for row in rows:
        if row.get("p", {}).get("value") != edge.property_uri:
            continue
        found_type = row.get("t", {}).get("value")
        kind = row.get("kind", {}).get("value", "")
        if edge.object_class == "Literal":
            hit = kind not in {"blank", "iri"} and _same_datatype(edge.datatype, kind)
        elif edge.object_class == "BlankNode":
            hit = kind == "blank"
        elif edge.object_class == "Resource":
            hit = kind == "iri" and found_type is None
        else:
            hit = found_type in set(members.get(edge.object_class, ())) | {edge.object_class}
        if hit:
            best = max(best, int(row.get("starts", {}).get("value", 0)))
    return best


def find_tested_paths(
    schema: MinedSchema,
    helper: SparqlHelper,
    *,
    max_hops: int = 5,
    budget_s: float = 1800.0,
    members: Mapping[str, Sequence[str]] | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> NavigationSummary:
    """Find the paths of two to *max_hops* steps that instances of the data follow.

    The paths are tested one step at a time. For each matched path (at first each edge of the
    schema), one query counts the start instances that follow it and then each property that the
    schema gives for its last class, by value type. A path is extended only from a matched path,
    because a longer path cannot match when its start does not; a path does not repeat an edge.
    Only matched paths are kept, with their number of matched start instances. The search stops
    when the time budget is spent; a length is complete when every path of the shorter length
    was extended without a failed query. Each query has the rest of the budget, one try and no
    page recovery: a query that does not answer in that time is a failed query (Bgee run 13
    waited for one extension query 2 h at a time, in ever smaller pages, until the job ended). *members* gives the member terms of each group of
    ontology terms (the report of the mining), which the data use as types instead of the group.
    """
    from datetime import datetime, timezone

    from rdfsolve.sparql_helper import SparqlHelperError

    if not 2 <= max_hops <= 6 or budget_s < 0:
        raise ValueError("Use 2..6 hops and a nonnegative budget")
    members = members or {}
    unique, outgoing, suffix = _schema_graph(schema, max_hops)
    graphs = list(schema.about.graph_uris or [])
    context = (schema.about.type_graph_uris or []) + (schema.about.type_context_graph_uris or [])
    dataset, _, _ = _graph_scope(graphs, context)
    deadline = clock() + budget_s

    def key(edge: SchemaPattern) -> tuple[str, str, str, str]:
        """Identify an edge of the schema graph."""
        return (edge.subject_class, edge.property_uri, edge.object_class, edge.datatype or "")

    tested: Counter[int] = Counter()
    matched: Counter[int] = Counter()
    kept: list[NavigationPath] = []
    complete: list[int] = []
    queries = failed = 0
    stop: Literal["budget"] | None = None
    frontier: list[tuple[SchemaPattern, ...]] = [
        (unique[k],) for k in sorted(unique) if unique[k].object_class in outgoing
    ]
    for hops in range(2, max_hops + 1):
        extended: list[tuple[SchemaPattern, ...]] = []
        whole = not complete or complete[-1] == hops - 1
        for prefix in frontier:
            used = {key(step) for step in prefix}
            candidates = [e for e in outgoing[prefix[-1].object_class] if key(e) not in used]
            if not candidates:
                continue
            if clock() >= deadline:
                stop, whole = "budget", False
                break
            query = _extension_query(prefix, candidates, dataset, context or None, members)
            queries += 1
            try:
                with helper.budget(max(1.0, deadline - clock())):
                    result = helper.select_with_fallback(query, purpose="navigation/tested-paths")
            except (SparqlHelperError, TimeoutError, OSError) as error:
                logger.warning("Path extension query failed: %s", error)
                failed += 1
                whole = False
                continue
            rows = result.get("results", {}).get("bindings", [])
            tested[hops] += len(candidates)
            observed_at = datetime.now(timezone.utc).isoformat()
            for edge in candidates:
                if not _matched_starts(edge, rows, members):
                    continue
                # The path is kept when its own query matches; support_query makes that query
                # again (the release validation repeats it), so it is not stored with the path.
                if clock() >= deadline:
                    stop, whole = "budget", False
                    break
                steps = [*prefix, edge]
                support = _support_query(steps, dataset, context or None, members)
                queries += 1
                try:
                    with helper.budget(max(1.0, deadline - clock())):
                        answer = helper.select_with_fallback(
                            support, purpose="navigation/path-support"
                        )
                    row = answer.get("results", {}).get("bindings", [{}])[0]
                    sources = int(row["sources"]["value"])
                    followed = int(row["matched"]["value"]) if sources else 0
                except (SparqlHelperError, TimeoutError, OSError, LookupError, ValueError) as error:
                    logger.warning("Path support query failed: %s", error)
                    failed += 1
                    whole = False
                    continue
                if not followed:
                    continue
                matched[hops] += 1
                kept.append(
                    NavigationPath(
                        steps=steps,
                        evidence="instance_tested",
                        instance_support="matched",
                        source_count=sources,
                        matched_sources=followed,
                        graph_uris=graphs,
                        type_context_graph_uris=context,
                        observed_at=observed_at,
                    )
                )
                if edge.object_class in outgoing:
                    extended.append((*prefix, edge))
            if stop:
                break
        if whole:
            complete.append(hops)
        if stop:
            break
        frontier = extended
    through = {s.subject_class for r in kept for s in r.steps} | {
        s.object_class for r in kept for s in r.steps
    }
    return NavigationSummary(
        member_terms={c: list(members[c]) for c in sorted(through) if c in members},
        max_hops=max_hops,
        max_paths_per_length=0,
        edge_count=len(unique),
        walk_counts={hops: sum(suffix[hops].values()) for hops in range(1, max_hops + 1)},
        paths=kept,
        strategy="tested",
        tested_by_length=dict(tested),
        matched_by_length=dict(matched),
        complete_lengths=complete,
        budget_s=budget_s,
        stop_reason=stop,
        query_count=queries,
        failed_queries=failed,
    )


def support_query(route: NavigationPath, summary: NavigationSummary | None = None) -> str:
    """Return the query that counts the start instances of a tested path and those that follow it.

    It is the query that find_tested_paths sent for the path (one row: ?sources, ?matched). The
    member terms of groups of ontology terms are taken from *summary*.
    """
    context = list(route.type_context_graph_uris)
    dataset, _, _ = _graph_scope(list(route.graph_uris), context)
    members = summary.member_terms if summary is not None else {}
    return _support_query(route.steps, dataset, context or None, members)
