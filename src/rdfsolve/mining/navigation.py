"""Compose bounded navigation routes from an existing class graph."""

from __future__ import annotations

import logging
from collections import Counter, defaultdict, deque
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING

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
