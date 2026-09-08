"""Compose bounded navigation routes from an existing class graph."""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from collections.abc import Iterator
from typing import TYPE_CHECKING

from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary
from rdfsolve.schema_models.pattern import SchemaPattern

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema


def discover_paths(
    schema: MinedSchema, *, max_hops: int = 3, max_paths_per_length: int = 100
) -> NavigationSummary:
    """Count schema walks with dynamic programming; retain a bounded sample.

    No endpoint requests are made. Cycles are allowed up to max_hops.
    A shared class is a possible join, not evidence of shared entities.
    Counts are exact for the supplied schema graph, not the source dataset.
    Samples rotate across starting classes. Each step prefers predicates and
    classes not yet used in that route; ties use lexical order.
    They are bounded coverage samples, not frequency estimates.
    """
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
    )
