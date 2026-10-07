"""Tested navigation paths from a row store, by joins instead of SPARQL probes.

The same search as navigation.find_tested_paths, with the same edges (the distinct patterns
of the schema) and the same rules; the queries are replaced by joins over the rows:

- an edge (C, p, D) is a table of (s, o): rows of p whose subject is a member of C and whose
  object is a member of D, a literal of the edge's datatype (integer and decimal families as
  in navigation._same_datatype), a blank node, or an IRI without a type ("Resource");
- a path is a table of (start, end) pairs; extending it is one join of its ends with the
  edges that leave its last class (one join for all of them, as the extension query does with
  VALUES); an edge is matched when a start reaches it; a path is extended only when matched,
  and does not repeat an edge;
- the matched sources of a path are its distinct starts; its source count is the number of
  members of its first class.

The search deepens one length at a time (paths of length h are searched depth first up to h),
so that a length is complete when its search ends within the budget, and only the pairs of the
current branch are held in memory. Nodes are QLever's ids. Where many instances meet (a
branch's pairs would pass PAIR_LIMIT), the branch carries the set of nodes it reaches: it still
decides exactly which edges are followed, and the starts of a path found that way are counted
backwards along the path. The budget is checked before every join.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from rdfsolve.mining.navigation import _same_datatype, _schema_graph
from rdfsolve.mining.scan import RowStore, StoreView, _bare
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary

if TYPE_CHECKING:
    import polars as pl

    from rdfsolve.schema_models.core import MinedSchema

__all__ = ["find_tested_paths_in_store"]

# A branch whose next (start, end) pairs would pass this many rows carries the set of nodes it
# reaches instead (find_tested_paths_in_store.extend).
PAIR_LIMIT = 20_000_000
# Paths of one length from one start class that are kept (find_tested_paths_in_store); the
# others are counted (omitted_by_class) and not extended. onco (job 115871) kept 18.9 M
# matched paths in its 1800 s budget, a 4.96 GB schema, and its outputs ran out of memory.
PATHS_PER_CLASS = 1000


def find_tested_paths_in_store(
    schema: MinedSchema,
    store: RowStore | StoreView,
    *,
    max_hops: int = 5,
    budget_s: float = 1800.0,
    members: Mapping[str, Sequence[str]] | None = None,
    clock: Callable[[], float] = time.monotonic,
    max_paths_per_class: int | None = None,
) -> NavigationSummary:
    """Find the paths of two to *max_hops* steps that instances of the data follow.

    *members* gives the member terms of each group of ontology terms, which the data use as
    types instead of the group. At most *max_paths_per_class* (PATHS_PER_CLASS) matched paths
    of each length are kept for each start class, the first in the search's order; the others
    are counted by length and class (omitted_by_class, truncated_lengths) and not extended, so
    that the schema, its outputs and the search stay bounded whatever the time budget.
    """
    import polars as pl

    cap = PATHS_PER_CLASS if max_paths_per_class is None else max_paths_per_class
    if not 2 <= max_hops <= 6 or budget_s < 0 or cap < 1:
        raise ValueError("Use 2..6 hops, a nonnegative budget and at least one path a class")
    members = members or {}
    unique, _outgoing, suffix = _schema_graph(schema, max_hops)
    keys = sorted(unique)
    index = {k: i for i, k in enumerate(keys)}
    # The scope of the schema, as find_tested_paths reads it: edges in the RDF merge of its data
    # graphs, types also from its type graphs and type context graphs (a per-graph schema: its
    # graph, with the types of the source's graphs).
    graphs = list(schema.about.graph_uris or [])
    context = (schema.about.type_graph_uris or []) + (schema.about.type_context_graph_uris or [])
    store = store.base.view(graphs, context) if graphs else store.base
    # Instances by QLever id (16 bytes a row); the source counts read the members by term.
    types = store.type_ids().select("sid", c=_bare(pl.col("c").cast(pl.String))).unique().collect()
    typed = types.select("sid").unique()
    roots = store.members().select("s", c=_bare(pl.col("c"))).unique().collect()

    def instances(cls: str) -> pl.DataFrame:
        """Return the ids (sid) of the instances of *cls* and of the terms it stands for."""
        terms = [cls, *members.get(cls, ())]
        return types.filter(pl.col("c").is_in(terms)).select("sid").unique()

    loaded: dict[str, pl.DataFrame] = {}

    def predicate_rows(predicate: str) -> pl.DataFrame:
        """Return the rows of a predicate, read once for all the edges that use it."""
        if predicate not in loaded:
            loaded.clear()  # the edges are built predicate by predicate: keep one in memory
            loaded[predicate] = store.rows(predicate).select("sid", "oid", "kind", "d").collect()
        return loaded[predicate]

    def step(edge: Any) -> pl.DataFrame:
        """Return the (s, o) id pairs of the rows that match *edge*'s pattern."""
        if edge.property_uri not in store.predicates:
            return pl.DataFrame(schema={"s": pl.UInt64, "o": pl.UInt64})
        rows = predicate_rows(edge.property_uri).join(
            instances(edge.subject_class), on="sid", how="semi"
        )
        target = edge.object_class
        if target == "Literal":
            found = [
                d
                for d in rows.filter(pl.col("kind") == "literal")["d"].unique().to_list()
                if d and _same_datatype(edge.datatype, d)
            ]
            rows = rows.filter((pl.col("kind") == "literal") & pl.col("d").is_in(found))
        elif target == "BlankNode":
            rows = rows.filter(pl.col("kind") == "bnode")
        elif target == "Resource":
            rows = rows.filter(pl.col("kind") == "iri").join(
                typed, left_on="oid", right_on="sid", how="anti"
            )
        else:
            rows = rows.join(instances(target).rename({"sid": "oid"}), on="oid", how="semi")
        return rows.select(s="sid", o="oid").unique()

    # Edges by predicate, so that each predicate is read once; the budget counts this too.
    deadline = clock() + budget_s
    steps: dict[int, pl.DataFrame] = {}
    for k in sorted(keys, key=lambda k: (k[1], k)):
        if clock() >= deadline:
            break
        steps[index[k]] = step(unique[k])
    keys = [k for k in keys if index[k] in steps]
    leaving: dict[str, pl.DataFrame] = {}
    for k in keys:
        frame = steps[index[k]].with_columns(e=pl.lit(index[k], pl.Int32))
        leaving[k[0]] = pl.concat([leaving[k[0]], frame]) if k[0] in leaving else frame
    fanout = {c: frame.group_by("s").len() for c, frame in leaving.items()}
    # The source count of a path: the members of its first class (_subject_type_pattern).
    sources = {
        c: roots.filter(pl.col("c").is_in([c, *members.get(c, ())]))["s"].n_unique()
        for c in {k[0] for k in keys}
    }
    tested: Counter[int] = Counter()
    matched: Counter[int] = Counter()
    found: dict[tuple[int, ...], int] = {}
    kept: Counter[tuple[int, str]] = Counter()
    omitted: dict[int, Counter[str]] = {}
    stopped = len(keys) < len(index)

    def followed(route: tuple[int, ...]) -> int:
        """Count the starts that follow the whole route, from its end backwards (exact)."""
        reach = steps[route[-1]].select("s").unique()
        for i in reversed(route[:-1]):
            reach = steps[i].join(reach.rename({"s": "o"}), on="o", how="semi").select("s").unique()
        return reach.height

    def extend(path: tuple[int, ...], pairs: pl.DataFrame, limit: int) -> None:
        """Extend *path* from its (start, end) pairs, or from its ends alone (column end only):
        a branch whose pairs would pass PAIR_LIMIT carries the set of nodes it reaches, which
        decides exactly which edges are followed; its starts are then counted backwards.
        """
        nonlocal stopped
        hops = len(path) + 1
        last = keys[path[-1]][2]
        if hops > limit or last not in leaving or stopped:
            return
        if clock() >= deadline:
            stopped = True
            return
        candidates = [index[k] for k in keys if k[0] == last and index[k] not in path]
        if not candidates:
            return
        if "start" in pairs.columns:
            size = (
                pairs.group_by("end")
                .len()
                .join(fanout[last], left_on="end", right_on="s")
                .select((pl.col("len").cast(pl.UInt64) * pl.col("len_right").cast(pl.UInt64)).sum())
                .item()
            )
            if size and size > PAIR_LIMIT:
                pairs = pairs.select("end").unique()
        edges = leaving[last].filter(pl.col("e").is_in(candidates))
        joined = pairs.join(edges, left_on="end", right_on="s")
        if "start" in pairs.columns:
            hits = dict(joined.group_by("e").agg(pl.col("start").n_unique()).iter_rows())
        else:
            hits = dict.fromkeys(joined["e"].unique().to_list())
        if hops == limit:
            tested[hops] += len(candidates)
        for e in sorted(hits):
            if clock() >= deadline:
                stopped = True
                return
            route = (*path, e)
            if hops == limit:
                matched[hops] += 1
                start = keys[route[0]][0]
                if kept[(hops, start)] >= cap:
                    omitted.setdefault(hops, Counter())[start] += 1
                    continue
                kept[(hops, start)] += 1
                found[route] = hits[e] if hits[e] is not None else followed(route)
            elif route in found:
                rows = joined.filter(pl.col("e") == e)
                following = (
                    rows.select("start", end="o").unique()
                    if "start" in rows.columns
                    else rows.select(end="o").unique()
                )
                extend(route, following, limit)

    complete: list[int] = []
    for limit in range(2, max_hops + 1):
        for k in keys:
            first = steps[index[k]]
            if first.height and not stopped:
                extend((index[k],), first.select(start="s", end="o").unique(), limit)
        if stopped:
            break
        complete.append(limit)
    observed_at = datetime.now(UTC).isoformat()
    paths = [
        NavigationPath(
            steps=[unique[keys[i]] for i in route],
            evidence="instance_tested",
            instance_support="matched",
            source_count=sources.get(keys[route[0]][0]),
            matched_sources=n,
            graph_uris=graphs,
            type_context_graph_uris=context,
            observed_at=observed_at,
        )
        for route, n in sorted(found.items())
    ]
    through = {s.subject_class for r in paths for s in r.steps} | {
        s.object_class for r in paths for s in r.steps
    }
    stop: Literal["budget"] | None = "budget" if stopped else None
    return NavigationSummary(
        member_terms={c: list(members[c]) for c in sorted(through) if c in members},
        max_hops=max_hops,
        max_paths_per_length=cap,
        edge_count=len(unique),
        walk_counts={hops: sum(suffix[hops].values()) for hops in range(1, max_hops + 1)},
        paths=paths,
        truncated_lengths=sorted(omitted),
        omitted_by_class={h: dict(sorted(c.items())) for h, c in sorted(omitted.items())},
        strategy="tested",
        tested_by_length=dict(tested),
        matched_by_length=dict(matched),
        complete_lengths=complete,
        budget_s=budget_s,
        stop_reason=stop,
        query_count=0,
        failed_queries=0,
    )
