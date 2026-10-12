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

import logging
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

logger = logging.getLogger(__name__)

# Seconds between two progress lines while the edges are read.
PROGRESS_SECONDS = 60.0

# A branch whose next (start, end) pairs would pass this many rows carries the set of nodes it
# reaches instead (find_tested_paths_in_store.extend).
PAIR_LIMIT = 20_000_000
# Paths of one length from one start class that are kept (find_tested_paths_in_store); the
# others are counted (omitted_by_class) and not extended: without a cap, millions of matched
# paths can fill the schema and run its outputs out of memory.
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
    start_sample: int | None = None,
) -> NavigationSummary:
    """Find the paths of two to *max_hops* steps that instances of the data follow.

    *members* gives the member terms of each group of ontology terms, which the data use as
    types instead of the group. At most *max_paths_per_class* (PATHS_PER_CLASS) matched paths
    of each length are kept for each start class, the first in the search's order; the others
    are counted by length and class (omitted_by_class, truncated_lengths) and not extended, so
    that the schema, its outputs and the search stay bounded whatever the time budget.

    With *start_sample*, each start class is tested on at most that many of its instances
    (chosen by a hash of their ids, the same on every run), and the edges are read only for
    the nodes those starts reach, hop by hop, instead of for every instance: a path is kept
    when a sampled start follows it, its matched_sources count sampled starts, and its
    source_count is the size of the sample (the summary records start_sample).
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
    # Instances by QLever id, the classes as categories (12 bytes a row); the type table is
    # never held as text.
    type_rows = store.graph_types().select(
        "sid", c=_bare(pl.col("c").cast(pl.String)).cast(pl.Categorical)
    )
    if not getattr(store, "distinct_types", False):
        type_rows = type_rows.unique()
    types = type_rows.collect(engine="streaming")
    typed = types.select("sid").unique()
    # The instances of each class, split once: filtering the whole table for each edge read
    # every row of it once an edge.
    by_class = {
        str(key[0]): frame.select("sid")
        for key, frame in types.partition_by("c", as_dict=True, include_key=False).items()
    }
    del types

    def instances(cls: str) -> pl.DataFrame:
        """Return the ids (sid) of the instances of *cls* and of the terms it stands for."""
        found = [by_class[t] for t in (cls, *members.get(cls, ())) if t in by_class]
        if not found:
            return pl.DataFrame(schema={"sid": pl.UInt64})
        if len(found) == 1:
            return found[0]
        joined: pl.DataFrame = pl.concat(found).unique()
        return joined

    def source_count(cls: str) -> int:
        """Return the members of *cls* and of the terms it stands for (_subject_type_pattern):
        its instances, or, with type context graphs, those with an edge in the data graphs.
        """
        if not getattr(store, "context", ()):
            return instances(cls).height
        terms = [cls, *members.get(cls, ())]
        return int(
            store.members()
            .filter(_bare(pl.col("c")).is_in(terms))
            .select(pl.col("s").n_unique())
            .collect(engine="streaming")
            .item()
        )

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
        return matching(rows, edge)

    def matching(rows: pl.DataFrame, edge: Any, *, few: bool = False) -> pl.DataFrame:
        """Return the (s, o) id pairs of *rows* whose object matches *edge*'s pattern. With
        *few* rows, the instances and typed nodes are read for their objects only.
        """
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
            iris = rows.filter(pl.col("kind") == "iri")
            known = typed
            if few:
                known = typed.join(iris.select(sid="oid").unique(), on="sid", how="semi")
            rows = iris.join(known, left_on="oid", right_on="sid", how="anti")
        else:
            members_of = instances(target)
            if few:
                members_of = members_of.join(rows.select(sid="oid").unique(), on="sid", how="semi")
            rows = rows.join(members_of.rename({"sid": "oid"}), on="oid", how="semi")
        # The rows of a predicate are its triples, each once: their (s, o) pairs are distinct.
        return rows.select(s="sid", o="oid")

    # Edges by predicate, so that each predicate is read once; the budget counts this too. The
    # pairs of each edge are kept once, in the table of the edges leaving its subject class
    # (column e), and read from there: the matched rows of a large source are most of its
    # triples, and a second copy of them did not fit in memory.
    deadline = clock() + budget_s
    parts: dict[str, list[pl.DataFrame]] = {}
    done: set[int] = set()
    logged = clock()
    sample: dict[str, pl.DataFrame] = {}
    if start_sample is not None:
        sample = {c: _sample(instances(c), start_sample) for c in sorted({k[0] for k in keys})}
        _sampled_edges(
            store, keys, unique, index, sample, matching, max_hops, deadline, clock, parts, done
        )
    for number, k in enumerate(
        [] if start_sample is not None else sorted(keys, key=lambda k: (k[1], k))
    ):
        if clock() >= deadline:
            logger.info("Navigation: the budget ended after %d of %d edges", number, len(keys))
            break
        if clock() - logged >= PROGRESS_SECONDS:
            logged = clock()
            logger.info("Navigation: %d of %d edges read", number, len(keys))
        frame = step(unique[k]).with_columns(e=pl.lit(index[k], pl.Int32))
        parts.setdefault(k[0], []).append(frame)
        done.add(index[k])
    loaded.clear()
    keys = [k for k in keys if index[k] in done]
    leaving: dict[str, pl.DataFrame] = {}
    for c in list(parts):
        leaving[c] = pl.concat(parts.pop(c), rechunk=True)
    fanout = {c: frame.group_by("s").len() for c, frame in leaving.items()}

    def steps(e: int) -> pl.DataFrame:
        """Return the (s, o) pairs of edge *e*."""
        return leaving[keys_of[e][0]].filter(pl.col("e") == e).select("s", "o")

    keys_of = {index[k]: k for k in keys}
    # The source count of a path: the members of its first class (_subject_type_pattern), or
    # the sampled ones.
    if start_sample is not None:
        sources = {c: sample[c].height for c in {k[0] for k in keys}}
    else:
        sources = {c: source_count(c) for c in {k[0] for k in keys}}

    def starts(e: int, pairs: pl.DataFrame) -> pl.DataFrame:
        """Return the pairs of a route's first edge *e* whose subject is a start."""
        if start_sample is None:
            return pairs
        return pairs.join(sample[keys_of[e][0]].rename({"sid": "s"}), on="s", how="semi")

    tested: Counter[int] = Counter()
    matched: Counter[int] = Counter()
    found: dict[tuple[int, ...], int] = {}
    kept: Counter[tuple[int, str]] = Counter()
    omitted: dict[int, Counter[str]] = {}
    stopped = len(keys) < len(index)

    def followed(route: tuple[int, ...]) -> int:
        """Count the starts that follow the whole route, from its end backwards (exact)."""
        reach = steps(route[-1]).select("s").unique()
        for i in reversed(route[:-1]):
            reach = steps(i).join(reach.rename({"s": "o"}), on="o", how="semi").select("s").unique()
        return starts(route[0], reach).height

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
            first = starts(index[k], steps(index[k]))
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
        start_sample=start_sample,
    )


def _sample(instances: pl.DataFrame, size: int) -> pl.DataFrame:
    """Return at most *size* of *instances* (sid), the smallest by a hash of the id: the same
    on every run, and spread over the ids.
    """
    import polars as pl

    if instances.height <= size:
        return instances
    return instances.sort(pl.col("sid").hash(seed=0)).head(size)


def _sampled_edges(
    store: RowStore | StoreView,
    keys: list[Any],
    unique: Mapping[Any, Any],
    index: Mapping[Any, int],
    sample: Mapping[str, pl.DataFrame],
    matching: Callable[..., pl.DataFrame],
    max_hops: int,
    deadline: float,
    clock: Callable[[], float],
    parts: dict[str, list[pl.DataFrame]],
    done: set[int],
) -> None:
    """Read the pairs of each edge for the nodes that the sampled starts reach, hop by hop.

    At each hop, the nodes reached at the previous one (the samples at the first) are the
    subjects read: each predicate is read once a hop, for the nodes of all the classes its
    edges leave, and the objects that are instances of a class are the next hop's nodes of that
    class, those not read before. The pairs of each edge are added to *parts* (by subject
    class, with the edge's number in column e) and the edges read to *done*: every edge when
    the hops end before the *deadline* (an edge no sampled start reaches has no pairs), else
    those read.
    """
    import polars as pl

    frontier = {c: frame for c, frame in sample.items() if frame.height}
    seen = dict(frontier)
    by_predicate: dict[str, list[Any]] = {}
    for k in keys:
        by_predicate.setdefault(k[1], []).append(k)
    found: dict[int, list[pl.DataFrame]] = {}
    leaves = {k[0] for k in keys}
    complete = True
    for hop in range(max_hops):
        if not frontier:
            break
        if clock() >= deadline:
            complete = False
            break
        reached: dict[str, list[pl.DataFrame]] = {}
        for predicate in sorted(by_predicate):
            if clock() >= deadline:
                complete = False
                break
            own = [k for k in by_predicate[predicate] if k[0] in frontier]
            if not own or predicate not in store.predicates:
                continue
            subjects = pl.concat([frontier[c] for c in sorted({k[0] for k in own})]).unique()
            rows = (
                store.rows(predicate)
                .select("sid", "oid", "kind", "d")
                .join(subjects.lazy(), on="sid", how="semi")
                .collect(engine="streaming")
            )
            for k in own:
                pairs = matching(
                    rows.join(frontier[k[0]], on="sid", how="semi"), unique[k], few=True
                )
                found.setdefault(index[k], []).append(pairs)
                if k[2] in leaves:
                    reached.setdefault(k[2], []).append(pairs.select(sid="o"))
        logger.info("Navigation: hop %d of %d read for the sampled starts", hop + 1, max_hops)
        frontier = {}
        for c, frames in reached.items():
            new = pl.concat(frames).unique()
            if c in seen:
                new = new.join(seen[c], on="sid", how="anti")
            if new.height:
                frontier[c] = new
                seen[c] = pl.concat([seen[c], new]) if c in seen else new
    empty = pl.DataFrame(schema={"s": pl.UInt64, "o": pl.UInt64})
    for k in keys:
        if index[k] in found or complete:
            frame = pl.concat(found.get(index[k], [empty])).unique()
            parts.setdefault(k[0], []).append(frame.with_columns(e=pl.lit(index[k], pl.Int32)))
            done.add(index[k])
