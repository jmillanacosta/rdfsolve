"""Tested navigation paths from a row store, by semi-joins over node sets instead of SPARQL.

The same search as navigation.find_tested_paths, with the same edges (the distinct patterns
of the schema) and the same rules; the queries are replaced by operations on sets of nodes:

- an edge (C, p, D) matches the rows of p whose subject is a member of C and whose object is
  a member of D, a literal of the edge's datatype (integer and decimal families as in
  navigation._same_datatype), a blank node, or an IRI without a type ("Resource");
- a path is matched when a start (a member of its first class) follows all of it; a path is
  extended only when matched, and does not repeat an edge;
- the matched sources of a path are the starts that follow it; its source count is the number
  of members of its first class.

Neither question needs the (start, node) pairs of a path, which grow with the starts times
the nodes each of them reaches. Whether a path is matched depends only on the set of nodes its
starts reach: the objects of its last edge from the set the shorter path reaches (a forward
semi-join an edge). Its matched starts are counted backwards: each node that a prefix reaches
gets one bit for each kept path through it (whether the node follows the rest of that path),
and the bits are carried back over the prefix's edges to the starts, OR-ed over the pairs of
each edge; a start's bits are the paths it follows. The memory is that of node sets and of
bits a node, never pairs of a path; the bits of a batch of paths stay within LABEL_BYTES.

The search goes one length at a time, so that a length is complete when its search ends
within the budget. Nodes are numbered by their QLever id (the typed nodes of the scope, in
id order); the rows of a predicate between typed nodes are held once, sorted by subject, and
the other edges are held as the sets of their subjects. The budget is checked before every
step.
"""

from __future__ import annotations

import logging
import time
from collections import Counter, OrderedDict
from collections.abc import Callable, Hashable, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

import numpy as np

from rdfsolve.mining.navigation import _same_datatype, _schema_graph
from rdfsolve.mining.scan import RowStore, StoreView, _bare
from rdfsolve.schema_models.navigation import (
    NON_RELATION_PROPERTIES,
    NavigationPath,
    NavigationSummary,
)

if TYPE_CHECKING:
    import polars as pl

    from rdfsolve.schema_models.core import MinedSchema

__all__ = ["find_tested_paths_in_store"]

logger = logging.getLogger(__name__)

OWL_ANNOTATION_PROPERTY = "http://www.w3.org/2002/07/owl#AnnotationProperty"
# Seconds between two progress lines while the edges are read and the paths are tested.
PROGRESS_SECONDS = 60.0
# Paths of one length from one start class that are kept (find_tested_paths_in_store); the
# others are counted (omitted_by_class) and not extended: without a cap, millions of matched
# paths can fill the schema and run its outputs out of memory.
PATHS_PER_CLASS = 1000
# Bytes of the node sets reached by path prefixes kept for reuse; the sets of the current step
# are held whatever this limit.
SET_CACHE_BYTES = 2 << 30
# Bytes of the bits that count the starts of a batch of paths (one bit a path for each node
# of the largest node set of the batch; find_tested_paths_in_store); a batch has at least one
# word of paths.
LABEL_BYTES = 1 << 30
# A set operation on more elements than this reads a mark per node (one byte a node) instead
# of searching sorted arrays; and lookups are made in chunks of this many elements.
DENSE = 1 << 22
# The values of the pairs whose key is in a set are found by searching each node of the set
# when the set has fewer than one node for this many pairs, else by reading every pair.
SPARSE = 16


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
    of each length are kept for each start class, the first in the order of their edges; the
    others are counted by length and class (omitted_by_class, truncated_lengths) and not
    extended, so that the schema, its outputs and the search stay bounded whatever the time
    budget.

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
    deadline = clock() + budget_s
    # Steps are relations between records: the membership properties of the store, the
    # properties the data declare owl:AnnotationProperty and NON_RELATION_PROPERTIES are left
    # out of the paths (not of the schema's one-hop patterns).
    declared = (
        store.graph_types()
        .filter(pl.col("c") == f"<{OWL_ANNOTATION_PROPERTY}>")
        .select(p=_bare(pl.col("s")))
        .unique()
        .collect(engine="streaming")["p"]
        .to_list()
    )
    left_out = NON_RELATION_PROPERTIES | set(store.base.manifest.get("membership") or [])
    left_out |= set(declared)
    keys = [k for k in keys if k[1] not in left_out]
    index = {k: i for i, k in enumerate(keys)}

    # The typed nodes, numbered in id order (n), and the members of each class by number. The
    # type table is read by id and never held as text.
    type_rows = store.graph_types().select(
        "sid", c=_bare(pl.col("c").cast(pl.String)).cast(pl.Categorical)
    )
    if not getattr(store, "distinct_types", False):
        type_rows = type_rows.unique()
    # Sorted by id, a node's number is the count of distinct ids before it: no join with a
    # table of every node, whose hash table outgrew memory on large stores.
    types = type_rows.collect(engine="streaming").sort("sid")
    sid = types["sid"].to_numpy()
    first = np.r_[True, sid[1:] != sid[:-1]] if len(sid) else np.zeros(0, bool)
    nodes = Nodes(sid[first])
    numbers = (np.cumsum(first) - 1).astype(nodes.dtype)
    del sid, first
    by_class = {
        str(key[0]): frame["n"].to_numpy()
        for key, frame in types.select("c", n=numbers)
        .partition_by("c", as_dict=True, include_key=False, maintain_order=True)
        .items()
    }
    del types, numbers
    held: dict[str, np.ndarray] = {}

    def instances(cls: str) -> np.ndarray:
        """Return the numbers of the instances of *cls* and of the terms it stands for."""
        if cls not in held:
            found = [by_class[t] for t in (cls, *members.get(cls, ())) if t in by_class]
            held[cls] = (
                nodes.empty()
                if not found
                else found[0]
                if len(found) == 1
                else nodes.distinct(np.concatenate(found))
            )
        return held[cls]

    def source_count(cls: str) -> int:
        """Return the members of *cls* and of the terms it stands for (_subject_type_pattern):
        its instances, or, with type context graphs, those with an edge in the data graphs.
        """
        if not getattr(store, "context", ()):
            return len(instances(cls))
        terms = [cls, *members.get(cls, ())]
        return int(
            store.members()
            .filter(_bare(pl.col("c")).is_in(terms))
            .select(pl.col("s").n_unique())
            .collect(engine="streaming")
            .item()
        )

    starts: dict[str, np.ndarray] = {}
    if start_sample is not None:
        for c in sorted({k[0] for k in keys}):
            chosen = _sample(pl.DataFrame({"sid": nodes.sid[instances(c)]}), start_sample)
            starts[c] = nodes.number(chosen["sid"].to_numpy())
    else:
        starts = {c: instances(c) for c in {k[0] for k in keys}}
    # The classes that edges leave: an edge into one of them can be extended.
    leaves = {k[0] for k in keys}

    # The rows of each predicate are read once: the subjects of each edge (by its predicate
    # and object), and, for the edges into classes that edges leave, the (subject, object)
    # pairs between typed nodes, sorted both ways.
    subjects: dict[tuple[str, str, str], np.ndarray] = {}
    pairs: dict[str, Pairs] = {}
    by_predicate: dict[str, list[Any]] = {}
    for k in keys:
        by_predicate.setdefault(k[1], []).append(k)
    sources_of: dict[str, pl.LazyFrame]
    complete = True
    if start_sample is not None:
        sources_of, complete = _sampled_rows(
            store, keys, nodes, instances, starts, max_hops, deadline, clock
        )
    else:
        sources_of = {
            p: store.rows(p).select("sid", "oid", "kind", "d")
            for p in by_predicate
            if p in store.predicates
        }
    done: set[int] = set()
    logged = clock()
    read = 0
    for predicate in sorted(by_predicate):
        if clock() >= deadline or not complete:
            logger.info("Navigation: the budget ended after %d of %d edges", read, len(keys))
            break
        if clock() - logged >= PROGRESS_SECONDS:
            logged = clock()
            logger.info("Navigation: %d of %d edges read", read, len(keys))
        edges = by_predicate[predicate]
        if predicate in sources_of:
            _read_predicate(
                sources_of[predicate],
                edges,
                unique,
                nodes,
                instances,
                leaves,
                subjects,
                pairs,
                predicate,
                {**by_class, **held},
            )
        done.update(index[k] for k in edges)
        read += len(edges)
    sources_of.clear()
    stopped = len(done) < len(index)

    # The source count of a path: the members of its first class (_subject_type_pattern), or
    # the sampled ones.
    if start_sample is not None:
        sources = {c: len(starts[c]) for c in leaves}
    else:
        sources = {c: source_count(c) for c in leaves}

    def subject_set(e: int) -> np.ndarray:
        """Return the subjects of the rows of edge *e*, of any class."""
        k = keys[e]
        return subjects.get(k[1:], nodes.empty())

    def forward(found: np.ndarray, e: int) -> np.ndarray:
        """Return the objects of edge *e* (into a class edges leave) from the nodes *found*."""
        k = keys[e]
        if k[1] not in pairs:
            return nodes.empty()
        held_pairs = pairs[k[1]]
        reached = nodes.follow(held_pairs.subjects, held_pairs.objects, found)
        return nodes.meet(reached, instances(k[2]))

    prefixes = SetCache(SET_CACHE_BYTES)
    sizes: dict[tuple[int, ...], int] = {}

    def reached(route: tuple[int, ...]) -> np.ndarray:
        """Return the nodes that the starts reach by *route* (the starts, for no route)."""
        found = prefixes.get(route)
        if found is None:
            before = starts[keys[route[0]][0]] if len(route) == 1 else reached(route[:-1])
            found = forward(before, route[-1])
            prefixes.put(route, found)
            sizes[route] = len(found)
        return np.asarray(found)

    def labels(
        prefix: tuple[int, ...], routes: list[tuple[int, ...]]
    ) -> tuple[np.ndarray, np.ndarray, list[tuple[tuple[int, ...], int, int]]]:
        """Return the nodes that *prefix* (at least one edge) reaches, and one bit a route of *routes* for each: whether the node follows the rest of the route;
        with the word and bit of each route. The routes have one length, extend *prefix*, and
        are in order; those of one prefix one edge shorter fill whole words.
        """
        found = reached(prefix)
        depth = len(prefix)
        marks = np.zeros((len(found), _width(routes)), np.uint64)
        places: list[tuple[tuple[int, ...], int, int]] = []
        if depth == len(routes[0]) - 1:
            look = nodes.lookup(found)
            for j, route in enumerate(routes):
                marks[look.positions(subject_set(route[-1])), j // 64] |= np.uint64(1 << (j % 64))
                places.append((route, j // 64, j % 64))
            return found, marks, places
        offset = 0
        for e, group in _groups(routes, depth):
            child, below, where = labels((*prefix, e), group)
            width = below.shape[1]
            if len(child) and keys[e][1] in pairs:
                held_pairs = pairs[keys[e][1]]
                nodes.carry(
                    held_pairs.subjects,
                    held_pairs.objects,
                    found,
                    child,
                    below,
                    marks[:, offset : offset + width],
                )
            places.extend((route, word + offset, bit) for route, word, bit in where)
            offset += width
            del child, below
        return found, marks, places

    leaving: dict[str, list[int]] = {}
    for k in keys:
        if index[k] in done:
            leaving.setdefault(k[0], []).append(index[k])
    tested: Counter[int] = Counter()
    matched: Counter[int] = Counter()
    found: dict[tuple[int, ...], int] = {}
    kept: Counter[tuple[int, str]] = Counter()
    omitted: dict[int, Counter[str]] = {}
    complete_lengths: list[int] = []
    # The paths of one edge that a start follows, into a class that edges leave.
    level: list[tuple[int, ...]] = []
    if not stopped:
        for k in keys:
            if k[2] in leaves and len(starts[k[0]]) and len(reached((index[k],))):
                level.append((index[k],))
    for hops in range(2, max_hops + 1):
        if stopped:
            break
        new: list[tuple[int, ...]] = []
        for path in level:
            last = keys[path[-1]][2]
            if last not in leaving:
                continue
            candidates = [e for e in leaving[last] if e not in path]
            if not candidates:
                continue
            if clock() >= deadline:
                stopped = True
                break
            if clock() - logged >= PROGRESS_SECONDS:
                logged = clock()
                logger.info("Navigation: %d-hop paths: %d matched so far", hops, matched[hops])
            ends = nodes.lookup(reached(path))
            if clock() - logged >= PROGRESS_SECONDS:
                logged = clock()
                logger.info(
                    "Navigation: %d-hop paths: %d nodes reached by a prefix (%d matched so far)",
                    hops,
                    len(ends.found),
                    matched[hops],
                )
            tested[hops] += len(candidates)
            start = keys[path[0]][0]
            for e in candidates:
                if not ends.meets(subject_set(e)):
                    continue
                matched[hops] += 1
                if kept[(hops, start)] >= cap:
                    omitted.setdefault(hops, Counter())[start] += 1
                    continue
                kept[(hops, start)] += 1
                new.append((*path, e))
        # The starts of each kept path, counted backwards from the nodes of each prefix: one
        # bit a path for each node (whether it follows the rest of the path), carried over the
        # edges of the prefixes to the starts. The paths of one start class are counted
        # together, in batches whose bits fit LABEL_BYTES for the largest node set they hold.
        counted = 0
        for start, batch in _batches(new, keys, sizes):
            if clock() >= deadline:
                stopped = True
                break
            if clock() - logged >= PROGRESS_SECONDS:
                logged = clock()
                logger.info(
                    "Navigation: %d-hop paths: the starts of %d of %d counted",
                    hops,
                    counted,
                    len(new),
                )
            counted += len(batch)
            # The starts are not given bits: each start's bits are OR-ed from its pairs over the
            # first edge and counted as they are made, so the starts of a large class hold none.
            for e, group in _groups(batch, 0):
                child, below, places = labels((e,), group)
                counts = np.zeros(below.shape[1] * 64, np.int64)
                if len(child) and keys[e][1] in pairs:
                    held_pairs = pairs[keys[e][1]]
                    counts = nodes.count_carried(
                        held_pairs.subjects, held_pairs.objects, starts[start], child, below
                    )
                for route, word, bit in places:
                    found[route] = int(counts[word * 64 + bit])
                del child, below
        logger.info("Navigation: %d-hop paths: %d matched, %d kept", hops, matched[hops], len(new))
        if stopped:
            break
        complete_lengths.append(hops)
        level = new
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
        complete_lengths=complete_lengths,
        budget_s=budget_s,
        stop_reason=stop,
        query_count=0,
        failed_queries=0,
        start_sample=start_sample,
    )


class Nodes:
    """Sets of typed nodes, as sorted arrays of their numbers, and the operations on them."""

    def __init__(self, sid: np.ndarray) -> None:
        """Hold the sorted ids *sid* of the nodes, numbered by their position."""
        self.sid = sid
        self.size = len(sid)
        self.dtype = np.uint32 if self.size < 2**32 else np.uint64

    def empty(self) -> np.ndarray:
        """Return the empty set."""
        return np.zeros(0, self.dtype)

    def number(self, sid: np.ndarray) -> np.ndarray:
        """Return the sorted numbers of the typed nodes among the ids *sid*."""
        sid = np.unique(sid)
        at = np.searchsorted(self.sid, sid).clip(max=max(self.size - 1, 0))
        if not self.size:
            return self.empty()
        return np.asarray(at[self.sid[at] == sid].astype(self.dtype))

    def find(self, sid: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Return the numbers of the ids *sid* and whether each is a typed node (the number of
        an id that is not is meaningless).
        """
        if not self.size:
            return np.zeros(len(sid), self.dtype), np.zeros(len(sid), bool)
        at = np.searchsorted(self.sid, sid).clip(max=self.size - 1)
        return at.astype(self.dtype), self.sid[at] == sid

    def distinct(self, values: np.ndarray) -> np.ndarray:
        """Return the sorted distinct *values*, by a mark per node when there are many."""
        if len(values) <= DENSE:
            return _unique(values).astype(self.dtype)
        marks = np.zeros(self.size, bool)
        for at in range(0, len(values), DENSE):
            marks[values[at : at + DENSE]] = True
        return np.flatnonzero(marks).astype(self.dtype)

    def meet(self, a: np.ndarray, b: np.ndarray) -> np.ndarray:
        """Return the intersection of the sets *a* and *b*."""
        if len(a) > len(b):
            a, b = b, a
        if not len(a):
            return self.empty()
        if len(a) > DENSE:
            marks = np.zeros(self.size, bool)
            marks[b] = True
            return np.asarray(a[marks[a]])
        at = np.searchsorted(b, a).clip(max=len(b) - 1)
        return np.asarray(a[b[at] == a])

    def lookup(self, found: np.ndarray) -> Lookup:
        """Return *found*, made ready to be met with many other sets."""
        return Lookup(self, found)

    def follow(self, keys: np.ndarray, values: np.ndarray, found: np.ndarray) -> np.ndarray:
        """Return the distinct values of the pairs (keys sorted, *values* aligned) whose key
        is in *found*: by searching each node of *found* when it is small next to the keys,
        else by a mark per node, read in chunks.
        """
        if not len(found) or not len(keys):
            return self.empty()
        if len(found) * SPARSE < len(keys):
            low = np.searchsorted(keys, found, "left")
            counts = np.searchsorted(keys, found, "right") - low
            total = int(counts.sum())
            if total <= DENSE:
                return _unique(values[_ranges(low, counts, total)]).astype(self.dtype)
            marks = np.zeros(self.size, bool)
            ends = np.cumsum(counts)
            first = 0
            while first < len(found):
                last = int(np.searchsorted(ends, (ends[first] - counts[first]) + DENSE, "right"))
                last = max(last, first + 1)
                part = counts[first:last]
                marks[values[_ranges(low[first:last], part, int(part.sum()))]] = True
                first = last
            return np.flatnonzero(marks).astype(self.dtype)
        wanted = np.zeros(self.size, bool)
        wanted[found] = True
        marks = np.zeros(self.size, bool)
        for at in range(0, len(keys), DENSE):
            chunk = slice(at, at + DENSE)
            marks[values[chunk][wanted[keys[chunk]]]] = True
        return np.flatnonzero(marks).astype(self.dtype)

    def carry(
        self,
        keys: np.ndarray,
        values: np.ndarray,
        found: np.ndarray,
        child: np.ndarray,
        below: np.ndarray,
        out: np.ndarray,
    ) -> None:
        """Set in *out* (a row a node of *found*) the bits *below* (a row a node of *child*)
        of the nodes each node has a pair to (keys sorted, *values* aligned), in chunks.
        """
        low = np.searchsorted(keys, found, "left")
        counts = np.searchsorted(keys, found, "right") - low
        ends = np.cumsum(counts)
        first = 0
        while first < len(found):
            last = int(np.searchsorted(ends, (ends[first] - counts[first]) + DENSE, "right"))
            last = max(last, first + 1)
            part = counts[first:last]
            total = int(part.sum())
            if total:
                ends_at = values[_ranges(low[first:last], part, total)]
                source = np.repeat(np.arange(first, last), part)
                at = np.searchsorted(child, ends_at).clip(max=len(child) - 1)
                hit = child[at] == ends_at
                source, at = source[hit], at[hit]
                if len(source):
                    runs = np.flatnonzero(np.r_[True, source[1:] != source[:-1]])
                    out[source[runs]] |= np.bitwise_or.reduceat(below[at], runs, axis=0)
            first = last

    def count(self, marks: np.ndarray) -> np.ndarray:
        """Return the number of rows of *marks* with each bit set (bit b of word w at
        w * 64 + b).
        """
        width = marks.shape[1]
        totals = np.zeros(width * 64, np.int64)
        rows = max(1, DENSE // (width * 8))
        for at in range(0, len(marks), rows):
            block = np.ascontiguousarray(marks[at : at + rows]).astype("<u8", copy=False)
            bits = np.unpackbits(block.view(np.uint8), axis=1, bitorder="little")
            totals += bits.sum(axis=0, dtype=np.int64)
        return totals

    def count_carried(
        self,
        keys: np.ndarray,
        values: np.ndarray,
        found: np.ndarray,
        child: np.ndarray,
        below: np.ndarray,
    ) -> np.ndarray:
        """Return, for each bit of *below* (a row a node of *child*), the nodes of *found* that
        have a pair (keys sorted, *values* aligned) to a node with that bit: carry() into rows
        that are counted as they are made, in chunks, and never held.
        """
        totals = np.zeros(below.shape[1] * 64, np.int64)
        low = np.searchsorted(keys, found, "left")
        counts = np.searchsorted(keys, found, "right") - low
        ends = np.cumsum(counts)
        pending: np.ndarray | None = None
        pending_source = -1
        first = 0
        while first < len(found):
            last = int(np.searchsorted(ends, (ends[first] - counts[first]) + DENSE, "right"))
            last = max(last, first + 1)
            part = counts[first:last]
            total = int(part.sum())
            if total:
                ends_at = values[_ranges(low[first:last], part, total)]
                source = np.repeat(np.arange(first, last), part)
                at = np.searchsorted(child, ends_at).clip(max=len(child) - 1)
                hit = child[at] == ends_at
                source, at = source[hit], at[hit]
                if len(source):
                    runs = np.flatnonzero(np.r_[True, source[1:] != source[:-1]])
                    rows = np.bitwise_or.reduceat(below[at], runs, axis=0)
                    sources = source[runs]
                    # A node's pairs can span two chunks: its row is counted once, whole.
                    if pending is not None:
                        if sources[0] == pending_source:
                            rows[0] |= pending
                        else:
                            totals += self.count(pending[None, :])
                    pending, pending_source = rows[-1].copy(), int(sources[-1])
                    totals += self.count(rows[:-1])
            first = last
        if pending is not None:
            totals += self.count(pending[None, :])
        return totals


class Lookup:
    """A set of nodes tested against many sets: by search when small, else by a mark per
    node, made once, with chunks so that a test ends at its first common node.
    """

    def __init__(self, nodes: Nodes, found: np.ndarray) -> None:
        """Hold *found*."""
        self.nodes = nodes
        self.found = found
        self.marks: np.ndarray | None = None
        self.where: np.ndarray | None = None

    def meets(self, other: np.ndarray) -> bool:
        """Tell whether *other* has a node of this set."""
        found = self.found
        if not len(found) or not len(other) or found[-1] < other[0] or other[-1] < found[0]:
            return False
        small, large = (found, other) if len(found) <= len(other) else (other, found)
        if len(small) <= DENSE:
            at = np.searchsorted(large, small).clip(max=len(large) - 1)
            return bool((large[at] == small).any())
        if self.marks is None:
            self.marks = np.zeros(self.nodes.size, bool)
            self.marks[found] = True
        return any(self.marks[other[at : at + DENSE]].any() for at in range(0, len(other), DENSE))

    def positions(self, other: np.ndarray) -> np.ndarray:
        """Return the positions in this set of its nodes that are in *other*."""
        found = self.found
        if not len(found) or not len(other):
            return np.zeros(0, np.int64)
        if len(other) <= len(found):
            at = np.searchsorted(found, other).clip(max=len(found) - 1)
            return np.asarray(at[found[at] == other])
        if len(found) <= DENSE:
            at = np.searchsorted(other, found).clip(max=len(other) - 1)
            return np.flatnonzero(other[at] == found)
        if self.where is None:
            self.where = np.full(self.nodes.size, -1, np.int64)
            self.where[found] = np.arange(len(found))
        at = self.where[other]
        return np.asarray(at[at >= 0])


class Pairs:
    """The rows of a predicate between typed nodes, sorted by subject."""

    def __init__(self, subjects: np.ndarray, objects: np.ndarray) -> None:
        """Hold the pairs (*subjects* sorted, *objects* aligned)."""
        self.subjects = subjects
        self.objects = objects


class SetCache:
    """Node sets by key, the least recently used dropped past a number of bytes."""

    def __init__(self, limit: int) -> None:
        """Keep at most *limit* bytes (and always the last set put)."""
        self.limit = limit
        self.size = 0
        self.sets: OrderedDict[Hashable, Any] = OrderedDict()

    def get(self, key: Hashable) -> Any:
        """Return the set of *key*, or None."""
        found = self.sets.get(key)
        if found is not None:
            self.sets.move_to_end(key)
        return found

    def put(self, key: Hashable, found: Any) -> None:
        """Keep *found* as the set of *key*."""
        self.sets[key] = found
        self.size += found.nbytes
        while self.size > self.limit and len(self.sets) > 1:
            _, dropped = self.sets.popitem(last=False)
            self.size -= dropped.nbytes


def _unique(values: np.ndarray) -> np.ndarray:
    """Return the sorted distinct *values* (by sorting: faster here than np.unique's hashing)."""
    values = np.sort(values)
    if len(values) < 2:
        return values
    return values[np.r_[True, values[1:] != values[:-1]]]


def _ranges(low: np.ndarray, counts: np.ndarray, total: int) -> np.ndarray:
    """Return the positions low[i] .. low[i] + counts[i] - 1 for each i, in order."""
    offsets = np.cumsum(counts) - counts
    return np.repeat(low.astype(np.int64) - offsets, counts) + np.arange(total)


def _width(routes: Sequence[tuple[int, ...]]) -> int:
    """Return the words of bits of *routes* (in order): those of one prefix one edge shorter
    fill whole words.
    """
    words = 0
    parent: tuple[int, ...] | None = None
    held = 0
    for route in routes:
        if route[:-1] != parent or held == 64:
            words += 1
            parent, held = route[:-1], 0
        held += 1
    return words


def _groups(
    routes: Sequence[tuple[int, ...]], depth: int
) -> list[tuple[int, list[tuple[int, ...]]]]:
    """Return *routes* (in order) grouped by their edge at *depth*, in order."""
    groups: list[tuple[int, list[tuple[int, ...]]]] = []
    for route in routes:
        if not groups or groups[-1][0] != route[depth]:
            groups.append((route[depth], []))
        groups[-1][1].append(route)
    return groups


def _batches(
    routes: Sequence[tuple[int, ...]],
    keys: Sequence[Any],
    sizes: Mapping[tuple[int, ...], int],
) -> list[tuple[str, list[tuple[int, ...]]]]:
    """Split *routes* (in order) into batches of one start class whose bits fit LABEL_BYTES
    for the largest node set they hold (the nodes each prefix reaches).
    """
    batches: list[tuple[str, list[tuple[int, ...]]]] = []
    words = widest = held = 0
    parent: tuple[int, ...] | None = None
    for route in routes:
        start = keys[route[0]][0]
        size = max(0, *(sizes.get(route[:i], 0) for i in range(1, len(route))))
        word = route[:-1] != parent or held == 64
        if batches and (
            batches[-1][0] != start or (word and (words + 1) * max(widest, size) * 8 > LABEL_BYTES)
        ):
            batches.append((start, []))
            words = widest = 0
            word = True
        elif not batches:
            batches.append((start, []))
        if word:
            words += 1
            parent, held = route[:-1], 0
        held += 1
        widest = max(widest, size)
        batches[-1][1].append(route)
    return batches


def _shared(found: np.ndarray, *sets: Mapping[Any, np.ndarray]) -> np.ndarray:
    """Return a set of *sets* equal to *found* if there is one, else *found*: the subjects of
    many edges are the same nodes (often every member of a class), held once.
    """
    for held in sets:
        for other in held.values():
            if (
                len(other) == len(found)
                and other.dtype == found.dtype
                and (not len(found) or (other[0] == found[0] and other[-1] == found[-1]))
                and np.array_equal(other, found)
            ):
                return other
    return found


def _read_predicate(
    rows: pl.LazyFrame,
    edges: list[Any],
    unique: Mapping[Any, Any],
    nodes: Nodes,
    instances: Callable[[str], np.ndarray],
    leaves: set[str],
    subjects: dict[tuple[str, str, str], np.ndarray],
    pairs: dict[str, Pairs],
    predicate: str,
    classes: Mapping[str, np.ndarray],
) -> None:
    """Read the rows of one predicate for its *edges*: the subjects of each (predicate, object)
    of an edge into *subjects*, and the pairs between typed nodes into *pairs* when an edge goes
    into a class that edges leave.

    The rows are read in batches; ids are numbered by searching the sorted ids of the typed
    nodes, and the subjects of an edge into literals, blank nodes or IRIs without a type are
    marked, a mark per node: the memory is that of the nodes and of the pairs kept.
    """
    objects = sorted({k[1:] for k in edges})
    into = [o for o in objects if o[1] not in {"Literal", "BlankNode", "Resource"}]
    marks = {o: np.zeros(nodes.size, bool) for o in objects if o not in into}
    datatype = {o: unique[next(k for k in edges if k[1:] == o)].datatype for o in marks}
    same: dict[tuple[Any, str], bool] = {}
    kept_subjects: list[np.ndarray] = []
    kept_objects: list[np.ndarray] = []
    for batch in rows.select("sid", "oid", "kind", "d").collect_batches(
        chunk_size=DENSE, engine="streaming"
    ):
        subject, typed = nodes.find(batch["sid"].to_numpy())
        if not typed.any():
            continue
        value, typed_value = nodes.find(batch["oid"].to_numpy())
        kind = batch["kind"]
        for o, mark in marks.items():
            if o[1] == "Literal":
                found = [d for d in batch["d"].drop_nulls().unique().to_list() if d]
                for d in found:
                    if (o, d) not in same:
                        same[(o, d)] = _same_datatype(datatype[o], d)
                chosen = [d for d in found if same[(o, d)]]
                literal = (kind == "literal") & batch["d"].is_in(chosen)
                hit: np.ndarray = literal.fill_null(False).to_numpy()
            elif o[1] == "BlankNode":
                hit = (kind == "bnode").to_numpy()
            else:
                hit = (kind == "iri").to_numpy() & ~typed_value
            mark[subject[hit & typed]] = True
        if into:
            both = typed & typed_value
            kept_subjects.append(subject[both])
            kept_objects.append(value[both])
    for o, mark in marks.items():
        subjects[o] = _shared(np.flatnonzero(mark).astype(nodes.dtype), subjects, classes)
    del marks
    if not into:
        return
    subject = np.concatenate(kept_subjects) if kept_subjects else nodes.empty()
    value = np.concatenate(kept_objects) if kept_objects else nodes.empty()
    del kept_subjects, kept_objects
    inside = np.zeros(nodes.size, bool)
    for o in into:
        inside[:] = False
        inside[instances(o[1])] = True
        subjects[o] = _shared(nodes.distinct(subject[inside[value]]), subjects, classes)
    if not any(o[1] in leaves for o in into):
        return
    # The pairs into the classes that edges leave, from the classes of the predicate's edges.
    inside[:] = False
    for o in into:
        if o[1] in leaves:
            inside[instances(o[1])] = True
    keep = inside[value]
    inside[:] = False
    for c in {k[0] for k in edges if k[2] in leaves}:
        inside[instances(c)] = True
    keep &= inside[subject]
    del inside
    subject, value = subject[keep], value[keep]
    del keep
    # Rows read in subject order need no sort.
    if len(subject) > 1 and not bool((subject[1:] >= subject[:-1]).all()):
        order = np.argsort(subject, kind="stable")
        subject, value = subject[order], value[order]
        del order
    pairs[predicate] = Pairs(subject, value)


def _sample(instances: pl.DataFrame, size: int) -> pl.DataFrame:
    """Return at most *size* of *instances* (sid), the smallest by a hash of the id: the same
    on every run, and spread over the ids.
    """
    import polars as pl

    if instances.height <= size:
        return instances
    return instances.sort(pl.col("sid").hash(seed=0)).head(size)


def _sampled_rows(
    store: RowStore | StoreView,
    keys: list[Any],
    nodes: Nodes,
    instances: Callable[[str], np.ndarray],
    starts: Mapping[str, np.ndarray],
    max_hops: int,
    deadline: float,
    clock: Callable[[], float],
) -> tuple[dict[str, pl.LazyFrame], bool]:
    """Read the rows of each predicate for the nodes that the sampled starts reach, hop by hop.

    At each hop, the nodes reached at the previous one (the samples at the first) are the
    subjects read: each predicate is read once a hop, for the nodes of all the classes its
    edges leave, and the objects that are instances of a class are the next hop's nodes of that
    class, those not read before. Return the rows read of each predicate, and whether the hops
    ended before the *deadline*.
    """
    import polars as pl

    def frame(numbers: np.ndarray) -> pl.DataFrame:
        """Return the ids of the nodes *numbers* (sid)."""
        return pl.DataFrame({"sid": nodes.sid[numbers]})

    frontier = {c: found for c, found in starts.items() if len(found)}
    seen = dict(frontier)
    by_predicate: dict[str, list[Any]] = {}
    for k in keys:
        by_predicate.setdefault(k[1], []).append(k)
    read: dict[str, list[pl.DataFrame]] = {}
    leaves = {k[0] for k in keys}
    for hop in range(max_hops):
        if not frontier:
            break
        if clock() >= deadline:
            return {}, False
        reached: dict[str, list[np.ndarray]] = {}
        for predicate in sorted(by_predicate):
            if clock() >= deadline:
                return {}, False
            own = [k for k in by_predicate[predicate] if k[0] in frontier]
            if not own or predicate not in store.predicates:
                continue
            numbers = np.unique(np.concatenate([frontier[c] for c in sorted({k[0] for k in own})]))
            rows = (
                store.rows(predicate)
                .select("sid", "oid", "kind", "d")
                .join(frame(numbers).lazy(), on="sid", how="semi")
                .collect(engine="streaming")
            )
            read.setdefault(predicate, []).append(rows)
            subject, typed = nodes.find(rows["sid"].to_numpy())
            value, typed_value = nodes.find(rows["oid"].to_numpy())
            subject, value = subject[typed & typed_value], value[typed & typed_value]
            for k in own:
                if k[2] not in leaves:
                    continue
                at = np.searchsorted(frontier[k[0]], subject).clip(max=len(frontier[k[0]]) - 1)
                hit = (frontier[k[0]][at] == subject) & np.isin(value, instances(k[2]))
                reached.setdefault(k[2], []).append(value[hit])
        logger.info("Navigation: hop %d of %d read for the sampled starts", hop + 1, max_hops)
        frontier = {}
        for c, found in reached.items():
            new = np.unique(np.concatenate(found)).astype(nodes.dtype)
            if c in seen:
                new = new[~np.isin(new, seen[c])]
            if len(new):
                frontier[c] = new
                seen[c] = np.union1d(seen[c], new).astype(nodes.dtype) if c in seen else new
    return {
        p: pl.concat(frames).unique(subset=["sid", "oid"]).lazy() for p, frames in read.items()
    }, True
