"""Ontology terms used as types, grouped after counting from the rows of a row store.

A scan run (rdfsolve.mining.scan) has every row on disk, so the grouping of ontology terms is a
rewrite of the type table (term -> representative), applied to subjects and objects alike, and
the patterns are counted again with count_patterns: every count is exact. Summing the rows of the
member terms (ontology_as_data.subsume_patterns) is an upper bound: a record typed with two
members of one representative is counted twice, and the distinct counts cannot be summed at
all.

- superclasses(): the hierarchy as ontology.hierarchy.fetch_superclasses reads it, from the
  rdfs:subClassOf rows (and hierarchy files, as two_phase_strategy._group_terms reads them);
- minimal_types(): ABSTAT's minimal types;
- group_terms(): rdfsolve's own representatives (choose_representatives, parentless_candidates,
  group_by_shape, unchanged, with the budget and the group-before-mining threshold), one rewrite,
  one recount; the count-aware cut as an optional rule;
- foldable_expressions() / fold_class_expressions(): the anonymous classes ``p some F`` used as
  types become edge rows (C, p, F), their fillers grouped per (C, p) slot.

Typical order in a scan run: name_class_expressions(store); expressions =
foldable_expressions(store); types = without_classes(type_table(store), expressions);
grouping = group_terms(store, types=types, ...); folded = fold_class_expressions(store,
expressions, types=grouping.types, ...); patterns = grouping.patterns + folded.patterns.
"""

from __future__ import annotations

import heapq
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from rdfsolve.mining.ontology_as_data import (
    NAMESPACE_GROUP_MIN_TERMS,
    Subsumption,
    choose_representatives,
    group_by_shape,
    parentless_candidates,
)
from rdfsolve.mining.query_builders import MEMBERSHIP
from rdfsolve.ontology.terms import namespace
from rdfsolve.ontology.vocabulary import NOT_DATA_TYPES
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.pattern import PatternType, SchemaPattern

if TYPE_CHECKING:
    import polars as pl

    from rdfsolve.mining.scan import RowStore, StoreView

SUBCLASS_OF = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
SOME = re.compile(r"^<([^<>\s]+)> some <([^<>\s]+)>$")
SLOT_BUDGET = 10

__all__ = [
    "ClassExpressionFolding",
    "GroupedBeforeCounting",
    "RetypedStore",
    "TermGrouping",
    "count_aware_cut",
    "exact_term_rows",
    "fold_class_expressions",
    "foldable_expressions",
    "group_before_counting",
    "group_terms",
    "hierarchy_edges",
    "minimal_types",
    "record_types",
    "retype",
    "superclasses",
    "type_table",
    "without_classes",
]


# The type table


def _bare(term: str) -> str:
    return term[1:-1] if term.startswith("<") and term.endswith(">") else term


class RetypedStore:
    """A row store (or a view of one) whose type table is replaced; count_patterns reads it.

    Everything else (rows, graphs, predicates, the types of blank nodes in the data graphs) is
    the store's own. With *rows*, the rows the table is made distinct from (retype with
    distinct=False), graph_types() returns them and the tables by node are made distinct from
    them, as a store's own type rows: a count of classes then reads the rows, and the table by
    id leaves the node text out before it is made distinct.
    """

    # The replaced type table is made distinct where it is read (type_table), as given.
    distinct_types = False

    def __init__(
        self,
        store: RowStore | StoreView | RetypedStore,
        types: pl.LazyFrame,
        rows: pl.LazyFrame | None = None,
    ) -> None:
        """Keep *store* and the type table (s, c) that replaces its own (made from *rows*)."""
        self._store = store
        self._types = types
        self._rows = rows

    def __getattr__(self, name: str) -> Any:
        """Read everything but the types from the store."""
        return getattr(self._store, name)

    def graph_types(self) -> pl.LazyFrame:
        """Return the replaced type rows (s, sid, c), without graphs: the table, or its rows."""
        return self._types if self._rows is None else self._rows

    def types(self) -> pl.LazyFrame:
        """Return the replaced type table (s, c)."""
        return self.graph_types().select("s", "c").unique()

    def type_ids(self) -> pl.LazyFrame:
        """Return the replaced type table by id (sid, c), which count_patterns joins on."""
        return self.graph_types().select("sid", "c").unique()

    def members(self) -> pl.LazyFrame:
        """Return the members of each class in the replaced type table."""
        members = self._store.members().select("s").unique()
        return self.types().join(members, on="s", how="semi")


def _keys(types: pl.LazyFrame) -> list[str]:
    """Return the record columns of a type table: s, and QLever's id sid when it has one."""
    return ["s", "sid"] if "sid" in types.collect_schema().names() else ["s"]


def type_table(store: RowStore | StoreView) -> pl.LazyFrame:
    """Return the type table of the store's scope with the ids: (s, sid, c), one row each.

    The rows are made distinct unless the store's membership rows already are
    (RowStore.distinct_types): the classes of a type table (_type_classes) are then read
    from the rows, without holding every (node, class) row of a large index at once.
    """
    rows = store.graph_types()
    table = rows.select(*_keys(rows), "c")
    return table if getattr(store, "distinct_types", False) else table.unique()


def _table(types: pl.LazyFrame) -> pl.LazyFrame:
    return types.select(*_keys(types), "c").unique()


def retype(
    types: pl.LazyFrame, representative: Mapping[str, str], *, distinct: bool = True
) -> pl.LazyFrame:
    """Rewrite the classes of a type table (terms as <iri>) by *representative*; one row per
    (s, c), so a record typed with two members of one representative is one member of it.

    With *distinct* False, the rewritten rows are not made distinct (a record typed with two
    members of one representative has two rows): the rows of a RetypedStore.
    """
    import polars as pl

    keys = _keys(types)
    if not representative:
        rows = types.select(*keys, "c")
        return rows.unique() if distinct else rows
    mapping = pl.LazyFrame(
        {
            "c": [f"<{t}>" for t in representative],
            "r": [f"<{r}>" for r in representative.values()],
        },
        schema={"c": pl.String, "r": pl.String},
    )
    rows = (
        types.select(*keys, "c")
        .join(mapping, on="c", how="left")
        .select(*keys, c=pl.coalesce("r", "c"))
    )
    return rows.unique() if distinct else rows


def without_classes(types: pl.LazyFrame, classes: Iterable[str]) -> pl.LazyFrame:
    """Leave the rows of *classes* (IRIs) out of a type table."""
    import polars as pl

    terms = [f"<{c}>" for c in classes]
    return types.filter(~pl.col("c").is_in(terms)) if terms else types


def _type_classes(types: pl.LazyFrame) -> list[str]:
    """Return the IRI classes of a type table, as class discovery returns them."""
    import polars as pl

    found = types.select("c").unique().filter(pl.col("c").str.starts_with("<")).collect()
    return sorted(_bare(c) for c in found["c"])


# Hierarchy


def _hierarchy_store(
    store: RowStore | StoreView, ontology_graph_uris: Sequence[str] | None
) -> RowStore | StoreView:
    """Return the rows fetch_superclasses reads: the selected graphs, else the whole index."""
    base = getattr(store, "base", store)
    return base.view(list(ontology_graph_uris)) if ontology_graph_uris else base


def superclasses(
    store: RowStore | StoreView,
    terms: Iterable[str],
    *,
    ontology_graph_uris: Sequence[str] | None = None,
    hierarchy_files: Sequence[str | Path] = (),
) -> dict[str, set[str]]:
    """Return the named rdfs:subClassOf parents of *terms* and of all their ancestors.

    As ontology.hierarchy.fetch_superclasses: IRI parents other than the term itself, parents
    in NOT_DATA_TYPES left out, followed to the roots; read from the selected ontology graphs,
    else from the whole index (the endpoint's default dataset). With *hierarchy_files*, a term
    without a parent in the data takes the files' parents (ontology.hierarchy.fill_parents, as
    two_phase_strategy._group_terms does).
    """
    import polars as pl

    from rdfsolve.ontology.hierarchy import fill_parents, read_hierarchy

    source = _hierarchy_store(store, ontology_graph_uris)
    rows = None
    if SUBCLASS_OF in source.predicates:
        rows = (
            source.rows(SUBCLASS_OF)
            .filter(pl.col("s").str.starts_with("<") & (pl.col("kind") == "iri"))
            .select("s", "o")
        )
    # Only the rows of the terms and of their ancestors are read, one level at a time: the
    # rdfs:subClassOf rows of a whole index can be millions.
    parents: dict[str, set[str]] = {}
    frontier = sorted(set(terms))
    while frontier:
        found: dict[str, set[str]] = defaultdict(set)
        if rows is not None:
            wanted = [f"<{t}>" for t in frontier] + [t for t in frontier if t.startswith("<")]
            for s, o in rows.filter(pl.col("s").is_in(wanted)).unique().collect().iter_rows():
                child, parent = _bare(s), _bare(o)
                if parent != child and parent not in NOT_DATA_TYPES:
                    found[child].add(parent)
        for term in frontier:
            parents[term] = found.get(term, set())
        frontier = sorted({p for ps in parents.values() for p in ps} - parents.keys())
    if hierarchy_files:
        tables = [read_hierarchy([path]) for path in hierarchy_files]
        fill_parents(parents, tables, list(parents))
    return parents


def _ancestors(parents: Mapping[str, set[str]], terms: Iterable[str]) -> dict[str, set[str]]:
    """Strict ancestors of each term (cycles followed once)."""
    out: dict[str, set[str]] = {}
    for term in terms:
        seen: set[str] = set()
        stack = list(parents.get(term, ()))
        while stack:
            node = stack.pop()
            if node not in seen:
                seen.add(node)
                stack.extend(parents.get(node, ()))
        seen.discard(term)
        out[term] = seen
    return out


# Minimal types


def minimal_types(
    store: RowStore | StoreView,
    *,
    types: pl.LazyFrame | None = None,
    ontology_graph_uris: Sequence[str] | None = None,
    hierarchy_files: Sequence[str | Path] = (),
) -> pl.LazyFrame:
    """Return the type table with only the minimal types of each record.

    ABSTAT's minimal types (Spahiu, Porrini, Palmonari, Rula, Maurino: "ABSTAT: Ontology-driven
    Linked Data Summaries with Pattern Minimalization", ESWC 2016 Satellite Events): of the
    types of a record, those that are not a strict ancestor (rdfs:subClassOf closure) of
    another of its types. Two types that are ancestors of each other (equivalent by cycles) are
    both kept. Types outside the hierarchy are kept. Untyped records stay untyped (ABSTAT gives
    them owl:Thing; rdfsolve gives their IRI subjects the untyped patterns of rdfs:Resource). Where no record asserts an
    ancestor of its own type the table is unchanged.
    """
    import polars as pl

    table = _table(types) if types is not None else type_table(store)
    classes = _type_classes(table)
    parents = superclasses(
        store,
        classes,
        ontology_graph_uris=ontology_graph_uris,
        hierarchy_files=hierarchy_files,
    )
    ancestors = _ancestors(parents, classes)
    pairs = [
        (f"<{term}>", f"<{above}>")
        for term, ups in ancestors.items()
        for above in ups
        if term not in ancestors.get(above, ())
    ]
    if not pairs:
        return table
    below = pl.LazyFrame(pairs, schema={"t": pl.String, "c": pl.String}, orient="row")
    # The records are joined by QLever's id when the table has it, not by their text.
    record = _keys(table)[-1]
    redundant = (
        table.join(table.select(record, t="c"), on=record)
        .join(below, on=["t", "c"], how="semi")
        .select(record, "c")
        .unique()
    )
    return table.join(redundant, on=[record, "c"], how="anti")


# Grouping


def count_aware_cut(
    terms: Iterable[str],
    parents: Mapping[str, set[str]],
    budget: int,
    instances: Mapping[str, int],
    fixed: Iterable[str] = (),
) -> Subsumption:
    """Merge the class with the fewest instances into a parent until at most *budget* remain.

    An option to choose_representatives's level-by-level lifting, which overshoots where
    hierarchies converge (one more level can drop far below the budget). The
    weight of a representative is the sum of its members' instances (used only to rank). The
    parent chosen is one already kept, else the heaviest, then by IRI. A class without a parent
    stays. Untuned: with several parents it can climb across ontologies;
    *fixed* classes (for example shape groups) count toward the budget and are never moved.
    """
    fixed_set = set(fixed)
    rep = {t: t for t in sorted(set(terms)) if t not in fixed_set}
    result = Subsumption(budget=budget)
    weight: dict[str, int] = defaultdict(int)
    members: dict[str, set[str]] = defaultdict(set)
    for term in rep:
        weight[term] += instances.get(term, 0)
        members[term].add(term)
    live = set(rep)
    result.classes_before = len(live | fixed_set)
    heap = [(weight[t], t) for t in sorted(live)]
    heapq.heapify(heap)
    extra = len(fixed_set - live)
    while len(live) + extra > budget and heap:
        w, node = heapq.heappop(heap)
        if node not in live or w != weight[node]:
            continue
        ups = sorted(p for p in parents.get(node, ()) if p != node)
        if not ups:
            continue
        target = min(ups, key=lambda p: (p not in live, -weight.get(p, 0), p))
        for term in members.pop(node):
            rep[term] = target
            members[target].add(term)
        live.discard(node)
        weight[target] = weight.get(target, 0) + weight.pop(node)
        live.add(target)
        result.levels_lifted += 1
        heapq.heappush(heap, (weight[target], target))
    result.representative = rep
    result.classes_after = len(set(rep.values()) | fixed_set)
    result.over_budget = result.classes_after > budget
    return result


def _shapes(
    store: RowStore | StoreView, types: pl.LazyFrame, terms: Sequence[str]
) -> dict[str, Any]:
    """ontology_as_data.fetch_shapes from the rows: the predicates of the instances of each term
    in the whole index (ql:has-predicate), the membership predicates left out; their number and
    one of them.

    The subjects of each predicate are read as those of the instances (by id when the type
    table has ids), so that only the instances' subjects are made distinct, never all the
    subjects of a large predicate.
    """
    import polars as pl

    from rdfsolve.mining.ontology_as_data import Shape

    shapes = {t: Shape(frozenset()) for t in terms}
    if not terms:
        return shapes
    base = getattr(store, "base", store)
    wanted = pl.LazyFrame({"c": [f"<{t}>" for t in terms]}, schema={"c": pl.String})
    instances = types.join(wanted, on="c", how="semi").collect()
    key = _keys(types)[-1]
    nodes = instances.lazy().select(key).unique()
    membership = set(MEMBERSHIP.get())
    found = []
    for predicate in base.predicates:
        if predicate in membership:
            continue
        subjects = base.rows(predicate).select(key).join(nodes, on=key, how="semi").unique()
        found.append(
            instances.lazy()
            .join(subjects, on=key, how="semi")
            .select("c")
            .unique()
            .with_columns(p=pl.lit(predicate))
            .collect()
        )
    if found:
        for c, props in pl.concat(found).group_by("c").agg(pl.col("p")).iter_rows():
            shapes[_bare(c)] = Shape(frozenset(props))
    for c, n, example in (
        instances.group_by("c")
        .agg(n=pl.col("s").n_unique(), x=pl.col("s").sort().first())
        .iter_rows()
    ):
        shapes[_bare(c)].instances = n
        shapes[_bare(c)].example = _bare(example)
    return shapes


@dataclass
class TermGrouping:
    """The grouped view of a scan run and how it was made.

    *patterns* are counted on the rewritten type table (exact; count_semantics as
    count_patterns gives, never upper_bound); *raw_patterns* are the per-term rows (the exact
    layer, the miner's raw_patterns); *representative* maps each grouped term to its class,
    *members* each class to its terms; *summary* is the report record
    ontology_term_subsumption, *before_mining* ontology_term_grouping (or None); *types* the
    rewritten type table.
    """

    patterns: list[SchemaPattern]
    raw_patterns: list[SchemaPattern]
    representative: dict[str, str]
    members: dict[str, list[str]]
    summary: dict[str, Any]
    before_mining: dict[str, Any] | None
    types: pl.LazyFrame
    subsumed_classes: set[str] = field(default_factory=set)
    # The per-term type table the grouping started from (minimal types when asked), which
    # raw_patterns count: the release of the exact layer reads it (term_release).
    base_types: pl.LazyFrame | None = None
    settings: dict[str, Any] = field(default_factory=dict)
    # The per-term rows written to a file (count_patterns with rows_path) when the terms were
    # grouped before counting: the release reads them instead of raw_patterns, which then
    # count the grouped table.
    raw_rows: Path | None = None
    # Records kept as classes released as their own terms (classes_as_data): each record class
    # and its kind (c, kind, as <iri>), a lazy table; term_release then streams the classes.
    record_kinds: pl.LazyFrame | None = None


def _members(representative: Mapping[str, str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for term, rep in representative.items():
        if rep != term:
            out[rep].append(term)
    return {rep: sorted(terms) for rep, terms in sorted(out.items())}


def _group_before_mining(
    store: RowStore | StoreView,
    types: pl.LazyFrame,
    classes: list[str],
    budget: int,
    limit: int,
    ontology_graph_uris: Sequence[str] | None,
    hierarchy_files: Sequence[str | Path],
) -> tuple[dict[str, str], dict[str, Any]]:
    """two_phase_strategy._group_terms on the rows: the same choice and the same record."""
    from rdfsolve.ontology.hierarchy import read_hierarchy

    parents = superclasses(store, classes, ontology_graph_uris=ontology_graph_uris)
    files = {str(path): read_hierarchy([path]) for path in hierarchy_files}
    loaded: list[str] = []
    if files:
        from rdfsolve.ontology.hierarchy import fill_parents

        loaded = fill_parents(parents, list(files.values()), classes)
    chosen = choose_representatives(classes, parents, budget)
    candidates = parentless_candidates(chosen, parents)
    shapes = _shapes(store, types, candidates)
    shape_groups = group_by_shape(chosen, parents, {t: s.properties for t, s in shapes.items()})
    members = chosen.members()
    record = {
        "before_mining": True,
        "limit": limit,
        "budget": budget,
        "classes_before": chosen.classes_before,
        "classes_after": chosen.classes_after,
        "levels_lifted": chosen.levels_lifted,
        "over_budget": chosen.over_budget,
        "hierarchy_graph_uris": list(ontology_graph_uris) if ontology_graph_uris else None,
        "terms_without_parent_by_namespace": dict(
            Counter(namespace(c) for c in classes if not parents.get(c)).most_common()
        ),
        "hierarchy_files": [
            {"path": path, "pairs": sum(len(ps) for ps in table.values())}
            for path, table in files.items()
        ],
        "terms_with_loaded_parent_by_namespace": dict(
            Counter(namespace(c) for c in loaded).most_common()
        ),
        "grouping_of_terms_without_parent": "shape",
        "namespace_min_terms": NAMESPACE_GROUP_MIN_TERMS,
        "shape_groups": {
            group: {
                "properties": sorted(shape),
                "terms": len(members.get(group, [])),
                "namespaces": dict(
                    Counter(namespace(t) for t in members.get(group, [])).most_common()
                ),
                "instances": sum(shapes[t].instances for t in members.get(group, [])),
                "example_instance": next(
                    (shapes[t].example for t in members.get(group, []) if shapes[t].example),
                    None,
                ),
            }
            for group, shape in shape_groups.items()
        },
        "terms_in_shapes_of_one_term": sum(1 for t in shapes if chosen.representative.get(t) == t),
        "unreadable_shape_terms": [],
        "representative_members": members,
        "review_state": "unreviewed",
        "counts": "recounted from the rewritten type table",
    }
    return dict(chosen.representative), record


@dataclass
class GroupedBeforeCounting:
    """The type table that a scan counts when its classes pass group_before_mining."""

    # The rewritten type table (s, sid, c) and the store that reads it.
    types: pl.LazyFrame
    store: RetypedStore
    # The record of the grouping (report config ontology_term_grouping).
    record: dict[str, Any]
    # Each grouped term and its representative (bare IRIs), for the generic choice; None for
    # records as classes, whose terms are mapped to their kind by a join (too many to list).
    representative: dict[str, str] | None
    # Records kept as classes (classes_as_data): each record class and its kind (c, kind, as
    # <iri>), a lazy table, never a list: there can be tens of millions.
    kinds: pl.LazyFrame | None = None


def _record_parents(store: RowStore | StoreView, table: pl.LazyFrame) -> pl.LazyFrame:
    """Return (c, kind) for each rdfs:subClassOf parent in the data of a class of *table*.

    Parents in NOT_DATA_TYPES and the class itself are left out; terms are written <iri>.
    """
    import polars as pl

    if SUBCLASS_OF not in store.base.predicates:
        return pl.LazyFrame(schema={"c": pl.String, "kind": pl.String})
    parents = (
        store.base.rows(SUBCLASS_OF)
        .filter(pl.col("s").str.starts_with("<") & (pl.col("kind") == "iri"))
        .select(c="s", kind="o")
        .filter(
            (pl.col("c") != pl.col("kind"))
            & ~pl.col("kind").is_in([f"<{t}>" for t in NOT_DATA_TYPES])
        )
    )
    return parents.join(table.select("c").unique(), on="c", how="semi").unique()


def _record_kinds(store: RowStore | StoreView, table: pl.LazyFrame) -> pl.LazyFrame:
    """Return (c, kind) for each class of *table* with an rdfs:subClassOf parent in the data:
    a record kept as a class, under its kind (classes_as_data). A class with several parents
    takes the first by IRI, so that each record has one kind and counts stay exact.
    """
    import polars as pl

    return _record_parents(store, table).group_by("c").agg(pl.col("kind").min())


def record_types(store: RowStore | StoreView, table: pl.LazyFrame) -> pl.LazyFrame:
    """Return the per-record type table of records kept as classes (classes_as_data).

    The minimal types (minimal_types) under the data's rdfs:subClassOf rows, one level: a
    record typed with a record class and with that class's parent keeps the record class.
    Computed by joins on the rows, without listing the classes.
    """
    keys = _keys(table)
    table = table.select(*keys, "c").unique()
    redundant = table.join(_record_parents(store, table), on="c").select(*keys, c="kind")
    return table.join(redundant.unique(), on=[*keys, "c"], how="anti")


def group_before_counting(
    store: RowStore | StoreView,
    *,
    limit: int | None,
    budget: int = 300,
    classes_as_data: bool = False,
    ontology_graph_uris: Sequence[str] | None = None,
    hierarchy_files: Sequence[str | Path] = (),
) -> GroupedBeforeCounting | None:
    """Group the terms used as types before a scan counts its patterns; None below *limit*.

    rdfsolve groups the ontology terms used as types under their ancestors before mining when
    a dataset has more than group_before_mining classes (two_phase_strategy._group_terms).
    Counting every class first and grouping afterwards does not fit in memory with millions of
    classes. Here the type table is rewritten first and counted once:

    - with *classes_as_data* (each record an rdfs:Class under its kind), each
      class takes its rdfs:subClassOf parent in the data, by a join, as the SPARQL path groups
      term rows under their ancestors; a table that still has more than *limit* classes is
      then grouped as below;
    - otherwise, _group_before_mining: rdfsolve's representatives under the ancestors, the
      parentless terms grouped by shape, within *budget*.
    """
    import polars as pl

    table = type_table(store)
    if limit is None:
        return None
    # The distinct classes of the type rows, read without making the rows distinct first.
    count = int(
        store.graph_types()
        .filter(pl.col("c").str.starts_with("<"))
        .select(pl.col("c").n_unique())
        .collect(engine="streaming")
        .item()
    )
    if count <= limit:
        return None
    record: dict[str, Any] = {"before_mining": True, "before_counting": True, "limit": limit}
    representative: dict[str, str] | None = None
    rows: pl.LazyFrame | None = None
    kinds = None
    if classes_as_data:
        kinds = _record_kinds(store, table)
        table = (
            table.join(kinds, on="c", how="left")
            .select(*_keys(table), c=pl.coalesce("kind", "c"))
            .unique()
        )
        after = int(table.select(pl.col("c").n_unique()).collect(engine="streaming").item())
        record.update(
            classes_as_data=True,
            classes_before=count,
            records_as_classes=int(kinds.select(pl.len()).collect(engine="streaming").item()),
            classes_after=after,
        )
        count = after
    if count > limit:
        classes = _type_classes(table)
        chosen, found = _group_before_mining(
            store, table, classes, budget, limit, ontology_graph_uris, hierarchy_files
        )
        representative = {t: r for t, r in chosen.items() if r != t}
        # Counting reads the rewritten rows (RetypedStore): the table of a large index is
        # never made distinct whole, only by id and in parts (count_patterns).
        rows = retype(table, representative, distinct=False)
        table = rows.unique()
        record = {**found, **{k: v for k, v in record.items() if k != "limit"}, "limit": limit}
    return GroupedBeforeCounting(
        types=table,
        store=RetypedStore(store, table, rows),
        record=record,
        representative=representative,
        kinds=kinds,
    )


def exact_term_rows(
    before: GroupedBeforeCounting,
    grouping: TermGrouping,
    path: Path,
    *,
    expressions: Iterable[str] = (),
    ontology_graph_uris: Sequence[str] | None = None,
    hierarchy_files: Sequence[str | Path] = (),
) -> None:
    """Count the exact terms of a scan grouped before counting into *path*; update *grouping*.

    The patterns count the grouped table; the release (term_release) still ships the exact
    terms: counted here from the store's own types, written batch by batch (count_patterns with
    rows_path), with no patterns held. The types are the minimal ones (minimal_types); records
    kept as classes (classes_as_data) take record_types, by joins, and their kinds stay a lazy
    table (grouping.record_kinds), so that 65.9 million record classes are never listed. Each
    term takes the class its group is given: its first representative (or kind), then
    *grouping*'s.
    """
    from rdfsolve.mining.scan import count_patterns

    raw = cast("RowStore | StoreView", before.store._store)
    table = without_classes(type_table(raw), expressions)
    if before.kinds is not None:
        types = record_types(raw, table)
    else:
        types = minimal_types(
            raw,
            types=table,
            ontology_graph_uris=ontology_graph_uris,
            hierarchy_files=hierarchy_files,
        )
    count_patterns(RetypedStore(raw, types), rows_path=path)
    after = dict(grouping.representative)
    combined = {t: after.get(r, r) for t, r in (before.representative or {}).items()}
    combined.update({t: r for t, r in after.items() if t not in combined})
    grouping.representative = combined
    grouping.base_types = types
    grouping.raw_rows = path
    grouping.settings["grouped_before_counting"] = True
    if before.kinds is not None:
        grouping.record_kinds = before.kinds
        grouping.settings["records_as_classes"] = True


def group_terms(
    store: RowStore | StoreView,
    *,
    budget: int = 300,
    group_before_mining: int | None = None,
    types: pl.LazyFrame | None = None,
    ontology_graph_uris: Sequence[str] | None = None,
    hierarchy_files: Sequence[str | Path] = (),
    rule: Literal["levels", "count_aware"] = "levels",
    minimal: bool = False,
    counted: list[SchemaPattern] | None = None,
) -> TermGrouping:
    """Group the ontology terms used as types and count the grouped patterns exactly.

    The choice is rdfsolve's, unchanged: above *group_before_mining* classes, the grouping
    before mining (two_phase_strategy._group_terms: choose_representatives, then the terms no
    ancestor takes grouped by shape); then, as miner._run_term_subsumption_phase does on the
    mined patterns, choose_representatives again over the classes of the patterns when they are
    more than *budget*. Both choices are composed into one map, the type table is rewritten
    once (subjects and objects alike, the intent of the object grouping of commit 4ac8ce20)
    and the patterns are counted again with count_patterns. With *rule* "count_aware" the
    second choice is count_aware_cut (an option; its parent choice is untuned). With *minimal*
    the type table is first reduced to minimal types (minimal_types). *counted* are the
    patterns already counted from this type table, if any: they are the per-term layer, and
    when no term is grouped they are the patterns too (no count is made again).

    The classes after mining are those of the type table and the object classes of the
    patterns: the miner's pattern_classes also sees the classes of the membership rows
    (C, rdf:type, Resource), which it later leaves out; the type table holds them all.
    """
    from rdfsolve.mining.scan import count_patterns

    table = _table(types) if types is not None else type_table(store)
    if minimal:
        table = minimal_types(
            store,
            types=table,
            ontology_graph_uris=ontology_graph_uris,
            hierarchy_files=hierarchy_files,
        )
    raw = (
        counted
        if counted is not None and not minimal
        else count_patterns(RetypedStore(store, table))
    )
    classes = _type_classes(table)
    before: dict[str, str] = {}
    before_record = None
    if group_before_mining is not None and len(classes) > group_before_mining:
        before, before_record = _group_before_mining(
            store,
            table,
            classes,
            budget,
            group_before_mining,
            ontology_graph_uris,
            hierarchy_files,
        )
    grouped_before = {t: r for t, r in before.items() if r != t}
    first = retype(table, grouped_before)
    objects = {p.object_class for p in raw if p.object_class not in _SENTINEL_OBJECTS}
    pattern_classes = (
        set(_type_classes(first)) | {grouped_before.get(o, o) for o in objects}
    ) - set(_SENTINEL_OBJECTS)
    summary: dict[str, Any] = {
        "budget": budget,
        "probed_patterns": None,
        "classes_before": len(pattern_classes),
        "classes_after": len(pattern_classes),
        "subsumed": False,
        "representative_members": {},
        "classes_as_data": False,
        "object_classes_grouped_before_mining": {
            "before": len(objects),
            "after": len({grouped_before.get(o, o) for o in objects}),
        },
        "counts": "recounted from the rewritten type table",
        "rule": rule,
        "minimal_types": minimal,
    }
    after: dict[str, str] = {}
    if len(pattern_classes) > budget:
        parents = superclasses(
            store,
            pattern_classes,
            ontology_graph_uris=ontology_graph_uris,
        )
        if rule == "count_aware":
            import polars as pl

            counts = first.group_by("c").agg(n=pl.col("s").n_unique()).collect()
            instances = {_bare(c): n for c, n in counts.iter_rows()}
            chosen = count_aware_cut(pattern_classes, parents, budget, instances)
        else:
            chosen = choose_representatives(pattern_classes, parents, budget)
        after = dict(chosen.representative)
        members_after = chosen.members()
        summary.update(
            {
                "classes_after": chosen.classes_after,
                "subsumed": bool(members_after),
                "levels_lifted": chosen.levels_lifted,
                "over_budget": chosen.over_budget,
                "hierarchy_source": "selected named graphs rdfs:subClassOf"
                if ontology_graph_uris
                else "endpoint default dataset rdfs:subClassOf",
                "hierarchy_graph_uris": list(ontology_graph_uris) if ontology_graph_uris else None,
                "representatives": {rep: len(terms) for rep, terms in members_after.items()},
                "representative_members": members_after,
            }
        )
    combined = {t: after.get(r, r) for t, r in grouped_before.items()}
    combined.update({t: r for t, r in after.items() if r != t and t not in combined})
    rewritten = retype(table, combined) if combined else table
    patterns = (
        count_patterns(RetypedStore(store, rewritten))
        if combined
        else [p.model_copy(deep=True) for p in raw]
    )
    members = _members(combined)
    # A row of a representative is an interpretation of its members' rows (as the miner marks
    # subsumed rows); its counts are exact.
    for pattern in patterns:
        if pattern.subject_class in members or pattern.object_class in members:
            pattern.evidence_source = "inferred"
    return TermGrouping(
        patterns=patterns,
        raw_patterns=raw,
        representative=combined,
        members=members,
        summary=summary,
        before_mining=before_record,
        types=rewritten,
        subsumed_classes=set(members),
        base_types=table,
        settings={
            "budget": budget,
            "group_before_mining": group_before_mining,
            "rule": rule,
            "minimal_types": minimal,
            "ontology_graph_uris": list(ontology_graph_uris) if ontology_graph_uris else None,
            "hierarchy_files": [str(path) for path in hierarchy_files],
        },
    )


# Class expressions


def foldable_expressions(store: RowStore | StoreView) -> dict[str, tuple[str, str]]:
    """Return the anonymous classes of the form ``p some F`` (F a named class): IRI -> (p, F).

    The classes are those name_class_expressions named (store.class_expressions); other
    expressions (intersections, ``only``, cardinalities, nested ones) stay classes.
    """
    out = {}
    for iri, expression in store.class_expressions.items():
        match = SOME.match(expression.get("manchester_iris", ""))
        if match:
            out[iri] = (match.group(1), match.group(2))
    return dict(sorted(out.items()))


@dataclass
class ClassExpressionFolding:
    """Edge rows read from the class expressions that type records, and how they were made.

    *patterns* (C, p, F'): count, distinct subjects = the members of C typed by ``p some F``
    with F grouped to F' in the slot (C, p); distinct objects = the filler terms F under F'.
    They are marked evidence_source "inferred" (an instance-level reading of the restriction,
    not an asserted triple) and listed in *record* (the report record class_expression_folding).
    """

    patterns: list[SchemaPattern]
    representative: dict[tuple[str, str], dict[str, str]]
    record: dict[str, Any]


def fold_class_expressions(
    store: RowStore | StoreView,
    expressions: Mapping[str, tuple[str, str]],
    *,
    types: pl.LazyFrame | None = None,
    slot_budget: int = SLOT_BUDGET,
    ontology_graph_uris: Sequence[str] | None = None,
    hierarchy_files: Sequence[str | Path] = (),
) -> ClassExpressionFolding:
    """Fold the members of ``p some F`` classes into edge rows (C, p, F), F grouped per slot.

    Each member x of a named class C (from
    *types*, for example the grouped type table, without the expression classes) typed by
    ``p some F`` gives the row (C, p, F), counted over members; then the fillers of each slot
    (C, p) are grouped with choose_representatives to at most *slot_budget* classes over the
    rdfs:subClassOf hierarchy, and counted again (a member once per slot and representative).
    """
    import polars as pl

    record: dict[str, Any] = {
        "slot_budget": slot_budget,
        "expressions": len(expressions),
        "slots": {},
    }
    if not expressions:
        return ClassExpressionFolding([], {}, record)
    lookup = pl.LazyFrame(
        [(f"<{iri}>", p, f) for iri, (p, f) in expressions.items()],
        schema={"c": pl.String, "p": pl.String, "f": pl.String},
        orient="row",
    )
    typed = store.types().join(lookup, on="c", how="inner").select("s", "p", "f").unique()
    named = (types if types is not None else store.types()).filter(
        pl.col("c").str.starts_with("<") & ~pl.col("c").is_in([f"<{iri}>" for iri in expressions])
    )
    edges = (
        typed.join(named, on="s", how="inner")
        .select("s", "p", "f", c=pl.col("c").str.strip_prefix("<").str.strip_suffix(">"))
        .collect()
    )
    record["members"] = typed.select("s").unique().collect().height
    record["members_without_named_class"] = (
        typed.join(named, on="s", how="anti").select("s").unique().collect().height
    )
    fillers = sorted(set(edges["f"]))
    parents = superclasses(
        store, fillers, ontology_graph_uris=ontology_graph_uris, hierarchy_files=hierarchy_files
    )
    maps: dict[tuple[str, str], dict[str, str]] = {}
    for (c, p), slot in edges.group_by("c", "p"):
        terms = sorted(set(slot["f"]))
        chosen = choose_representatives(terms, parents, slot_budget)
        maps[(str(c), str(p))] = dict(chosen.representative)
        record["slots"][f"{c} {p}"] = {
            "fillers": len(terms),
            "classes_after": chosen.classes_after,
            "levels_lifted": chosen.levels_lifted,
            "over_budget": chosen.over_budget,
            "representative_members": chosen.members(),
        }
    mapped = edges.with_columns(
        r=pl.struct("c", "p", "f").map_elements(
            lambda row: maps[(row["c"], row["p"])].get(row["f"], row["f"]),
            return_dtype=pl.String,
        )
    )
    counted = mapped.group_by("c", "p", "r").agg(
        members=pl.col("s").n_unique(), fillers=pl.col("f").n_unique()
    )
    patterns = [
        SchemaPattern(
            subject_class=c,
            property_uri=p,
            object_class=r,
            count=n,
            distinct_subjects=n,
            distinct_objects=k,
            count_semantics="endpoint_default",
            evidence_source="inferred",
            pattern_type=PatternType.OBJECT_PROPERTY,
        )
        for c, p, r, n, k in counted.sort("c", "p", "r").iter_rows()
    ]
    record["patterns"] = [[p.subject_class, p.property_uri, p.object_class] for p in patterns]
    return ClassExpressionFolding(patterns, maps, record)


def hierarchy_edges(
    store: RowStore | StoreView,
    terms: Iterable[str],
    *,
    ontology_graph_uris: Sequence[str] | None = None,
    hierarchy_files: Sequence[str | Path] = (),
) -> pl.DataFrame:
    """Return the hierarchy of *terms* as the grouping reads it: (child, parent, source).

    *source* is "data" for an rdfs:subClassOf row of the store (superclasses(), followed to the
    roots), "file" for a pair the hierarchy files give to one of *terms* without a parent in the
    data, and to its ancestors (fill_parents with *terms*, as _group_before_mining calls it).
    """
    import polars as pl

    from rdfsolve.ontology.hierarchy import fill_parents, read_hierarchy

    terms = sorted(set(terms))
    data = superclasses(store, terms, ontology_graph_uris=ontology_graph_uris)
    full = {term: set(ps) for term, ps in data.items()}
    if hierarchy_files:
        fill_parents(full, [read_hierarchy([path]) for path in hierarchy_files], terms)
    rows = [
        (child, parent, "data" if parent in data.get(child, ()) else "file")
        for child, parents in sorted(full.items())
        for parent in sorted(parents)
    ]
    return pl.DataFrame(
        rows, schema={"child": pl.String, "parent": pl.String, "source": pl.String}, orient="row"
    )
