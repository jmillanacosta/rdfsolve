"""Ontology terms used as types, grouped after counting from the rows of a row store.

A scan run (rdfsolve.mining.scan) has every row on disk, so the grouping of ontology terms is a
rewrite of the type table (term -> representative), applied to subjects and objects alike, and
the patterns are counted again with count_patterns: every count is exact. Summing the rows of the
member terms (ontology_as_data.subsume_patterns) is an upper bound: a record typed with two
members of one representative is counted twice (TERA: NCBI taxa typed with their taxon term and
their division, 6.2 M triples counted twice in 133 rows), and the distinct counts cannot be
summed at all. Experiment: article/experiments/scan-summary-20261006/terms/FINDINGS.md.

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
from typing import TYPE_CHECKING, Any, Literal

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
    "RetypedStore",
    "TermGrouping",
    "count_aware_cut",
    "fold_class_expressions",
    "foldable_expressions",
    "group_terms",
    "hierarchy_edges",
    "minimal_types",
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
    the store's own.
    """

    def __init__(self, store: RowStore | StoreView | RetypedStore, types: pl.LazyFrame) -> None:
        """Keep *store* and the type table (s, c) that replaces its own."""
        self._store = store
        self._types = types

    def __getattr__(self, name: str) -> Any:
        """Read everything but the types from the store."""
        return getattr(self._store, name)

    def graph_types(self) -> pl.LazyFrame:
        """Return the replaced type table (s, sid, c), without graphs."""
        return self._types

    def types(self) -> pl.LazyFrame:
        """Return the replaced type table (s, c)."""
        return self._types.select("s", "c").unique()

    def type_ids(self) -> pl.LazyFrame:
        """Return the replaced type table by id (sid, c), which count_patterns joins on."""
        return self._types.select("sid", "c").unique()

    def members(self) -> pl.LazyFrame:
        """Return the members of each class in the replaced type table."""
        members = self._store.members().select("s").unique()
        return self.types().join(members, on="s", how="semi")


def _keys(types: pl.LazyFrame) -> list[str]:
    """Return the record columns of a type table: s, and QLever's id sid when it has one."""
    return ["s", "sid"] if "sid" in types.collect_schema().names() else ["s"]


def type_table(store: RowStore | StoreView) -> pl.LazyFrame:
    """Return the type table of the store's scope with the ids: (s, sid, c), one row each."""
    rows = store.graph_types()
    return rows.select(*_keys(rows), "c").unique()


def _table(types: pl.LazyFrame) -> pl.LazyFrame:
    return types.select(*_keys(types), "c").unique()


def retype(types: pl.LazyFrame, representative: Mapping[str, str]) -> pl.LazyFrame:
    """Rewrite the classes of a type table (terms as <iri>) by *representative*; one row per
    (s, c), so a record typed with two members of one representative is one member of it.
    """
    import polars as pl

    keys = _keys(types)
    if not representative:
        return types.select(*keys, "c").unique()
    mapping = pl.LazyFrame(
        {
            "c": [f"<{t}>" for t in representative],
            "r": [f"<{r}>" for r in representative.values()],
        },
        schema={"c": pl.String, "r": pl.String},
    )
    return (
        types.select(*keys, "c")
        .join(mapping, on="c", how="left")
        .select(*keys, c=pl.coalesce("r", "c"))
        .unique()
    )


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
    # rdfs:subClassOf rows of a whole index can be millions (bio2rdf.chembl: 1.4 million).
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
    ancestor of its own type the table is unchanged (HRA-KG, lifesciencedict); TERA's NCBI taxa
    lose their division (1,829,829 rows).
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
    hierarchies converge (HRA-KG: 839 classes after 22 levels, 35 after 23, budget 300). The
    weight of a representative is the sum of its members' instances (used only to rank). The
    parent chosen is one already kept, else the heaviest, then by IRI. A class without a parent
    stays. Untuned: with several parents it can climb across ontologies (CL to UBERON, BFO);
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
    """
    import polars as pl

    from rdfsolve.mining.ontology_as_data import Shape

    shapes = {t: Shape(frozenset()) for t in terms}
    if not terms:
        return shapes
    base = getattr(store, "base", store)
    wanted = pl.LazyFrame({"c": [f"<{t}>" for t in terms]}, schema={"c": pl.String})
    instances = types.join(wanted, on="c", how="semi").collect()
    membership = set(MEMBERSHIP.get())
    found = []
    for predicate in base.predicates:
        if predicate in membership:
            continue
        subjects = base.rows(predicate).select("s").unique()
        found.append(
            instances.lazy()
            .join(subjects, on="s", how="semi")
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

    The owner's decision of 2026-10-06 (A + B): each member x of a named class C (from
    *types*, for example the grouped type table, without the expression classes) typed by
    ``p some F`` gives the row (C, p, F), counted over members; then the fillers of each slot
    (C, p) are grouped with choose_representatives to at most *slot_budget* classes over the
    rdfs:subClassOf hierarchy, and counted again (a member once per slot and representative).
    WikiPathways: 199 members of PCL_0010001 typed by ``RO_0015002 some CL_x`` (177 fillers)
    give 177 rows of one member each; with a slot budget of 10, 10 CL classes.
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
