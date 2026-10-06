"""Group the ontology terms of a released schema on the client, from its term release files.

A release ships exact terms (rdfsolve.mining.term_release): ``<stem>_terms.parquet`` (the
per-term patterns with their counts, and a manifest) and ``<stem>_term_classes.parquet`` (the
classes, their instances, parents, default representative and overlaps). These functions need
nothing else: no data, no endpoint.

    release = read_term_release("hra-kg_local_terms.parquet")
    table = regroup(release, representatives(release, budget=50))
    patterns = to_patterns(table)

- representatives(): rdfsolve's rule (choose_representatives, and the shape groups the release
  records) at any budget; at the release's budget it gives the release's default grouping;
- representatives_under(): each term under the most specific of chosen ancestors;
- regroup(): sums the per-term rows of each group, with flags of exactness. Triples are exact
  when no record has two types merged into one class (the release lists the pairs of classes
  that share records; with minimal types, a record's types are pairwise incomparable); distinct
  subjects when, in addition, each subject term gives one row to the group; distinct objects
  likewise for each object term. Otherwise the sum is an upper bound, and it is marked.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import polars as pl

    from rdfsolve.schema_models.pattern import SchemaPattern

SENTINELS = ("Literal", "Resource", "BlankNode")
SHAPE_GROUP_PREFIX = "https://w3id.org/rdfsolve/term-shape/"
KEY = ["subject_class", "property", "object_class", "datatype", "graph"]

__all__ = [
    "TermRelease",
    "read_term_release",
    "regroup",
    "representatives",
    "representatives_under",
    "to_patterns",
]


@dataclass
class TermRelease:
    """The per-term rows, the classes and the manifest of one source's term release."""

    terms: pl.DataFrame
    classes: pl.DataFrame
    manifest: dict[str, Any]

    def parents(self, sources: Iterable[str] = ("data", "file")) -> dict[str, set[str]]:
        """Return the parents of each class, from the hierarchy sources given."""
        wanted = set(sources)
        out: dict[str, set[str]] = {}
        for name, parents, kinds in self.classes.select(
            "class", "parents", "parent_sources"
        ).iter_rows():
            out[name] = {p for p, k in zip(parents or [], kinds or [], strict=True) if k in wanted}
        return out

    @property
    def default(self) -> dict[str, str]:
        """The release's default grouping: term -> representative (grouped terms only)."""
        rows = self.classes.filter(self.classes["representative"].is_not_null())
        return dict(rows.select("class", "representative").iter_rows())

    @property
    def type_classes(self) -> list[str]:
        """The classes with members in the type table."""
        return sorted(self.classes.filter(self.classes["instances"] > 0)["class"])


def read_term_release(terms: str | Path, classes: str | Path | None = None) -> TermRelease:
    """Read ``<stem>_terms.parquet`` and its classes file (named in its manifest)."""
    import polars as pl
    import pyarrow.parquet as pq

    terms = Path(terms)
    metadata = pq.read_schema(terms).metadata or {}
    manifest = json.loads(metadata.get(b"rdfsolve", b"{}"))
    if classes is None:
        classes = terms.with_name(manifest["classes_file"])
    return TermRelease(pl.read_parquet(terms), pl.read_parquet(classes), manifest)


def _shape_groups(
    release: TermRelease, chosen: Any, parents: Mapping[str, set[str]]
) -> dict[str, str]:
    """Return the release's shape groups for the terms no ancestor takes at this budget.

    group_by_shape needs the data (the predicates of the instances); the release records the
    groups of its default grouping, and they are used for the parentless candidates of this
    choice; a group left with one term is not a group.
    """
    from rdfsolve.mining.ontology_as_data import parentless_candidates

    shapes = {t: r for t, r in release.default.items() if r.startswith(SHAPE_GROUP_PREFIX)}
    found: dict[str, list[str]] = defaultdict(list)
    for term in parentless_candidates(chosen, parents):
        if term in shapes:
            found[shapes[term]].append(term)
    return {t: group for group, terms in found.items() if len(terms) > 1 for t in terms}


def representatives(
    release: TermRelease,
    budget: int,
    *,
    group_before_mining: int | Literal["default"] | None = "default",
    rule: Literal["levels", "count_aware"] = "levels",
) -> dict[str, str]:
    """Return term -> representative at *budget* with rdfsolve's rule, from the release alone.

    As scan_terms.group_terms: above *group_before_mining* type classes (the release's setting
    by default), choose_representatives over the type classes with the data and file parents,
    and the recorded shape groups for the terms no ancestor takes; then, above *budget* classes
    of the patterns, choose_representatives again with the data parents (or count_aware_cut).
    At the release's budget this is its default grouping. Only grouped terms are returned.
    """
    from rdfsolve.mining.ontology_as_data import choose_representatives

    settings = release.manifest.get("default_grouping") or {}
    if group_before_mining == "default":
        group_before_mining = settings.get("group_before_mining")
    classes = release.type_classes
    full = release.parents()
    data = release.parents(("data",))
    before: dict[str, str] = {}
    if group_before_mining is not None and len(classes) > group_before_mining:
        chosen = choose_representatives(classes, full, budget)
        before = dict(chosen.representative)
        before.update(_shape_groups(release, chosen, full))
    before = {t: r for t, r in before.items() if r != t}
    objects = {o for o in release.terms["object_class"].unique() if o not in SENTINELS}
    pattern_classes = (
        {before.get(c, c) for c in classes} | {before.get(o, o) for o in objects}
    ) - set(SENTINELS)
    after: dict[str, str] = {}
    if len(pattern_classes) > budget:
        if rule == "count_aware":
            from rdfsolve.mining.scan_terms import count_aware_cut

            weight: dict[str, int] = defaultdict(int)
            for name, n in release.classes.select("class", "instances").iter_rows():
                weight[before.get(name, name)] += n
            chosen = count_aware_cut(pattern_classes, data, budget, weight)
        else:
            chosen = choose_representatives(pattern_classes, data, budget)
        after = dict(chosen.representative)
    combined = {t: after.get(r, r) for t, r in before.items()}
    combined.update({t: r for t, r in after.items() if r != t and t not in combined})
    return {t: r for t, r in combined.items() if r != t}


def representatives_under(
    release: TermRelease, ancestors: Iterable[str]
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Put each class under the most specific of *ancestors* above it (or the class itself).

    Return the map and the classes with several most specific ancestors among *ancestors*
    (not comparable; the first by IRI is taken).
    """
    chosen = set(ancestors)
    parents = release.parents()
    ups: dict[str, set[str]] = {}

    def above(term: str) -> set[str]:
        """Return the ancestors of *term*, without itself (cached)."""
        if term not in ups:
            seen: set[str] = set()
            stack = list(parents.get(term, ()))
            while stack:
                node = stack.pop()
                if node not in seen:
                    seen.add(node)
                    stack.extend(parents.get(node, ()))
            seen.discard(term)
            ups[term] = seen
        return ups[term]

    out: dict[str, str] = {}
    ambiguous: dict[str, list[str]] = {}
    for term in release.classes["class"]:
        if term in chosen:
            continue
        hits = above(term) & chosen
        lowest = sorted(h for h in hits if not any(h in above(o) for o in hits if o != h))
        if lowest:
            out[term] = lowest[0]
            if len(lowest) > 1:
                ambiguous[term] = lowest
    return out, ambiguous


def regroup(release: TermRelease, representative: Mapping[str, str]) -> pl.DataFrame:
    """Sum the per-term rows into the groups of *representative*, with flags of exactness.

    Columns: subject_class, property, object_class, datatype, graph, triples,
    distinct_subjects, distinct_objects (sums), member_rows, triples_exact, subjects_exact,
    objects_exact. A sum that is not exact is an upper bound.
    """
    import polars as pl

    mapping = dict(representative)
    terms = release.terms
    # A representative whose members include two classes that share records counts them twice.
    pairs = (
        release.classes.select("class", "overlaps")
        .explode("overlaps")
        .drop_nulls("overlaps")
        .iter_rows()
    )
    doubled = {
        mapping.get(a, a)
        for a, b in pairs
        if mapping.get(a, a) == mapping.get(b, b) and (a in mapping or b in mapping)
    }
    frame = terms.with_columns(
        term_subject=pl.col("subject_class"),
        term_object=pl.col("object_class"),
        subject_class=pl.col("subject_class").replace(mapping),
        object_class=pl.when(pl.col("object_class").is_in(list(SENTINELS)))
        .then(pl.col("object_class"))
        .otherwise(pl.col("object_class").replace(mapping)),
    )
    grouped = frame.group_by(KEY, maintain_order=True).agg(
        triples=pl.col("triples").sum(),
        distinct_subjects=pl.col("distinct_subjects").sum(),
        distinct_objects=pl.col("distinct_objects").sum(),
        member_rows=pl.len(),
        subject_terms=pl.col("term_subject").n_unique(),
        object_terms=pl.col("term_object").n_unique(),
        unknown_distinct=pl.col("distinct_subjects").is_null().any()
        | pl.col("distinct_objects").is_null().any(),
    )
    doubled_list = sorted(doubled)
    triples_exact = ~pl.col("subject_class").is_in(doubled_list) & ~pl.col("object_class").is_in(
        doubled_list
    )
    sentinel = pl.col("object_class").is_in(list(SENTINELS))
    return grouped.with_columns(
        triples_exact=triples_exact,
        subjects_exact=triples_exact
        & (pl.col("member_rows") == pl.col("subject_terms"))
        & ~pl.col("unknown_distinct"),
        objects_exact=triples_exact
        & (
            (pl.col("member_rows") == 1)
            | (~sentinel & (pl.col("member_rows") == pl.col("object_terms")))
        )
        & ~pl.col("unknown_distinct"),
    ).drop("subject_terms", "object_terms", "unknown_distinct")


def to_patterns(table: pl.DataFrame) -> list[SchemaPattern]:
    """Return schema patterns of a regrouped table: a count that is not exact is an upper bound,
    a distinct count that is not exact is left out (None).
    """
    from rdfsolve.schema_models.pattern import PatternType, SchemaPattern

    merged: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in table.iter_rows(named=True):
        merged[
            (row["subject_class"], row["property"], row["object_class"], row["datatype"])
        ].append(row)
    out = []
    for (subject, prop, obj, datatype), rows in merged.items():
        scoped = rows[0]["graph"] is not None
        exact = all(r["triples_exact"] for r in rows)
        one = len(rows) == 1
        out.append(
            SchemaPattern(
                subject_class=subject,
                property_uri=prop,
                object_class=obj,
                datatype=datatype,
                count=sum(r["triples"] for r in rows),
                count_semantics=(
                    "upper_bound"
                    if not exact
                    else ("quad_occurrences" if len(rows) > 1 else "triples_in_graph")
                    if scoped
                    else "endpoint_default"
                ),
                graphs={r["graph"]: r["triples"] for r in rows} if scoped else None,
                distinct_subjects=rows[0]["distinct_subjects"]
                if one and rows[0]["subjects_exact"]
                else None,
                distinct_objects=rows[0]["distinct_objects"]
                if one and rows[0]["objects_exact"]
                else None,
                evidence_source="inferred" if any(r["member_rows"] > 1 for r in rows) else "mined",
                pattern_type={
                    "Literal": PatternType.DATATYPE_PROPERTY,
                    "BlankNode": PatternType.BLANK_NODE_PROPERTY,
                }.get(obj, PatternType.OBJECT_PROPERTY),
            )
        )
    return out
