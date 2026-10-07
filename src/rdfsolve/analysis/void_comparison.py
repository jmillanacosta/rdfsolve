"""Neutral comparison of a schema read from a published VoID with a schema mined from the data.

Both describe the same RDF data: the VoID that an endpoint publishes (rdfsolve.mining.
void_strategy) and the schema mined from the endpoint or from a local index of the same dumps.
Neither is treated as ground truth. The comparison reports, for classes, properties, class and
property pairs and whole patterns, what both state and what only one states; for the patterns
both state with a count, how the counts agree; and what each cost (queries and seconds, from the
mining reports).

A local index may hold only a sample of some graphs (the registry's ``sampled_graphs``). The
mined counts of those graphs are counts of the sample,
not of the data the VoID describes: such patterns are left out of the count agreement and
counted apart, and a pattern that only the VoID states is no evidence that the data lacks it.

The members of each class (void:entities of a class partition, and the mined entity counts) are
compared as the pattern counts are. A VoID that a catalog publishes of a dataset describes the dataset's data graph; a mined schema of more graphs (the dataset's own VoID graph
beside its data) is cut to the described graphs first (restrict_to_graphs).
"""

from __future__ import annotations

from statistics import median
from typing import Any

from pydantic import BaseModel, Field

from rdfsolve.schema_models._constants import UNTYPED_SUBJECTS_LABEL
from rdfsolve.schema_models.pattern import SchemaPattern

_KINDS = ("Literal", "Resource", "BlankNode")


def _subject(p: SchemaPattern) -> str:
    """Return a pattern's subject class, or "untyped subjects" for subjects without a type."""
    return UNTYPED_SUBJECTS_LABEL if p.untyped_subject else p.subject_class


def _key(p: SchemaPattern) -> tuple[str, str, str]:
    """Return a pattern's subject class, property, and object class or datatype."""
    obj = f"Literal^^{p.datatype or ''}" if p.object_class == "Literal" else p.object_class
    return _subject(p), p.property_uri, obj


class SetComparison(BaseModel):
    """What two schemas state for one dimension."""

    both: int
    void_only: int
    mined_only: int
    examples_void_only: list[str] = Field(default_factory=list)
    examples_mined_only: list[str] = Field(default_factory=list)

    @classmethod
    def of(cls, void: set[Any], mined: set[Any], examples: int = 10) -> SetComparison:
        """Compare two sets, with a few sorted examples of each side's own."""
        return cls(
            both=len(void & mined),
            void_only=len(void - mined),
            mined_only=len(mined - void),
            examples_void_only=[str(x) for x in sorted(void - mined, key=str)[:examples]],
            examples_mined_only=[str(x) for x in sorted(mined - void, key=str)[:examples]],
        )


class CountAgreement(BaseModel):
    """How the counts of the patterns that both schemas state agree."""

    compared: int
    # Patterns both state with counts, whose mined count is of a sampled graph: not compared.
    sampled_not_compared: int = 0
    within_1_percent: int
    within_10_percent: int
    median_ratio: float | None
    largest_differences: list[dict[str, Any]] = Field(default_factory=list)


class VoidComparison(BaseModel):
    """A VoID-read schema beside a mined schema of the same data."""

    classes: SetComparison
    properties: SetComparison
    class_properties: SetComparison
    patterns: SetComparison
    counts: CountAgreement
    # Members of the classes that both state with a count (None: not given).
    class_counts: CountAgreement | None = None
    cost: dict[str, dict[str, Any]]
    # Graphs of the mined side whose data is a sample, with how they were sampled.
    sampled_graphs: dict[str, str] = Field(default_factory=dict)
    # full_data, or sample when a sampled graph is in the mined side's scope.
    mined_count_basis: str = "full_data"
    # Whether a pattern stated only by the VoID shows that the mined data lacks it.
    void_only_absence_supported: bool = True


def restrict_to_graphs(patterns: list[SchemaPattern], graphs: set[str]) -> list[SchemaPattern]:
    """Return the patterns of a mined schema found in GRAPHS, counted in those graphs only.

    A pattern without per-graph counts is kept as it is (its graphs are not known).
    """
    kept = []
    for p in patterns:
        if p.graphs is None:
            kept.append(p)
            continue
        inside = {g: n for g, n in p.graphs.items() if g in graphs}
        if not inside:
            continue
        if inside == p.graphs:
            kept.append(p)
            continue
        kept.append(p.model_copy(update={"graphs": inside, "count": sum(inside.values())}))
    return kept


def _agreement(pairs: list[tuple[int, int, Any]], not_compared: int = 0) -> CountAgreement:
    """Return how the counts (VoID, mined, key) of the items that both state agree."""
    ratios = sorted(
        ((v / m, key, v, m) for v, m, key in pairs), key=lambda r: abs(r[0] - 1), reverse=True
    )
    return CountAgreement(
        compared=len(ratios),
        sampled_not_compared=not_compared,
        within_1_percent=sum(abs(r[0] - 1) <= 0.01 for r in ratios),
        within_10_percent=sum(abs(r[0] - 1) <= 0.1 for r in ratios),
        median_ratio=round(median(r[0] for r in ratios), 4) if ratios else None,
        largest_differences=[
            {
                "pattern": list(k) if isinstance(k, tuple) else k,
                "void": v,
                "mined": m,
                "ratio": round(r, 4),
            }
            for r, k, v, m in ratios[:10]
        ],
    )


def compare_void_with_mined(
    void_patterns: list[SchemaPattern],
    mined_patterns: list[SchemaPattern],
    *,
    void_report: dict[str, Any] | None = None,
    mined_report: dict[str, Any] | None = None,
    sampled_graphs: dict[str, str] | None = None,
    void_class_counts: dict[str, int] | None = None,
    mined_class_counts: dict[str, int] | None = None,
) -> VoidComparison:
    """Compare the patterns read from a VoID with those mined from the same data.

    *sampled_graphs* are the graphs whose mined data is a sample (registry sampled_graphs of the
    source of a local index). Those in the mined side's scope (its report's graph_uris, or the
    graphs of its patterns) make its counts a sample: a mined pattern counted in one of them, or
    without per-graph counts, is not compared by count.
    """
    scope = set((mined_report or {}).get("graph_uris") or ()) | {
        g for p in mined_patterns for g in (p.graphs or {})
    }
    sampled = {g: how for g, how in (sampled_graphs or {}).items() if not scope or g in scope}

    def from_sample(p: SchemaPattern) -> bool:
        """Return whether a mined pattern's count may be a count of a sampled graph."""
        return bool(sampled) and (p.graphs is None or any(g in sampled for g in p.graphs))

    def classes(ps: list[SchemaPattern]) -> set[str]:
        """Return the subject and object classes of patterns (subjects without a type: none)."""
        return {p.subject_class for p in ps if not p.untyped_subject} | {
            p.object_class for p in ps if p.object_class not in _KINDS
        }

    void_by_key = {_key(p): p for p in void_patterns}
    mined_by_key = {_key(p): p for p in mined_patterns}
    pairs = []
    not_compared = 0
    for key in void_by_key.keys() & mined_by_key.keys():
        v, m = void_by_key[key].count, mined_by_key[key].count
        if v and m:
            if from_sample(mined_by_key[key]):
                not_compared += 1
                continue
            pairs.append((v, m, key))
    class_counts = None
    if void_class_counts is not None and mined_class_counts is not None:
        class_counts = _agreement(
            [
                (void_class_counts[c], mined_class_counts[c], c)
                for c in sorted(void_class_counts.keys() & mined_class_counts.keys())
                if void_class_counts[c] and mined_class_counts[c]
            ]
        )

    def cost(report: dict[str, Any] | None) -> dict[str, Any]:
        """Return what a mining report says the schema cost."""
        if not report:
            return {}
        return {
            "strategy": report.get("strategy"),
            "queries_sent": report.get("total_queries_sent"),
            "queries_failed": report.get("total_queries_failed"),
            "seconds": sum((p.get("duration_s") or 0) for p in report.get("phases", [])),
            "measurement_gaps": len(report.get("measurement_gaps", [])),
        }

    return VoidComparison(
        classes=SetComparison.of(classes(void_patterns), classes(mined_patterns)),
        properties=SetComparison.of(
            {p.property_uri for p in void_patterns}, {p.property_uri for p in mined_patterns}
        ),
        class_properties=SetComparison.of(
            {(_subject(p), p.property_uri) for p in void_patterns},
            {(_subject(p), p.property_uri) for p in mined_patterns},
        ),
        patterns=SetComparison.of(set(void_by_key), set(mined_by_key)),
        counts=_agreement(pairs, not_compared),
        class_counts=class_counts,
        cost={"void": cost(void_report), "mined": cost(mined_report)},
        sampled_graphs=dict(sorted(sampled.items())),
        mined_count_basis="sample" if sampled else "full_data",
        void_only_absence_supported=not sampled,
    )
