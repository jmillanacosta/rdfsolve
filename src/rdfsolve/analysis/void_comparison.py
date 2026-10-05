"""Neutral comparison of a schema read from a published VoID with a schema mined from the data.

Both describe the same RDF data: the VoID that an endpoint publishes (rdfsolve.mining.
void_strategy) and the schema mined from the endpoint or from a local index of the same dumps.
Neither is treated as ground truth. The comparison reports, for classes, properties, class and
property pairs and whole patterns, what both state and what only one states; for the patterns
both state with a count, how the counts agree; and what each cost (queries and seconds, from the
mining reports).
"""

from __future__ import annotations

from statistics import median
from typing import Any

from pydantic import BaseModel, Field

from rdfsolve.schema_models.pattern import SchemaPattern

_KINDS = ("Literal", "Resource", "BlankNode")


def _key(p: SchemaPattern) -> tuple[str, str, str]:
    """Return a pattern's subject class, property, and object class or datatype."""
    obj = f"Literal^^{p.datatype or ''}" if p.object_class == "Literal" else p.object_class
    return p.subject_class, p.property_uri, obj


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
    cost: dict[str, dict[str, Any]]


def compare_void_with_mined(
    void_patterns: list[SchemaPattern],
    mined_patterns: list[SchemaPattern],
    *,
    void_report: dict[str, Any] | None = None,
    mined_report: dict[str, Any] | None = None,
) -> VoidComparison:
    """Compare the patterns read from a VoID with those mined from the same data."""

    def classes(ps: list[SchemaPattern]) -> set[str]:
        """Return the subject and object classes of patterns."""
        return {p.subject_class for p in ps} | {
            p.object_class for p in ps if p.object_class not in _KINDS
        }

    void_by_key = {_key(p): p for p in void_patterns}
    mined_by_key = {_key(p): p for p in mined_patterns}
    ratios = []
    for key in void_by_key.keys() & mined_by_key.keys():
        v, m = void_by_key[key].count, mined_by_key[key].count
        if v and m:
            ratios.append((v / m, key, v, m))
    ratios.sort(key=lambda r: abs(r[0] - 1), reverse=True)

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
            {(p.subject_class, p.property_uri) for p in void_patterns},
            {(p.subject_class, p.property_uri) for p in mined_patterns},
        ),
        patterns=SetComparison.of(set(void_by_key), set(mined_by_key)),
        counts=CountAgreement(
            compared=len(ratios),
            within_1_percent=sum(abs(r[0] - 1) <= 0.01 for r in ratios),
            within_10_percent=sum(abs(r[0] - 1) <= 0.1 for r in ratios),
            median_ratio=round(median(r[0] for r in ratios), 4) if ratios else None,
            largest_differences=[
                {"pattern": list(k), "void": v, "mined": m, "ratio": round(r, 4)}
                for r, k, v, m in ratios[:10]
            ],
        ),
        cost={"void": cost(void_report), "mined": cost(mined_report)},
    )
