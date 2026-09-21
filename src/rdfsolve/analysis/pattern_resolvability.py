"""Ontology-mediated resolvability of empirical class-property-value patterns.

This layer compares what KGs *instantiate*, not merely which ontology terms they
mention. Named OWL/RDFS relations are used as a conservative first pass; richer
OWL entailment or mapping-mediated resolution can be added without changing the
record contract.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field
from rdflib import Graph

from rdfsolve.analysis.ontology_resolvability import (
    NamedOntologyIndex,
    TermResolution,
    resolve_term_against,
)

PatternResolutionKind = Literal[
    "exact",
    "ontology_resolvable",
    "weak_shared_ancestor",
    "partial",
    "unresolved",
]

_SENTINELS = {"Literal", "Resource", "BlankNode", "IRI"}


class PatternView(BaseModel):
    """The class, property and value type of one observed pattern."""

    subject_class: str
    property_uri: str
    object_class: str | None = None
    datatype: str | None = None


class PatternResolution(BaseModel):
    """How one source pattern resolves against a target dataset's patterns."""

    source: PatternView
    target: PatternView | None = None
    kind: PatternResolutionKind
    subject_relation: TermResolution | None = None
    property_relation: TermResolution | None = None
    object_relation: TermResolution | None = None
    datatype_match: bool | None = None


class PatternResolvabilitySummary(BaseModel):
    """Pattern resolution counts between a source and a target dataset."""

    source_dataset: str
    target_dataset: str
    source_pattern_count: int
    by_kind: dict[str, int]
    patterns: list[PatternResolution]

    @property
    def strongly_resolved_fraction(self) -> float | None:
        """Return the share of exact or ontology-resolvable patterns, or None when there are none."""
        if not self.source_pattern_count:
            return None
        strong = self.by_kind.get("exact", 0) + self.by_kind.get("ontology_resolvable", 0)
        return strong / self.source_pattern_count


def _field(item: Any, name: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(name)
    return getattr(item, name, None)


def pattern_view(item: Any) -> PatternView | None:
    """Return a pattern view of a mapping or model, or None without a class and property."""
    subject = _field(item, "subject_class")
    prop = _field(item, "property_uri")
    if not isinstance(subject, str) or not isinstance(prop, str):
        return None
    return PatternView(
        subject_class=subject,
        property_uri=prop,
        object_class=_field(item, "object_class"),
        datatype=_field(item, "datatype"),
    )


def _term_pair_resolution(
    source: str,
    target: str,
    index: NamedOntologyIndex,
    *,
    kind: Literal["class", "property"],
) -> TermResolution:
    return resolve_term_against(source, [target], index, kind=kind)


def _object_resolution(
    source: PatternView,
    target: PatternView,
    index: NamedOntologyIndex,
) -> tuple[TermResolution | None, bool | None, bool]:
    """Return object relation, datatype match, and whether the component exists."""
    source_obj = source.object_class
    target_obj = target.object_class
    # Literal branches are compared by datatype, including None/unknown datatype.
    if source_obj == "Literal" or target_obj == "Literal" or source.datatype or target.datatype:
        if source_obj != target_obj:
            return None, False, True
        return None, source.datatype == target.datatype, True
    if source_obj is None and target_obj is None:
        return None, None, False
    if source_obj is None or target_obj is None:
        return None, None, True
    if source_obj in _SENTINELS or target_obj in _SENTINELS:
        relation = TermResolution(
            source_term=source_obj,
            kind="exact" if source_obj == target_obj else "unresolved",
            target_terms=[target_obj] if source_obj == target_obj else [],
        )
        return relation, None, True
    return _term_pair_resolution(source_obj, target_obj, index, kind="class"), None, True


def resolve_pattern_pair(
    source: PatternView,
    target: PatternView,
    index: NamedOntologyIndex,
) -> PatternResolution:
    """Classify one empirical pattern pair without collapsing relation evidence."""
    subject = _term_pair_resolution(source.subject_class, target.subject_class, index, kind="class")
    predicate = _term_pair_resolution(
        source.property_uri, target.property_uri, index, kind="property"
    )
    obj, datatype_match, has_object = _object_resolution(source, target, index)

    component_kinds = [subject.kind, predicate.kind]
    if has_object:
        if datatype_match is not None:
            component_kinds.append("exact" if datatype_match else "unresolved")
        elif obj is not None:
            component_kinds.append(obj.kind)
        else:
            component_kinds.append("unresolved")

    if all(kind == "exact" for kind in component_kinds):
        resolution: PatternResolutionKind = "exact"
    else:
        strong = {"exact", "equivalent", "source_subsumed_by_target", "target_subsumed_by_source"}
        if all(kind in strong for kind in component_kinds):
            resolution = "ontology_resolvable"
        elif (
            all(kind != "unresolved" for kind in component_kinds)
            and "shared_named_ancestor" in component_kinds
        ):
            resolution = "weak_shared_ancestor"
        elif any(kind != "unresolved" for kind in component_kinds):
            resolution = "partial"
        else:
            resolution = "unresolved"

    return PatternResolution(
        source=source,
        target=target,
        kind=resolution,
        subject_relation=subject,
        property_relation=predicate,
        object_relation=obj,
        datatype_match=datatype_match,
    )


def _rank(kind: PatternResolutionKind) -> int:
    return {
        "exact": 4,
        "ontology_resolvable": 3,
        "weak_shared_ancestor": 2,
        "partial": 1,
        "unresolved": 0,
    }[kind]


def compare_pattern_resolvability(
    source_patterns: Iterable[Any],
    target_patterns: Iterable[Any],
    ontology_graph: Graph,
    *,
    source_dataset: str,
    target_dataset: str,
    keep_all: bool = True,
) -> PatternResolvabilitySummary:
    """Find the strongest target pattern for every source empirical pattern.

    This O(n*m) implementation is intended for bounded paper-analysis tables and
    conformance fixtures. Large-corpus analysis should later index target terms
    by ontology relation neighborhoods without changing the output model.
    """
    source = [view for item in source_patterns if (view := pattern_view(item)) is not None]
    target = [view for item in target_patterns if (view := pattern_view(item)) is not None]
    index = NamedOntologyIndex(ontology_graph)
    rows: list[PatternResolution] = []
    counts: Counter[str] = Counter()
    for source_pattern in source:
        best: PatternResolution | None = None
        for target_pattern in target:
            candidate = resolve_pattern_pair(source_pattern, target_pattern, index)
            if best is None or _rank(candidate.kind) > _rank(best.kind):
                best = candidate
                if candidate.kind == "exact":
                    break
        if best is None:
            best = PatternResolution(source=source_pattern, kind="unresolved")
        counts[best.kind] += 1
        if keep_all:
            rows.append(best)
    return PatternResolvabilitySummary(
        source_dataset=source_dataset,
        target_dataset=target_dataset,
        source_pattern_count=len(source),
        by_kind=dict(sorted(counts.items())),
        patterns=rows,
    )


__all__ = [
    "PatternResolution",
    "PatternResolvabilitySummary",
    "PatternView",
    "compare_pattern_resolvability",
    "pattern_view",
    "resolve_pattern_pair",
]
