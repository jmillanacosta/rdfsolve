"""Ontology-mediated resolvability of operational KG vocabularies.

Only named OWL/RDFS relations are used here. This module deliberately does not
claim complete OWL entailment; owlapy-backed reasoning can later supply a richer
semantic index over the same versioned ontology artifacts.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable
from typing import Literal

from pydantic import BaseModel, Field
from rdflib import OWL, RDF, RDFS, Graph, URIRef

from rdfsolve.evidence.ontology import OntologyUsage

ResolutionKind = Literal[
    "exact",
    "equivalent",
    "source_subsumed_by_target",
    "target_subsumed_by_source",
    "shared_named_ancestor",
    "unresolved",
]


class TermResolution(BaseModel):
    """How one source term resolves against a target ontology."""

    source_term: str
    kind: ResolutionKind
    target_terms: list[str] = Field(default_factory=list)
    witness_terms: list[str] = Field(default_factory=list)


class UsageResolvability(BaseModel):
    """Resolution of one dataset's ontology terms against another's."""

    source_dataset: str
    target_dataset: str
    ontology_artifact_id: str
    term_kind: Literal["class", "property"]
    source_term_count: int
    resolved_term_count: int
    by_kind: dict[str, int]
    terms: list[TermResolution]

    @property
    def resolved_fraction(self) -> float | None:
        """Return the share of source terms that resolve, or None when there are none."""
        return self.resolved_term_count / self.source_term_count if self.source_term_count else None


def _named_edges(graph: Graph, predicate: URIRef) -> dict[str, set[str]]:
    edges: dict[str, set[str]] = defaultdict(set)
    for child, parent in graph.subject_objects(predicate):
        if isinstance(child, URIRef) and isinstance(parent, URIRef):
            edges[str(child)].add(str(parent))
    return edges


def _symmetric_edges(graph: Graph, predicate: URIRef) -> dict[str, set[str]]:
    edges: dict[str, set[str]] = defaultdict(set)
    for left, right in graph.subject_objects(predicate):
        if isinstance(left, URIRef) and isinstance(right, URIRef):
            edges[str(left)].add(str(right))
            edges[str(right)].add(str(left))
    return edges


def _closure(start: str, edges: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    queue = deque([start])
    while queue:
        current = queue.popleft()
        for nxt in edges.get(current, ()):
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
    return seen


class NamedOntologyIndex:
    """Named-term semantic index for cheap, deterministic release analyses."""

    def __init__(self, graph: Graph):
        """Index the named class and property hierarchies of an ontology graph."""
        self.graph = graph
        self.class_parents = _named_edges(graph, RDFS.subClassOf)
        self.property_parents = _named_edges(graph, RDFS.subPropertyOf)
        self.class_equivalents = _symmetric_edges(graph, OWL.equivalentClass)
        self.property_equivalents = _symmetric_edges(graph, OWL.equivalentProperty)
        self.classes = (
            self._signature((OWL.Class, RDFS.Class))
            | set(self.class_parents)
            | {term for values in self.class_parents.values() for term in values}
        )
        self.properties = (
            self._signature(
                (RDF.Property, OWL.ObjectProperty, OWL.DatatypeProperty, OWL.AnnotationProperty)
            )
            | set(self.property_parents)
            | {term for values in self.property_parents.values() for term in values}
        )

    def _signature(self, kinds: tuple[URIRef, ...]) -> set[str]:
        return {
            str(subject)
            for kind in kinds
            for subject in self.graph.subjects(RDF.type, kind)
            if isinstance(subject, URIRef)
        }

    def equivalents(self, term: str, *, kind: Literal["class", "property"]) -> set[str]:
        """Return the terms equivalent to a term, transitively."""
        return _closure(
            term, self.class_equivalents if kind == "class" else self.property_equivalents
        )

    def ancestors(self, term: str, *, kind: Literal["class", "property"]) -> set[str]:
        """Return the named ancestors of a term, transitively."""
        return _closure(term, self.class_parents if kind == "class" else self.property_parents)


def resolve_term_against(
    source_term: str,
    target_terms: Iterable[str],
    index: NamedOntologyIndex,
    *,
    kind: Literal["class", "property"],
) -> TermResolution:
    """Return the strongest named-term relation to the target operational vocabulary."""
    target = set(target_terms)
    if source_term in target:
        return TermResolution(source_term=source_term, kind="exact", target_terms=[source_term])

    equivalents = index.equivalents(source_term, kind=kind)
    hits = sorted(equivalents & target)
    if hits:
        return TermResolution(source_term=source_term, kind="equivalent", target_terms=hits)

    source_ancestors = index.ancestors(source_term, kind=kind)
    hits = sorted(source_ancestors & target)
    if hits:
        return TermResolution(
            source_term=source_term,
            kind="source_subsumed_by_target",
            target_terms=hits,
        )

    target_descendant_hits: list[str] = []
    for candidate in target:
        if source_term in index.ancestors(candidate, kind=kind):
            target_descendant_hits.append(candidate)
    if target_descendant_hits:
        return TermResolution(
            source_term=source_term,
            kind="target_subsumed_by_source",
            target_terms=sorted(target_descendant_hits),
        )

    # Shared ancestry is deliberately weakest and excludes top-level universal
    # resources, which otherwise make nearly every pair look related.
    excluded = {str(OWL.Thing), str(RDFS.Resource)}
    best_targets: list[str] = []
    witnesses: set[str] = set()
    for candidate in target:
        common = (source_ancestors & index.ancestors(candidate, kind=kind)) - excluded
        if common:
            best_targets.append(candidate)
            witnesses.update(common)
    if best_targets:
        return TermResolution(
            source_term=source_term,
            kind="shared_named_ancestor",
            target_terms=sorted(best_targets),
            witness_terms=sorted(witnesses),
        )

    return TermResolution(source_term=source_term, kind="unresolved")


def compare_usage_resolvability(
    source: OntologyUsage,
    target: OntologyUsage,
    ontology_graph: Graph,
    *,
    kind: Literal["class", "property"],
    include_shared_ancestor_as_resolved: bool = False,
) -> UsageResolvability:
    """Measure asymmetric ontology-mediated resolvability without collapsing relation types."""
    if source.ontology_artifact_id != target.ontology_artifact_id:
        raise ValueError("Ontology usages must reference the same ontology artifact")
    source_terms = source.resolved_classes if kind == "class" else source.resolved_properties
    target_terms = target.resolved_classes if kind == "class" else target.resolved_properties
    index = NamedOntologyIndex(ontology_graph)
    terms = [
        resolve_term_against(term, target_terms, index, kind=kind) for term in sorted(source_terms)
    ]
    by_kind: dict[str, int] = defaultdict(int)
    for term in terms:
        by_kind[term.kind] += 1
    resolved_kinds = {
        "exact",
        "equivalent",
        "source_subsumed_by_target",
        "target_subsumed_by_source",
    }
    if include_shared_ancestor_as_resolved:
        resolved_kinds.add("shared_named_ancestor")
    resolved = sum(count for relation, count in by_kind.items() if relation in resolved_kinds)
    return UsageResolvability(
        source_dataset=source.dataset_id,
        target_dataset=target.dataset_id,
        ontology_artifact_id=source.ontology_artifact_id,
        term_kind=kind,
        source_term_count=len(source_terms),
        resolved_term_count=resolved,
        by_kind=dict(sorted(by_kind.items())),
        terms=terms,
    )


__all__ = [
    "NamedOntologyIndex",
    "TermResolution",
    "UsageResolvability",
    "compare_usage_resolvability",
    "resolve_term_against",
]
