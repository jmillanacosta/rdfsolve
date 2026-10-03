"""Which ontology each namespace of a data graph comes from, and the plan to acquire it.

The acquisition plan keeps three facts apart: an empirical namespace is used by a dataset; a
provider graph declares ontology material for it; a reference artifact can be retrieved.

Resolve observed ontology-like namespaces to retrievable ontology candidates.

Only deterministic source rules live here. Unknown namespaces remain unresolved
until a provider graph, registry (Bioregistry/OLS), or curated source record
identifies an ontology artifact. No ontology usage is inferred merely from a
namespace candidate.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Literal

from pydantic import BaseModel, Field

from rdfsolve.ontology.artifacts import LocalOntologyFileCandidate
from rdfsolve.ontology.discovery import OntologyGraphCandidate, OntologyVersionEvidence
from rdfsolve.ontology.terms import namespace
from rdfsolve.ontology.usage import ObservedOntologyTerms
from rdfsolve.ontology.vocabulary import INFRASTRUCTURE_NAMESPACES, OBO, STANDARD_NAMESPACES


class OntologySourceCandidate(BaseModel):
    """An ontology source resolved from an observed namespace."""

    ontology_id: str
    namespace: str
    source_url: str
    resolution_basis: Literal["obo_purl_namespace", "explicit"]
    observed_classes: list[str] = Field(default_factory=list)
    observed_properties: list[str] = Field(default_factory=list)


class UnresolvedOntologyNamespace(BaseModel):
    """An observed namespace without a resolved ontology source."""

    namespace: str
    observed_classes: list[str] = Field(default_factory=list)
    observed_properties: list[str] = Field(default_factory=list)


class OntologySourceResolution(BaseModel):
    """Resolved, unresolved and infrastructure namespaces of one dataset."""

    resolved: list[OntologySourceCandidate] = Field(default_factory=list)
    unresolved: list[UnresolvedOntologyNamespace] = Field(default_factory=list)
    infrastructure: list[UnresolvedOntologyNamespace] = Field(default_factory=list)


def _observed_namespace(iri: str) -> str | None:
    """Return the namespace of an observed HTTP IRI; other IRIs are not ontology terms here."""
    return namespace(iri) if iri.startswith(("http://", "https://")) else None


def _is_standard(iri: str) -> bool:
    return iri.startswith(STANDARD_NAMESPACES)


def resolve_ontology_sources(
    observed: ObservedOntologyTerms,
    *,
    explicit_sources: dict[str, tuple[str, str]] | None = None,
) -> OntologySourceResolution:
    """Resolve deterministic ontology source candidates from empirical terms.

    ``explicit_sources`` maps an operational namespace to ``(ontology_id,
    source_url)`` and is intended for provider/registry evidence. OBO term IRIs
    are resolved by the stable OBO PURL convention. All other namespaces remain
    unresolved rather than receiving a guessed download URL.
    """
    explicit_sources = explicit_sources or {}
    classes: dict[str, set[str]] = defaultdict(set)
    properties: dict[str, set[str]] = defaultdict(set)
    for term in observed.classes:
        if _is_standard(term):
            continue
        if ns := _observed_namespace(term):
            classes[ns].add(term)
    for term in observed.properties:
        if _is_standard(term):
            continue
        if ns := _observed_namespace(term):
            properties[ns].add(term)

    resolved: list[OntologySourceCandidate] = []
    unresolved: list[UnresolvedOntologyNamespace] = []
    infrastructure: list[UnresolvedOntologyNamespace] = []
    for space in sorted(set(classes) | set(properties)):
        if space in INFRASTRUCTURE_NAMESPACES:
            infrastructure.append(
                UnresolvedOntologyNamespace(
                    namespace=space,
                    observed_classes=sorted(classes[space]),
                    observed_properties=sorted(properties[space]),
                )
            )
            continue
        if space in explicit_sources:
            ontology_id, source_url = explicit_sources[space]
            resolved.append(
                OntologySourceCandidate(
                    ontology_id=ontology_id,
                    namespace=space,
                    source_url=source_url,
                    resolution_basis="explicit",
                    observed_classes=sorted(classes[space]),
                    observed_properties=sorted(properties[space]),
                )
            )
            continue
        if space.startswith(OBO) and space.endswith("_"):
            prefix = space[len(OBO) : -1].lower()
            resolved.append(
                OntologySourceCandidate(
                    ontology_id=prefix,
                    namespace=space,
                    source_url=f"https://purl.obolibrary.org/obo/{prefix}.owl",
                    resolution_basis="obo_purl_namespace",
                    observed_classes=sorted(classes[space]),
                    observed_properties=sorted(properties[space]),
                )
            )
            continue
        unresolved.append(
            UnresolvedOntologyNamespace(
                namespace=space,
                observed_classes=sorted(classes[space]),
                observed_properties=sorted(properties[space]),
            )
        )
    return OntologySourceResolution(
        resolved=resolved, unresolved=unresolved, infrastructure=infrastructure
    )


IdentityBasis = Literal[
    "reference_source",
    "declared_ontology_iri",
    "discovered_graph",
    "unresolved",
]


class OntologyGraphEvidenceRef(BaseModel):
    """Ontology-bearing graph whose declarations overlap empirical terms."""

    access_context: Literal[
        "remote_endpoint", "local_distribution", "grouped_local_distribution", "unknown"
    ] = "unknown"
    endpoint_url: str | None = None
    graph_uri: str
    explicit_ontology_iris: list[str] = Field(default_factory=list)
    version_evidence: list[OntologyVersionEvidence] = Field(default_factory=list)
    observed_classes: list[str] = Field(default_factory=list)
    observed_properties: list[str] = Field(default_factory=list)
    discovery_status: str = "matched"


class OntologyUsageCandidate(BaseModel):
    """Potential ontology identity for one empirically used namespace.

    ``graph_evidence`` records ontology-bearing graphs discovered in the
    relevant access context.  ``reference_sources`` records separately retrievable
    reference ontology candidates.  Local OWL-formatted distribution files are
    stored separately on the acquisition plan until their content is inspected.
    """

    namespace: str
    ontology_id: str | None = None
    observed_classes: list[str] = Field(default_factory=list)
    observed_properties: list[str] = Field(default_factory=list)
    graph_evidence: list[OntologyGraphEvidenceRef] = Field(default_factory=list)
    reference_sources: list[OntologySourceCandidate] = Field(default_factory=list)
    identity_basis: IdentityBasis = "unresolved"

    @property
    def has_declared_graph_evidence(self) -> bool:
        """Return whether the dataset declares ontology graphs."""
        return bool(self.graph_evidence)

    @property
    def has_reference_source(self) -> bool:
        """Return whether a reference source was found for the ontology."""
        return bool(self.reference_sources)


class OntologyAcquisitionPlan(BaseModel):
    """Dataset-snapshot ontology evidence/acquisition plan.

    ``mining_context`` is intentionally explicit.  A remote endpoint and a
    locally downloaded distribution provide different evidence about ontology
    availability and must not be conflated.
    """

    dataset_id: str
    mining_context: Literal[
        "remote_endpoint",
        "local_distribution",
        "grouped_local_distribution",
        "unknown",
    ] = "unknown"
    candidates: list[OntologyUsageCandidate] = Field(default_factory=list)
    local_ontology_file_candidates: list[LocalOntologyFileCandidate] = Field(default_factory=list)
    infrastructure_namespaces: list[str] = Field(default_factory=list)


def _stable_graph_identity(candidate: OntologyGraphCandidate) -> tuple[str, IdentityBasis]:
    if candidate.explicit_ontology_iris:
        return candidate.explicit_ontology_iris[0], "declared_ontology_iri"
    digest = hashlib.sha256(candidate.graph_uri.encode()).hexdigest()[:16]
    return f"discovered-graph:{digest}", "discovered_graph"


def build_ontology_acquisition_plan(
    dataset_id: str,
    observed: ObservedOntologyTerms,
    *,
    graph_candidates: Sequence[OntologyGraphCandidate] = (),
    endpoint_url: str | None = None,
    explicit_sources: dict[str, tuple[str, str]] | None = None,
    local_ontology_file_candidates: list[LocalOntologyFileCandidate] | None = None,
    mining_context: Literal[
        "remote_endpoint",
        "local_distribution",
        "grouped_local_distribution",
        "unknown",
    ] = "unknown",
) -> OntologyAcquisitionPlan:
    """Combine empirical namespaces, provider graph evidence and reference sources.

    Only empirical namespaces produce usage candidates.  Ontology-looking provider
    graphs with no empirical overlap remain discovery artifacts and are deliberately
    absent from this usage plan.
    """
    class_terms: dict[str, set[str]] = defaultdict(set)
    prop_terms: dict[str, set[str]] = defaultdict(set)
    infrastructure: set[str] = set()

    for term in observed.classes:
        namespace = _observed_namespace(term)
        if not namespace:
            continue
        if namespace in INFRASTRUCTURE_NAMESPACES:
            infrastructure.add(namespace)
            continue
        class_terms[namespace].add(term)
    for term in observed.properties:
        namespace = _observed_namespace(term)
        if not namespace:
            continue
        if namespace in INFRASTRUCTURE_NAMESPACES:
            infrastructure.add(namespace)
            continue
        prop_terms[namespace].add(term)

    reference = resolve_ontology_sources(observed, explicit_sources=explicit_sources)
    references_by_ns: dict[str, list[OntologySourceCandidate]] = defaultdict(list)
    for item in reference.resolved:
        references_by_ns[item.namespace].append(item)
    infrastructure.update(item.namespace for item in reference.infrastructure)

    graph_evidence_by_ns: dict[str, list[OntologyGraphEvidenceRef]] = defaultdict(list)
    graph_identity_by_ns: dict[str, tuple[str, IdentityBasis]] = {}
    for graph in graph_candidates:
        # Usage attribution requires empirical overlap, not ontology discovery alone.
        if not graph.used_by_schema:
            continue
        overlap_classes: dict[str, list[str]] = defaultdict(list)
        overlap_props: dict[str, list[str]] = defaultdict(list)
        for term in graph.observed_class_overlap:
            if ns := _observed_namespace(term):
                overlap_classes[ns].append(term)
        for term in graph.observed_property_overlap:
            if ns := _observed_namespace(term):
                overlap_props[ns].append(term)
        ontology_id, basis = _stable_graph_identity(graph)
        for namespace in sorted(set(overlap_classes) | set(overlap_props)):
            if namespace in INFRASTRUCTURE_NAMESPACES:
                infrastructure.add(namespace)
                continue
            graph_evidence_by_ns[namespace].append(
                OntologyGraphEvidenceRef(
                    access_context=mining_context,
                    endpoint_url=endpoint_url,
                    graph_uri=graph.graph_uri,
                    explicit_ontology_iris=graph.explicit_ontology_iris,
                    version_evidence=graph.version_evidence,
                    observed_classes=sorted(overlap_classes[namespace]),
                    observed_properties=sorted(overlap_props[namespace]),
                    discovery_status=graph.discovery_status,
                )
            )
            graph_identity_by_ns.setdefault(namespace, (ontology_id, basis))

    candidates: list[OntologyUsageCandidate] = []
    for namespace in sorted(set(class_terms) | set(prop_terms)):
        references = references_by_ns.get(namespace, [])
        graph_evidence = graph_evidence_by_ns.get(namespace, [])
        usage_id: str | None
        if references:
            usage_id = references[0].ontology_id
            basis = "reference_source"
        elif namespace in graph_identity_by_ns:
            usage_id, basis = graph_identity_by_ns[namespace]
        else:
            usage_id, basis = None, "unresolved"
        candidates.append(
            OntologyUsageCandidate(
                namespace=namespace,
                ontology_id=usage_id,
                observed_classes=sorted(class_terms[namespace]),
                observed_properties=sorted(prop_terms[namespace]),
                graph_evidence=graph_evidence,
                reference_sources=references,
                identity_basis=basis,
            )
        )

    local_candidates: list[LocalOntologyFileCandidate] = []
    seen_local: set[tuple[str, str | None]] = set()
    for local in local_ontology_file_candidates or []:
        key = (local.source_url, local.source_dataset_id)
        if not local.source_url or key in seen_local:
            continue
        seen_local.add(key)
        local_candidates.append(local)

    if local_candidates and mining_context == "remote_endpoint":
        raise ValueError(
            "Local distribution ontology-file candidates cannot be attached to a remote endpoint run"
        )

    return OntologyAcquisitionPlan(
        dataset_id=dataset_id,
        mining_context=mining_context,
        candidates=candidates,
        local_ontology_file_candidates=local_candidates,
        infrastructure_namespaces=sorted(infrastructure),
    )


__all__ = [
    "INFRASTRUCTURE_NAMESPACES",
    "OntologyAcquisitionPlan",
    "OntologyGraphEvidenceRef",
    "OntologySourceCandidate",
    "OntologySourceResolution",
    "OntologyUsageCandidate",
    "UnresolvedOntologyNamespace",
    "build_ontology_acquisition_plan",
    "resolve_ontology_sources",
]
