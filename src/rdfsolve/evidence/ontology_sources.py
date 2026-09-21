"""Resolve observed ontology-like namespaces to retrievable ontology candidates.

Only deterministic source rules live here. Unknown namespaces remain unresolved
until a provider graph, registry (Bioregistry/OLS), or curated source record
identifies an ontology artifact. No ontology usage is inferred merely from a
namespace candidate.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from typing import Literal

from pydantic import BaseModel, Field

from rdfsolve.evidence.ontology import ObservedOntologyTerms

_OBO_BASE = "http://purl.obolibrary.org/obo/"
_STANDARD_PREFIXES = (
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/2001/XMLSchema#",
    "http://www.w3.org/2002/07/owl#",
)
INFRASTRUCTURE_NAMESPACES = frozenset(
    {
        "http://purl.org/dc/terms/",
        "http://purl.org/dc/elements/1.1/",
        "http://www.w3.org/2004/02/skos/core#",
        "http://www.w3.org/ns/shacl#",
        "http://www.w3.org/ns/sparql-service-description#",
        "http://rdfs.org/ns/void#",
        "http://ldf.fi/void-ext#",
        "http://xmlns.com/foaf/0.1/",
        "http://purl.org/pav/",
        "https://schema.org/",
        "http://schema.org/",
        "http://www.openlinksw.com/schemas/virtrdf#",
        "http://www.w3.org/ns/dcat#",
        "http://www.w3.org/ns/prov#",
        "http://www.geneontology.org/formats/oboInOwl#",
    }
)
_OBO_LOCAL = re.compile(r"^(?P<prefix>[A-Za-z][A-Za-z0-9]*)_(?P<local>.+)$")


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


def term_namespace(iri: str) -> str | None:
    """Return a conservative operational namespace for an observed HTTP IRI."""
    if not iri.startswith(("http://", "https://")):
        return None
    if iri.startswith(_OBO_BASE):
        local = iri[len(_OBO_BASE) :]
        match = _OBO_LOCAL.match(local)
        if match:
            return f"{_OBO_BASE}{match.group('prefix').upper()}_"
        return _OBO_BASE
    if "#" in iri:
        return iri.rsplit("#", 1)[0] + "#"
    return iri.rsplit("/", 1)[0] + "/" if "/" in iri else iri


def _is_standard(iri: str) -> bool:
    return iri.startswith(_STANDARD_PREFIXES)


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
        if ns := term_namespace(term):
            classes[ns].add(term)
    for term in observed.properties:
        if _is_standard(term):
            continue
        if ns := term_namespace(term):
            properties[ns].add(term)

    resolved: list[OntologySourceCandidate] = []
    unresolved: list[UnresolvedOntologyNamespace] = []
    infrastructure: list[UnresolvedOntologyNamespace] = []
    for namespace in sorted(set(classes) | set(properties)):
        if namespace in INFRASTRUCTURE_NAMESPACES:
            infrastructure.append(
                UnresolvedOntologyNamespace(
                    namespace=namespace,
                    observed_classes=sorted(classes[namespace]),
                    observed_properties=sorted(properties[namespace]),
                )
            )
            continue
        if namespace in explicit_sources:
            ontology_id, source_url = explicit_sources[namespace]
            resolved.append(
                OntologySourceCandidate(
                    ontology_id=ontology_id,
                    namespace=namespace,
                    source_url=source_url,
                    resolution_basis="explicit",
                    observed_classes=sorted(classes[namespace]),
                    observed_properties=sorted(properties[namespace]),
                )
            )
            continue
        if namespace.startswith(_OBO_BASE) and namespace.endswith("_"):
            prefix = namespace[len(_OBO_BASE) : -1].lower()
            resolved.append(
                OntologySourceCandidate(
                    ontology_id=prefix,
                    namespace=namespace,
                    source_url=f"https://purl.obolibrary.org/obo/{prefix}.owl",
                    resolution_basis="obo_purl_namespace",
                    observed_classes=sorted(classes[namespace]),
                    observed_properties=sorted(properties[namespace]),
                )
            )
            continue
        unresolved.append(
            UnresolvedOntologyNamespace(
                namespace=namespace,
                observed_classes=sorted(classes[namespace]),
                observed_properties=sorted(properties[namespace]),
            )
        )
    return OntologySourceResolution(
        resolved=resolved, unresolved=unresolved, infrastructure=infrastructure
    )


__all__ = [
    "INFRASTRUCTURE_NAMESPACES",
    "OntologySourceCandidate",
    "OntologySourceResolution",
    "UnresolvedOntologyNamespace",
    "resolve_ontology_sources",
    "term_namespace",
]
