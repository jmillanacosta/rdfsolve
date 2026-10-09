"""How a data graph uses ontology terms, assessed against an ontology artifact.

Discovery alone never establishes ontology usage (see rdfsolve.ontology.discovery). Usage is
attributed only through overlap with observed class and property terms.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field
from rdflib import OWL, RDF, RDFS, Graph, URIRef

from rdfsolve.ontology.vocabulary import CLASS_TYPES, PROPERTY_TYPES

_CLASS_KINDS: tuple[URIRef, ...] = tuple(URIRef(t) for t in CLASS_TYPES)
_PROPERTY_KINDS: tuple[URIRef, ...] = tuple(URIRef(t) for t in PROPERTY_TYPES)


class ObservedOntologyTerms(BaseModel):
    """Observed class/property terms extracted from empirical schema patterns."""

    classes: set[str] = Field(default_factory=set)
    properties: set[str] = Field(default_factory=set)


def _field(item: Any, name: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(name)
    return getattr(item, name, None)


def observed_terms_from_patterns(patterns: Iterable[Any]) -> ObservedOntologyTerms:
    """Collect empirical class/property IRIs without importing schema-model code.

    The loose input contract is intentional: release analysis can operate on
    serialized pattern dicts without importing optional conversion dependencies.
    """
    classes: set[str] = set()
    properties: set[str] = set()
    for pattern in patterns:
        # Subjects without a type are no use of the class rdfs:Resource.
        subject = (
            None
            if _field(pattern, "subject_binding") == "untyped"
            else _field(pattern, "subject_class")
        )
        obj = _field(pattern, "object_class")
        prop = _field(pattern, "property_uri")
        if isinstance(subject, str) and subject.startswith(("http://", "https://")):
            classes.add(subject)
        if isinstance(obj, str) and obj.startswith(("http://", "https://")):
            classes.add(obj)
        if isinstance(prop, str) and prop.startswith(("http://", "https://")):
            properties.add(prop)
    return ObservedOntologyTerms(classes=classes, properties=properties)


class OntologyTermUsage(BaseModel):
    """How one ontology term is declared and realized by an empirical KG."""

    term_iri: str
    declared_roles: list[str] = Field(default_factory=list)
    observed_roles: list[str] = Field(default_factory=list)

    @property
    def role_divergence(self) -> bool:
        """Whether a used RDF role falls outside the ontology declarations."""
        declared = set(self.declared_roles)
        observed = set(self.observed_roles)
        if not declared or not observed:
            return False
        compatible = {
            "class": {"class"},
            "rdf_property": {"predicate"},
            "object_property": {"predicate"},
            "datatype_property": {"predicate"},
            "annotation_property": {"predicate"},
        }
        allowed = set().union(*(compatible.get(role, set()) for role in declared))
        return bool(observed - allowed)


class OntologyUsage(BaseModel):
    """Dataset-specific resolution of empirically used terms against an ontology artifact.

    ``artifact_relation`` distinguishes an externally retrieved reference release
    from provider-declared ontology material.  A reference artifact is useful for
    semantic resolvability even when the exact provider-used release is unknown.
    """

    dataset_id: str
    ontology_artifact_id: str
    ontology_id: str | None = None
    namespace: str | None = None
    artifact_relation: Literal[
        "reference_release",
        "provider_release",
        "provider_graph",
        "local_distribution_artifact",
    ] = "reference_release"
    version_match_status: Literal["matched", "mismatched", "unknown"] = "unknown"
    resolved_classes: list[str] = Field(default_factory=list)
    unresolved_classes: list[str] = Field(default_factory=list)
    resolved_properties: list[str] = Field(default_factory=list)
    unresolved_properties: list[str] = Field(default_factory=list)
    term_usage: list[OntologyTermUsage] = Field(default_factory=list)

    @property
    def class_resolution_fraction(self) -> float | None:
        """Return the share of observed classes the ontology declares, or None without classes."""
        total = len(self.resolved_classes) + len(self.unresolved_classes)
        return len(self.resolved_classes) / total if total else None

    @property
    def property_resolution_fraction(self) -> float | None:
        """Return the share of observed properties the ontology declares, or None without any."""
        total = len(self.resolved_properties) + len(self.unresolved_properties)
        return len(self.resolved_properties) / total if total else None


class UsageOverlap(BaseModel):
    """Pairwise overlap of two operational ontology usages."""

    left_dataset: str
    right_dataset: str
    kind: Literal["class", "property"]
    left_terms: int
    right_terms: int
    intersection: int
    union: int
    jaccard: float | None
    left_containment: float | None
    right_containment: float | None


def _declared_role_map(graph: Graph) -> dict[str, set[str]]:
    roles: dict[str, set[str]] = {}

    def add(term: Any, role: str) -> None:
        """Record a role for a named term."""
        if isinstance(term, URIRef):
            roles.setdefault(str(term), set()).add(role)

    for term in graph.subjects(RDF.type, OWL.Class):
        add(term, "class")
    for term in graph.subjects(RDF.type, RDFS.Class):
        add(term, "class")
    for term in graph.subjects(RDF.type, RDF.Property):
        add(term, "rdf_property")
    for term in graph.subjects(RDF.type, OWL.ObjectProperty):
        add(term, "object_property")
    for term in graph.subjects(RDF.type, OWL.DatatypeProperty):
        add(term, "datatype_property")
    for term in graph.subjects(RDF.type, OWL.AnnotationProperty):
        add(term, "annotation_property")

    # Include named terms participating in common OWL/RDFS axioms even where an
    # explicit declaration triple is absent.  The role remains conservative.
    for child, parent in graph.subject_objects(RDFS.subClassOf):
        add(child, "class")
        if isinstance(parent, URIRef):
            add(parent, "class")
    for left, right in graph.subject_objects(OWL.equivalentClass):
        add(left, "class")
        if isinstance(right, URIRef):
            add(right, "class")
    for left, right in graph.subject_objects(OWL.disjointWith):
        add(left, "class")
        if isinstance(right, URIRef):
            add(right, "class")
    for child, parent in graph.subject_objects(RDFS.subPropertyOf):
        add(child, "rdf_property")
        if isinstance(parent, URIRef):
            add(parent, "rdf_property")
    for predicate in (RDFS.domain, RDFS.range):
        for prop, cls in graph.subject_objects(predicate):
            add(prop, "rdf_property")
            if isinstance(cls, URIRef):
                add(cls, "class")
    for left, right in graph.subject_objects(OWL.equivalentProperty):
        add(left, "rdf_property")
        if isinstance(right, URIRef):
            add(right, "rdf_property")
    for left, right in graph.subject_objects(OWL.inverseOf):
        add(left, "object_property")
        if isinstance(right, URIRef):
            add(right, "object_property")
    return roles


def _observed_role_map(patterns: Iterable[Any]) -> dict[str, set[str]]:
    roles: dict[str, set[str]] = {}

    def add(term: Any, role: str) -> None:
        """Record a role for an HTTP IRI."""
        if isinstance(term, str) and term.startswith(("http://", "https://")):
            roles.setdefault(term, set()).add(role)

    for pattern in patterns:
        if _field(pattern, "subject_binding") != "untyped":
            add(_field(pattern, "subject_class"), "class")
        add(_field(pattern, "object_class"), "class")
        add(_field(pattern, "property_uri"), "predicate")
    return roles


def assess_ontology_usage(
    patterns: Iterable[Any],
    ontology_graph: Graph,
    *,
    dataset_id: str,
    ontology_artifact_id: str,
    expected_classes: Iterable[str] | None = None,
    expected_properties: Iterable[str] | None = None,
) -> OntologyUsage:
    """Compare empirical use with one ontology artifact signature.

    ``expected_classes`` and ``expected_properties`` scope the denominator to
    terms attributed to this ontology candidate.  Without this filter, terms
    from unrelated namespaces in the same KG would be incorrectly counted as
    unresolved for every ontology artifact.
    """
    pattern_list = list(patterns)
    observed = observed_terms_from_patterns(pattern_list)
    if expected_classes is not None:
        observed.classes.intersection_update(set(expected_classes))
    if expected_properties is not None:
        observed.properties.intersection_update(set(expected_properties))
    declared_roles = _declared_role_map(ontology_graph)
    signature = set(declared_roles)
    resolved_classes = sorted(observed.classes & signature)
    resolved_properties = sorted(observed.properties & signature)
    unresolved_classes = sorted(observed.classes - signature)
    unresolved_properties = sorted(observed.properties - signature)
    observed_roles = _observed_role_map(pattern_list)
    usage = []
    for term in sorted((observed.classes | observed.properties) & signature):
        usage.append(
            OntologyTermUsage(
                term_iri=term,
                declared_roles=sorted(declared_roles.get(term, set())),
                observed_roles=sorted(observed_roles.get(term, set())),
            )
        )
    return OntologyUsage(
        dataset_id=dataset_id,
        ontology_artifact_id=ontology_artifact_id,
        resolved_classes=resolved_classes,
        unresolved_classes=unresolved_classes,
        resolved_properties=resolved_properties,
        unresolved_properties=unresolved_properties,
        term_usage=usage,
    )


def ontology_usage_slice(
    ontology_graph: Graph,
    used_terms: Iterable[str],
) -> Graph:
    """Derive a named-axiom usage slice around empirically used ontology terms.

    This is deliberately called a *usage slice*, not an OWL module.  It retains
    used terms, named superclass/superproperty ancestry, and directly relevant
    named equivalence/inverse/domain/range/disjointness assertions.  Complex
    class expressions remain in the full archived ontology artifact.
    """
    used = {URIRef(term) for term in used_terms if term.startswith(("http://", "https://"))}
    selected = set(used)

    # Add named ancestors to situate the operational terms in the ontology.
    pending = list(used)
    while pending:
        term = pending.pop()
        for predicate in (RDFS.subClassOf, RDFS.subPropertyOf):
            for parent in ontology_graph.objects(term, predicate):
                if isinstance(parent, URIRef) and parent not in selected:
                    selected.add(parent)
                    pending.append(parent)

    annotation_predicates = {
        RDFS.label,
        RDFS.comment,
        URIRef("http://www.w3.org/2004/02/skos/core#prefLabel"),
        URIRef("http://purl.obolibrary.org/obo/IAO_0000115"),
        OWL.deprecated,
    }
    structural_predicates = {
        RDF.type,
        RDFS.subClassOf,
        RDFS.subPropertyOf,
        RDFS.domain,
        RDFS.range,
        OWL.equivalentClass,
        OWL.equivalentProperty,
        OWL.inverseOf,
        OWL.disjointWith,
    }
    relation_predicates = structural_predicates - {RDF.type}

    # Add named terms directly referenced by selected terms before serializing,
    # so their declarations/labels are retained deterministically regardless of
    # RDF graph iteration order.
    referenced = set(selected)
    for term in list(selected):
        for predicate in relation_predicates:
            for obj in ontology_graph.objects(term, predicate):
                if isinstance(obj, URIRef):
                    referenced.add(obj)
    selected = referenced

    out = Graph()
    for prefix, namespace in ontology_graph.namespaces():
        out.bind(prefix, namespace)
    for subject, triple_predicate, value in ontology_graph:
        if subject in selected and (
            triple_predicate in annotation_predicates
            or triple_predicate in structural_predicates
            or (triple_predicate == RDF.type and value in _CLASS_KINDS + _PROPERTY_KINDS)
        ):
            out.add((subject, triple_predicate, value))
    return out


def compare_ontology_usage(
    left: OntologyUsage,
    right: OntologyUsage,
    *,
    kind: Literal["class", "property"],
) -> UsageOverlap:
    """Compare exact operational-term overlap for the same ontology artifact."""
    if left.ontology_artifact_id != right.ontology_artifact_id:
        raise ValueError("Ontology usages must reference the same ontology artifact")
    left_terms = set(left.resolved_classes if kind == "class" else left.resolved_properties)
    right_terms = set(right.resolved_classes if kind == "class" else right.resolved_properties)
    intersection = len(left_terms & right_terms)
    union = len(left_terms | right_terms)
    return UsageOverlap(
        left_dataset=left.dataset_id,
        right_dataset=right.dataset_id,
        kind=kind,
        left_terms=len(left_terms),
        right_terms=len(right_terms),
        intersection=intersection,
        union=union,
        jaccard=intersection / union if union else None,
        left_containment=intersection / len(left_terms) if left_terms else None,
        right_containment=intersection / len(right_terms) if right_terms else None,
    )
