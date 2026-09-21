"""Ontology discovery and usage evidence for mined RDF datasets.

This module deliberately separates two questions:

1. Which named graphs/artifacts look like ontology material, and what version
   evidence do they publish?
2. Which of those ontology terms are actually used by an empirical schema?

Discovery alone never establishes ontology usage. Usage is attributed only
through overlap with observed class/property terms (or, in later stages,
observed value terms).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field
from rdflib import OWL, RDF, RDFS, Dataset, Graph, Namespace, URIRef

DCTERMS = Namespace("http://purl.org/dc/terms/")
PAV = Namespace("http://purl.org/pav/")
SCHEMA = Namespace("https://schema.org/")

VERSION_PREDICATES: tuple[URIRef, ...] = (
    OWL.versionIRI,
    OWL.versionInfo,
    URIRef("http://www.w3.org/ns/dcat#version"),
    PAV.version,
    DCTERMS.issued,
    DCTERMS.modified,
    SCHEMA.version,
)

_CLASS_KINDS: tuple[URIRef, ...] = (OWL.Class, RDFS.Class)
_PROPERTY_KINDS: tuple[URIRef, ...] = (
    RDF.Property,
    OWL.ObjectProperty,
    OWL.DatatypeProperty,
    OWL.AnnotationProperty,
)


class OntologyVersionEvidence(BaseModel):
    """A version statement associated with ontology or graph discovery.

    Only ``scope="ontology"`` or ``scope="graph"`` should be used to identify
    an ontology artifact version automatically. ``other`` is retained as context.
    """

    subject: str
    predicate: str
    value: str
    scope: str


class OntologyGraphCandidate(BaseModel):
    """Evidence that one RDF graph contains ontology material.

    ``used_by_schema`` is deliberately derived only from observed-term overlap.
    A graph can be an ontology candidate without being used by the dataset.
    """

    graph_uri: str
    explicit_ontology_iris: list[str] = Field(default_factory=list)
    imports: list[str] = Field(default_factory=list)
    version_evidence: list[OntologyVersionEvidence] = Field(default_factory=list)

    declared_classes: int | None = None
    declared_properties: int | None = None
    candidate_reasons: list[str] = Field(default_factory=list)
    discovery_status: Literal["matched", "hint_only", "timeout", "error"] = "matched"
    discovery_error: str | None = None
    observed_at: str | None = None
    query_ids: list[str] = Field(default_factory=list)

    observed_class_overlap: list[str] = Field(default_factory=list)
    observed_property_overlap: list[str] = Field(default_factory=list)

    @property
    def used_by_schema(self) -> bool:
        """Whether empirical class/property use overlaps this graph."""
        return bool(self.observed_class_overlap or self.observed_property_overlap)


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
        subject = _field(pattern, "subject_class")
        obj = _field(pattern, "object_class")
        prop = _field(pattern, "property_uri")
        if isinstance(subject, str) and subject.startswith(("http://", "https://")):
            classes.add(subject)
        if isinstance(obj, str) and obj.startswith(("http://", "https://")):
            classes.add(obj)
        if isinstance(prop, str) and prop.startswith(("http://", "https://")):
            properties.add(prop)
    return ObservedOntologyTerms(classes=classes, properties=properties)


def _declared_terms(graph: Graph, kinds: tuple[URIRef, ...]) -> set[str]:
    terms: set[str] = set()
    for kind in kinds:
        terms.update(
            str(subject)
            for subject in graph.subjects(RDF.type, kind)
            if isinstance(subject, URIRef)
        )
    return terms


def _version_evidence(
    graph: Graph, graph_uri: str, ontology_iris: set[str]
) -> list[OntologyVersionEvidence]:
    rows: set[tuple[str, str, str, str]] = set()
    for predicate in VERSION_PREDICATES:
        for subject, value in graph.subject_objects(predicate):
            if not isinstance(subject, URIRef):
                continue
            subject_s = str(subject)
            scope = (
                "ontology"
                if subject_s in ontology_iris
                else "graph"
                if subject_s == graph_uri
                else "other"
            )
            rows.add((subject_s, str(predicate), str(value), scope))
    return [
        OntologyVersionEvidence(subject=s, predicate=p, value=v, scope=scope)
        for s, p, v, scope in sorted(rows)
    ]


def inspect_ontology_graph(
    graph: Graph,
    graph_uri: str,
    *,
    observed_classes: Iterable[str] = (),
    observed_properties: Iterable[str] = (),
) -> OntologyGraphCandidate | None:
    """Inspect one graph and return ontology-discovery evidence when present.

    Version metadata by itself is not sufficient to classify a graph as an
    ontology graph because dataset metadata graphs commonly publish versions.
    """
    ontology_iris = sorted(
        str(subject)
        for subject in graph.subjects(RDF.type, OWL.Ontology)
        if isinstance(subject, URIRef)
    )
    classes = _declared_terms(graph, _CLASS_KINDS)
    properties = _declared_terms(graph, _PROPERTY_KINDS)

    reasons: list[str] = []
    if ontology_iris:
        reasons.append("owl_ontology_declaration")
    if classes:
        reasons.append("class_declarations")
    if properties:
        reasons.append("property_declarations")
    lowered = graph_uri.lower()
    if "ontology" in lowered or ".owl" in lowered:
        reasons.append("graph_iri_hint")

    # A graph-name hint alone is useful discovery evidence, but a plain metadata
    # graph carrying only dcterms:issued/version is not an ontology candidate.
    if not reasons:
        return None

    imports = sorted(
        str(obj)
        for ontology in graph.subjects(RDF.type, OWL.Ontology)
        for obj in graph.objects(ontology, OWL.imports)
        if isinstance(obj, URIRef)
    )
    observed_class_set = set(observed_classes)
    observed_property_set = set(observed_properties)

    return OntologyGraphCandidate(
        graph_uri=graph_uri,
        explicit_ontology_iris=ontology_iris,
        imports=imports,
        version_evidence=_version_evidence(graph, graph_uri, set(ontology_iris)),
        declared_classes=len(classes),
        declared_properties=len(properties),
        candidate_reasons=sorted(set(reasons)),
        observed_class_overlap=sorted(classes & observed_class_set),
        observed_property_overlap=sorted(properties & observed_property_set),
    )


def discover_ontology_graphs(
    dataset: Dataset,
    *,
    observed_classes: Iterable[str] = (),
    observed_properties: Iterable[str] = (),
) -> list[OntologyGraphCandidate]:
    """Inspect named graphs in an RDFLib Dataset.

    This is the local/artifact implementation. Remote endpoint discovery should
    use the same evidence contract but apply bounded SPARQL probes instead of
    downloading every graph.
    """
    candidates: list[OntologyGraphCandidate] = []
    for graph in dataset.graphs():
        identifier = str(graph.identifier)
        candidate = inspect_ontology_graph(
            graph,
            identifier,
            observed_classes=observed_classes,
            observed_properties=observed_properties,
        )
        if candidate is not None:
            candidates.append(candidate)
    return sorted(candidates, key=lambda item: item.graph_uri)


__all__ = [
    "VERSION_PREDICATES",
    "ObservedOntologyTerms",
    "OntologyGraphCandidate",
    "OntologyVersionEvidence",
    "discover_ontology_graphs",
    "inspect_ontology_graph",
    "observed_terms_from_patterns",
]


class OntologyArtifact(BaseModel):
    """One retrieved ontology artifact or graph snapshot.

    Provider version information is optional.  Reproducibility is provided by
    the retrieval time and content hash even when no provider version exists.
    """

    artifact_id: str
    ontology_iris: list[str] = Field(default_factory=list)
    version_iris: list[str] = Field(default_factory=list)
    version_values: list[str] = Field(default_factory=list)
    issued_values: list[str] = Field(default_factory=list)
    modified_values: list[str] = Field(default_factory=list)
    source_url: str | None = None
    source_graph: str | None = None
    retrieved_at: str | None = None
    media_type: str | None = None
    sha256: str | None = None
    local_path: str | None = None
    imports: list[str] = Field(default_factory=list)


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
