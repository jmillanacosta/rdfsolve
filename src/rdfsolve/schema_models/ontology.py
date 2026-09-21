"""Ontology structure models (TBox)."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel, Field
from rdflib import OWL, RDF, RDFS, Graph, Literal, URIRef

from rdfsolve.schema_models.enrichment import TermAnnotation


class SubClassRelation(BaseModel):
    """rdfs:subClassOf relation."""

    child: str
    parent: str
    confidence: float = 1.0


class SubPropertyRelation(BaseModel):
    """rdfs:subPropertyOf relation."""

    child: str
    parent: str


class EquivalentClassRelation(BaseModel):
    """owl:equivalentClass relation between named classes."""

    class1: str
    class2: str


class EquivalentPropertyRelation(BaseModel):
    """owl:equivalentProperty relation between named properties."""

    property1: str
    property2: str


class DisjointClassRelation(BaseModel):
    """owl:disjointWith relation between named classes."""

    class1: str
    class2: str


class DomainAssertion(BaseModel):
    """rdfs:domain assertion."""

    property: str = Field(..., alias="property_uri")
    domain: str


class RangeAssertion(BaseModel):
    """rdfs:range assertion."""

    property: str = Field(..., alias="property_uri")
    range: str


class InverseRelation(BaseModel):
    """owl:inverseOf relation."""

    property1: str
    property2: str


class PropertyCharacteristic(BaseModel):
    """OWL property characteristic."""

    property: str = Field(..., alias="property_uri")
    characteristic: str


class Restriction(BaseModel):
    """OWL restriction."""

    on_property: str
    restriction_type: str
    value: str | None = None
    cardinality: int | None = None


class OntologyStructure(BaseModel):
    """Selected ontology classes and axioms, not a complete OWL model."""

    classes: list[str] = Field(default_factory=list)
    annotations: list[TermAnnotation] = Field(default_factory=list)
    subclass_relations: list[SubClassRelation] = []
    subproperty_relations: list[SubPropertyRelation] = []
    equivalent_classes: list[EquivalentClassRelation] = []
    equivalent_properties: list[EquivalentPropertyRelation] = []
    disjoint_classes: list[DisjointClassRelation] = []
    deprecated_terms: list[str] = []
    domain_assertions: list[DomainAssertion] = []
    range_assertions: list[RangeAssertion] = []
    inverse_properties: list[InverseRelation] = []
    property_characteristics: list[PropertyCharacteristic] = []
    restrictions: list[Restriction] = []

    def for_schema(
        self, class_uris: Iterable[str], property_uris: Iterable[str]
    ) -> OntologyStructure:
        """Keep used classes, used property axioms, and class ancestors.

        Do not expand descendants. An entity-like leaf stays only if the
        schema uses it as a class. This selects a view; it does not change
        the meaning of the source's class declarations.
        """
        properties = set(property_uris)
        domains = [item for item in self.domain_assertions if item.property in properties]
        ranges = [item for item in self.range_assertions if item.property in properties]
        # A used type alone does not establish an embedded ontology.
        referenced = set(self.classes)
        for sub in self.subclass_relations:
            referenced.update((sub.child, sub.parent))
        selected = (
            (set(class_uris) & referenced)
            | {item.domain for item in domains}
            | {item.range for item in ranges}
        )
        parents: dict[str, set[str]] = defaultdict(set)
        for sub in self.subclass_relations:
            parents[sub.child].add(sub.parent)
        pending = list(selected)
        while pending:
            for parent in parents[pending.pop()] - selected:
                selected.add(parent)
                pending.append(parent)
        # Property ancestry/equivalence is retained only around used properties.
        selected_properties = set(properties)
        prop_parents: dict[str, set[str]] = defaultdict(set)
        for subprop in self.subproperty_relations:
            prop_parents[subprop.child].add(subprop.parent)
        pending_properties = list(selected_properties)
        while pending_properties:
            for parent in prop_parents[pending_properties.pop()] - selected_properties:
                selected_properties.add(parent)
                pending_properties.append(parent)
        for eq_prop in self.equivalent_properties:
            if eq_prop.property1 in selected_properties or eq_prop.property2 in selected_properties:
                selected_properties.update((eq_prop.property1, eq_prop.property2))

        return OntologyStructure(
            classes=sorted(selected),
            annotations=self.annotations,
            subclass_relations=[item for item in self.subclass_relations if item.child in selected],
            subproperty_relations=[
                item for item in self.subproperty_relations if item.child in selected_properties
            ],
            equivalent_classes=[
                item
                for item in self.equivalent_classes
                if item.class1 in selected or item.class2 in selected
            ],
            equivalent_properties=[
                item
                for item in self.equivalent_properties
                if item.property1 in selected_properties or item.property2 in selected_properties
            ],
            disjoint_classes=[
                item
                for item in self.disjoint_classes
                if item.class1 in selected or item.class2 in selected
            ],
            deprecated_terms=sorted(
                term
                for term in self.deprecated_terms
                if term in selected or term in selected_properties
            ),
            domain_assertions=domains,
            range_assertions=ranges,
            inverse_properties=[
                item
                for item in self.inverse_properties
                if item.property1 in properties or item.property2 in properties
            ],
            property_characteristics=[
                item for item in self.property_characteristics if item.property in properties
            ],
            restrictions=[item for item in self.restrictions if item.on_property in properties],
        )

    def to_rdf_graph(self, base_uri: str = "http://example.org/ontology/") -> Graph:
        """Export as RDFLib Graph."""
        g = Graph()
        g.bind("rdf", RDF)
        g.bind("rdfs", RDFS)
        g.bind("owl", OWL)

        for class_uri in self.classes:
            g.add((URIRef(class_uri), RDF.type, RDFS.Class))

        for sub in self.subclass_relations:
            g.add((URIRef(sub.child), RDFS.subClassOf, URIRef(sub.parent)))

        for subprop in self.subproperty_relations:
            g.add((URIRef(subprop.child), RDFS.subPropertyOf, URIRef(subprop.parent)))

        for eq_class in self.equivalent_classes:
            g.add((URIRef(eq_class.class1), OWL.equivalentClass, URIRef(eq_class.class2)))

        for eq_prop in self.equivalent_properties:
            g.add((URIRef(eq_prop.property1), OWL.equivalentProperty, URIRef(eq_prop.property2)))

        for disjoint in self.disjoint_classes:
            g.add((URIRef(disjoint.class1), OWL.disjointWith, URIRef(disjoint.class2)))

        for term in self.deprecated_terms:
            g.add((URIRef(term), OWL.deprecated, Literal(True)))

        for dom in self.domain_assertions:
            g.add((URIRef(dom.property), RDFS.domain, URIRef(dom.domain)))

        for rng in self.range_assertions:
            g.add((URIRef(rng.property), RDFS.range, URIRef(rng.range)))

        for inv in self.inverse_properties:
            g.add((URIRef(inv.property1), OWL.inverseOf, URIRef(inv.property2)))

        for char in self.property_characteristics:
            g.add((URIRef(char.property), RDF.type, URIRef(char.characteristic)))

        terms = set(self.term_iris())
        for annotation in self.annotations:
            if annotation.term_iri in terms:
                g.add(
                    (
                        URIRef(annotation.term_iri),
                        URIRef(annotation.predicate),
                        annotation.text.to_rdf(),
                    )
                )

        return g

    def term_iris(self) -> list[str]:
        """Return retained terms, without adding unused descendants."""
        terms = set(self.classes)
        for sub in self.subclass_relations:
            terms.update((sub.child, sub.parent))
        for subprop in self.subproperty_relations:
            terms.update((subprop.child, subprop.parent))
        for eq_class in self.equivalent_classes:
            terms.update((eq_class.class1, eq_class.class2))
        for eq_prop in self.equivalent_properties:
            terms.update((eq_prop.property1, eq_prop.property2))
        for disjoint in self.disjoint_classes:
            terms.update((disjoint.class1, disjoint.class2))
        terms.update(self.deprecated_terms)
        for domain in self.domain_assertions:
            terms.update((domain.property, domain.domain))
        for range_ in self.range_assertions:
            terms.update((range_.property, range_.range))
        for inverse in self.inverse_properties:
            terms.update((inverse.property1, inverse.property2))
        terms.update(item.property for item in self.property_characteristics)
        return sorted(terms)

    def to_turtle(self, base_uri: str = "http://example.org/ontology/") -> str:
        """Export as RDFS/OWL Turtle."""
        g = self.to_rdf_graph(base_uri)
        result: str = g.serialize(format="turtle")
        return result

    def to_jsonld(self, base_uri: str = "http://example.org/ontology/") -> dict[str, Any]:
        """Export as JSON-LD."""
        g = self.to_rdf_graph(base_uri)
        serialized: str = g.serialize(format="json-ld", indent=2)
        result: dict[str, Any] = json.loads(serialized)
        return result


__all__ = [
    "DisjointClassRelation",
    "DomainAssertion",
    "EquivalentClassRelation",
    "EquivalentPropertyRelation",
    "InverseRelation",
    "OntologyStructure",
    "PropertyCharacteristic",
    "RangeAssertion",
    "Restriction",
    "SubClassRelation",
    "SubPropertyRelation",
]
