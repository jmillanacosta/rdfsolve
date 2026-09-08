"""Ontology structure models (TBox)."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel, Field
from rdflib import OWL, RDF, RDFS, Graph, Namespace, URIRef
from rdflib import Literal as RDFLiteral

from rdfsolve.schema_models.enrichment import TermAnnotation


class SubClassRelation(BaseModel):
    """rdfs:subClassOf relation."""

    child: str
    parent: str
    confidence: float = 1.0


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
        for relation in self.subclass_relations:
            referenced.update((relation.child, relation.parent))
        selected = (
            (set(class_uris) & referenced)
            | {item.domain for item in domains}
            | {item.range for item in ranges}
        )
        parents: dict[str, set[str]] = defaultdict(set)
        for relation in self.subclass_relations:
            parents[relation.child].add(relation.parent)
        pending = list(selected)
        while pending:
            for parent in parents[pending.pop()] - selected:
                selected.add(parent)
                pending.append(parent)
        return OntologyStructure(
            classes=sorted(selected),
            annotations=self.annotations,
            subclass_relations=[item for item in self.subclass_relations if item.child in selected],
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

        for rel in self.subclass_relations:
            g.add((URIRef(rel.child), RDFS.subClassOf, URIRef(rel.parent)))

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
        for relation in self.subclass_relations:
            terms.update((relation.child, relation.parent))
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
    "DomainAssertion",
    "InverseRelation",
    "OntologyStructure",
    "PropertyCharacteristic",
    "RangeAssertion",
    "Restriction",
    "SubClassRelation",
]
