"""Ontology structure models (TBox)."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field
from rdflib import OWL, RDF, RDFS, Graph, Namespace, URIRef
from rdflib import Literal as RDFLiteral


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
    """Complete ontology structure (TBox)."""

    subclass_relations: list[SubClassRelation] = []
    domain_assertions: list[DomainAssertion] = []
    range_assertions: list[RangeAssertion] = []
    inverse_properties: list[InverseRelation] = []
    property_characteristics: list[PropertyCharacteristic] = []
    restrictions: list[Restriction] = []

    def to_rdf_graph(self, base_uri: str = "http://example.org/ontology/") -> Graph:
        """Export as RDFLib Graph."""
        g = Graph()
        g.bind("rdf", RDF)
        g.bind("rdfs", RDFS)
        g.bind("owl", OWL)

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

        return g

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
