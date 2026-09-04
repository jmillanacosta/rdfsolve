"""Shared types and constants for mining modules."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper

ONTOLOGY_METACLASSES = frozenset(
    {
        "http://www.w3.org/2002/07/owl#Class",
        "http://www.w3.org/2000/01/rdf-schema#Class",
        "http://www.w3.org/2002/07/owl#ObjectProperty",
        "http://www.w3.org/2002/07/owl#DatatypeProperty",
        "http://www.w3.org/2002/07/owl#AnnotationProperty",
        "http://www.w3.org/2002/07/owl#FunctionalProperty",
        "http://www.w3.org/2002/07/owl#InverseFunctionalProperty",
        "http://www.w3.org/2002/07/owl#TransitiveProperty",
        "http://www.w3.org/2002/07/owl#SymmetricProperty",
        "http://www.w3.org/2002/07/owl#AsymmetricProperty",
        "http://www.w3.org/2002/07/owl#ReflexiveProperty",
        "http://www.w3.org/2002/07/owl#IrreflexiveProperty",
        "http://www.w3.org/2002/07/owl#Restriction",
        "http://www.w3.org/2002/07/owl#Axiom",
        "http://www.w3.org/2002/07/owl#Ontology",
        "http://www.w3.org/ns/shacl#SPARQLExecutable",
        "http://www.w3.org/ns/shacl#NodeShape",
        "http://www.w3.org/ns/shacl#PropertyShape",
        "http://rdfs.org/ns/void#Dataset",
        "http://rdfs.org/ns/void#Linkset",
        "http://www.w3.org/ns/dcat#Dataset",
        "http://www.w3.org/ns/dcat#Distribution",
    }
)

__all__ = ["ONTOLOGY_METACLASSES"]
