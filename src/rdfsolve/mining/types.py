"""Shared types and constants for mining modules."""

from __future__ import annotations

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
    }
)

METADATA_RECORD_TYPES = frozenset(
    {
        "http://www.w3.org/2002/07/owl#Ontology",
        "http://www.w3.org/ns/shacl#SPARQLExecutable",
        "http://www.w3.org/ns/shacl#SPARQLSelectExecutable",
        "http://www.w3.org/ns/shacl#SPARQLAskExecutable",
        "http://www.w3.org/ns/shacl#SPARQLConstructExecutable",
        "http://www.w3.org/ns/shacl#SPARQLUpdateExecutable",
        "http://www.w3.org/ns/shacl#SPARQLConstraint",
        "http://www.w3.org/ns/shacl#NodeShape",
        "http://www.w3.org/ns/shacl#PropertyShape",
        "http://rdfs.org/ns/void#Dataset",
        "http://rdfs.org/ns/void#Linkset",
        "http://www.w3.org/ns/dcat#Dataset",
        "http://www.w3.org/ns/dcat#Distribution",
    }
)
ONTOLOGY_METACLASSES |= METADATA_RECORD_TYPES

__all__ = ["METADATA_RECORD_TYPES", "ONTOLOGY_METACLASSES"]
