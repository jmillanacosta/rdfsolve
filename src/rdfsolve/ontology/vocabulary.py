"""The vocabulary that ontologies are written in, defined once for all of rdfsolve.

Which IRIs declare a class or a property, which predicates state ontology structure, which
namespaces annotate terms rather than describe data, and which predicates state a version.
"""

from __future__ import annotations

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
OBO = "http://purl.obolibrary.org/obo/"

RDF_TYPE = RDF + "type"
SUBCLASS_OF = RDFS + "subClassOf"
LABEL = RDFS + "label"
DEPRECATED = OWL + "deprecated"
OWL_CLASS = OWL + "Class"
RDFS_CLASS = RDFS + "Class"

# Types that declare a term a class, or a property.
CLASS_TYPES = (OWL_CLASS, RDFS_CLASS)
PROPERTY_TYPES = (
    RDF + "Property",
    OWL + "ObjectProperty",
    OWL + "DatatypeProperty",
    OWL + "AnnotationProperty",
)
# Every type of an OWL or RDFS construct: what an ontology is made of, not what data describe.
OWL_CONSTRUCT_TYPES = frozenset(
    {
        *CLASS_TYPES,
        *PROPERTY_TYPES[1:],
        *(
            OWL + name
            for name in (
                "FunctionalProperty",
                "InverseFunctionalProperty",
                "TransitiveProperty",
                "SymmetricProperty",
                "AsymmetricProperty",
                "ReflexiveProperty",
                "IrreflexiveProperty",
                "Restriction",
                "Axiom",
            )
        ),
    }
)

# Predicates that state the structure of an ontology between named terms.
STRUCTURE_PREDICATES = (
    SUBCLASS_OF,
    RDFS + "subPropertyOf",
    RDFS + "domain",
    RDFS + "range",
    OWL + "equivalentClass",
    OWL + "equivalentProperty",
    OWL + "inverseOf",
    OWL + "disjointWith",
)

# Namespaces whose predicates describe ontology structure or annotate terms, not data.
NON_DATA_NAMESPACES = (
    RDF,
    RDFS,
    OWL,
    "http://www.geneontology.org/formats/oboInOwl#",
    OBO + "IAO_",
    "http://www.w3.org/2004/02/skos/core#",
    "http://purl.org/dc/elements/1.1/",
    "http://purl.org/dc/terms/",
)

# Namespaces of infrastructure vocabularies: used by data, but not ontologies of a domain.
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
# The W3C namespaces of RDF itself.
STANDARD_NAMESPACES = (RDF, RDFS, "http://www.w3.org/2001/XMLSchema#", OWL)

# Predicates that state the version of an ontology or a graph.
VERSION_PREDICATES = (
    OWL + "versionIRI",
    OWL + "versionInfo",
    "http://www.w3.org/ns/dcat#version",
    "http://purl.org/pav/version",
    "http://purl.org/dc/terms/issued",
    "http://purl.org/dc/terms/modified",
    "https://schema.org/version",
)
DEFINITIONS = (OBO + "IAO_0000115", "http://www.w3.org/2004/02/skos/core#definition")

# Types of records that describe a dataset or a query, not entities: metadata, shapes, VoID.
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
# Types that are not classes of data: OWL and RDFS constructs and metadata records.
NOT_DATA_TYPES = OWL_CONSTRUCT_TYPES | METADATA_RECORD_TYPES
