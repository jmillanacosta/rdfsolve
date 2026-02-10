"""Standard RDF prefix mappings."""

from __future__ import annotations

STANDARD_PREFIXES: dict[str, str] = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "void": "http://rdfs.org/ns/void#",
    "sd": "http://www.w3.org/ns/sparql-service-description#",
    "sh": "http://www.w3.org/ns/shacl#",
    "schema": "https://schema.org/",
    "prov": "http://www.w3.org/ns/prov#",
}
