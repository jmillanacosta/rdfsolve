"""Terms that rdfsolve exporters write, each from a published vocabulary.

Source terms, such as the classes and properties of a mined dataset, are not
listed here. rdfsolve writes them only as they were observed or declared.
"""

from __future__ import annotations

from collections.abc import Iterable

from rdflib import RDF, Graph, URIRef

# Namespace -> specification of the vocabulary.
VOCABULARIES: dict[str, str] = {
    "http://rdfs.org/ns/void#": "https://www.w3.org/TR/void/",
    "http://ldf.fi/void-ext#": "https://github.com/sib-swiss/void-generator",
    "http://www.w3.org/ns/sparql-service-description#": "https://www.w3.org/TR/sparql11-service-description/",
    "http://www.w3.org/ns/shacl#": "https://www.w3.org/TR/shacl/",
    "http://purl.org/dc/terms/": "https://www.dublincore.org/specifications/dublin-core/dcmi-terms/",
    "http://xmlns.com/foaf/0.1/": "http://xmlns.com/foaf/spec/",
    "http://www.w3.org/2002/07/owl#": "https://www.w3.org/TR/owl2-rdf-based-semantics/",
    "http://www.w3.org/2000/01/rdf-schema#": "https://www.w3.org/TR/rdf-schema/",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "https://www.w3.org/TR/rdf11-concepts/",
    "https://schema.org/": "https://schema.org/docs/schemas.html",
}

_TERMS = {
    "http://rdfs.org/ns/void#": (
        "Dataset DatasetDescription Linkset class classPartition classes distinctSubjects "
        "documents entities exampleResource linkPredicate objectsTarget properties property "
        "propertyPartition sparqlEndpoint subjectsTarget triples vocabulary"
    ),
    "http://ldf.fi/void-ext#": "datatype datatypePartition",
    "http://www.w3.org/ns/sparql-service-description#": (
        "Dataset Graph NamedGraph Service defaultDataset endpoint graph name namedGraph"
    ),
    "http://www.w3.org/ns/shacl#": (
        "NodeShape PropertyShape SPARQLExecutable SPARQLSelectExecutable SPARQLAskExecutable "
        "SPARQLConstructExecutable alternativePath ask class closed construct datatype "
        "deactivated declare description ignoredProperties inversePath maxCount minCount name "
        "namespace nodeKind oneOrMorePath or path prefix prefixes property qualifiedMaxCount "
        "qualifiedMinCount qualifiedValueShape qualifiedValueShapesDisjoint select targetClass "
        "zeroOrMorePath zeroOrOnePath"
    ),
    "http://purl.org/dc/terms/": (
        "created creator description issued license modified publisher source title type"
    ),
    "http://xmlns.com/foaf/0.1/": "homepage primaryTopic",
    "http://www.w3.org/2002/07/owl#": "Ontology imports versionIRI versionInfo",
    "http://www.w3.org/2000/01/rdf-schema#": "comment label seeAlso",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "first rest type",
    "https://schema.org/": "isBasedOn target url",
}

GENERATED_TERMS: frozenset[str] = frozenset(
    namespace + name for namespace, names in _TERMS.items() for name in names.split()
)


def unregistered_terms(graph: Graph, source_terms: Iterable[str]) -> set[str]:
    """Return predicates and rdf:type classes that are neither registered nor source terms."""
    allowed = GENERATED_TERMS | set(source_terms)
    used = {str(p) for p in graph.predicates()} | {
        str(o) for o in graph.objects(None, RDF.type) if isinstance(o, URIRef)
    }
    return used - allowed
