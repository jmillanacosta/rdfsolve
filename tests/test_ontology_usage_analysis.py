from rdflib import OWL, RDF, RDFS, Dataset, Literal, Namespace, URIRef

from rdfsolve.analysis.ontology_usage import (
    discover_ontology_graphs,
    observed_terms_from_patterns,
)

PAV = Namespace("http://purl.org/pav/")


def test_discovery_is_not_usage_without_empirical_overlap():
    dataset = Dataset()
    graph = dataset.graph(URIRef("https://example.org/ontology/v1.owl"))
    graph.add((URIRef("urn:onto"), RDF.type, OWL.Ontology))
    graph.add((URIRef("urn:Class"), RDF.type, OWL.Class))
    graph.add((URIRef("urn:p"), RDF.type, OWL.ObjectProperty))
    graph.add((URIRef("urn:onto"), OWL.versionInfo, Literal("1.0")))

    [candidate] = discover_ontology_graphs(dataset)
    assert candidate.graph_uri == "https://example.org/ontology/v1.owl"
    assert candidate.explicit_ontology_iris == ["urn:onto"]
    assert candidate.declared_classes == 1
    assert candidate.declared_properties == 1
    assert candidate.used_by_schema is False
    assert [(v.predicate, v.value, v.scope) for v in candidate.version_evidence] == [
        (str(OWL.versionInfo), "1.0", "ontology")
    ]


def test_usage_requires_overlap_with_observed_classes_or_properties():
    dataset = Dataset()
    graph = dataset.graph(URIRef("urn:ontology-graph"))
    graph.add((URIRef("urn:onto"), RDF.type, OWL.Ontology))
    graph.add((URIRef("https://example.org/C"), RDF.type, OWL.Class))
    graph.add((URIRef("https://example.org/p"), RDF.type, OWL.DatatypeProperty))

    [candidate] = discover_ontology_graphs(
        dataset,
        observed_classes=["https://example.org/C"],
        observed_properties=["https://example.org/p"],
    )
    assert candidate.used_by_schema is True
    assert candidate.observed_class_overlap == ["https://example.org/C"]
    assert candidate.observed_property_overlap == ["https://example.org/p"]


def test_dataset_version_metadata_alone_does_not_create_ontology_candidate():
    dataset = Dataset()
    graph = dataset.graph(URIRef("urn:metadata"))
    graph.add((URIRef("urn:dataset"), PAV.version, Literal("2026.09")))
    graph.add((URIRef("urn:dataset"), URIRef("http://www.w3.org/ns/dcat#version"), Literal("2026-09")))
    assert discover_ontology_graphs(dataset) == []


def test_graph_iri_hint_discovers_candidate_but_does_not_claim_usage():
    dataset = Dataset()
    dataset.graph(URIRef("https://example.org/ontology/source.owl")).add(
        (URIRef("urn:x"), RDFS.label, Literal("metadata-only ontology graph"))
    )
    [candidate] = discover_ontology_graphs(dataset)
    assert candidate.candidate_reasons == ["graph_iri_hint"]
    assert candidate.used_by_schema is False


def test_ontology_imports_and_version_iri_are_retained():
    dataset = Dataset()
    graph = dataset.graph(URIRef("urn:g"))
    graph.add((URIRef("urn:onto"), RDF.type, OWL.Ontology))
    graph.add((URIRef("urn:onto"), OWL.versionIRI, URIRef("urn:onto/2026-09")))
    graph.add((URIRef("urn:onto"), OWL.imports, URIRef("urn:imported")))
    [candidate] = discover_ontology_graphs(dataset)
    assert candidate.imports == ["urn:imported"]
    assert [(v.predicate, v.value, v.scope) for v in candidate.version_evidence] == [
        (str(OWL.versionIRI), "urn:onto/2026-09", "ontology")
    ]


def test_observed_terms_are_extracted_from_serialized_patterns():
    terms = observed_terms_from_patterns(
        [
            {
                "subject_class": "https://example.org/A",
                "property_uri": "https://example.org/p",
                "object_class": "https://example.org/B",
            },
            {
                "subject_class": "https://example.org/A",
                "property_uri": "https://example.org/q",
                "object_class": "Literal",
            },
        ]
    )
    assert terms.classes == {"https://example.org/A", "https://example.org/B"}
    assert terms.properties == {"https://example.org/p", "https://example.org/q"}
