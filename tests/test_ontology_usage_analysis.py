from rdflib import OWL, RDF, Dataset, URIRef
from rdfsolve.analysis.ontology_usage import discover_ontology_graphs


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
