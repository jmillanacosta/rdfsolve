from rdflib import OWL, RDF, Graph, URIRef
from rdfsolve.evidence.ontology import assess_ontology_usage


def test_usage_denominator_can_be_scoped_to_one_ontology_candidate():
    graph = Graph()
    graph.add((URIRef("https://onto.example/A"), RDF.type, OWL.Class))
    graph.add((URIRef("https://onto.example/p"), RDF.type, OWL.ObjectProperty))
    patterns = [
        {
            "subject_class": "https://onto.example/A",
            "property_uri": "https://onto.example/p",
            "object_class": "Literal",
        },
        {
            "subject_class": "https://other.example/B",
            "property_uri": "https://other.example/q",
            "object_class": "Literal",
        },
    ]
    usage = assess_ontology_usage(
        patterns,
        graph,
        dataset_id="kg",
        ontology_artifact_id="onto",
        expected_classes=["https://onto.example/A"],
        expected_properties=["https://onto.example/p"],
    )
    assert usage.resolved_classes == ["https://onto.example/A"]
    assert usage.unresolved_classes == []
    assert usage.resolved_properties == ["https://onto.example/p"]
    assert usage.unresolved_properties == []
