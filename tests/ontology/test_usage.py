"""rdfsolve.ontology.usage: the ontology terms a dataset uses, in scope, as records."""

from rdflib import OWL, RDF, RDFS, Graph, Literal, URIRef

from rdfsolve.ontology.usage import assess_ontology_usage


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


def _ontology() -> Graph:
    g = Graph()
    g.add((URIRef("https://example.org/A"), RDF.type, OWL.Class))
    g.add((URIRef("https://example.org/B"), RDF.type, OWL.Class))
    g.add((URIRef("https://example.org/B"), RDFS.subClassOf, URIRef("https://example.org/A")))
    g.add((URIRef("https://example.org/A"), RDFS.label, Literal("A")))
    g.add((URIRef("https://example.org/B"), RDFS.label, Literal("B")))
    g.add((URIRef("https://example.org/p"), RDF.type, OWL.ObjectProperty))
    g.add((URIRef("https://example.org/p"), RDFS.domain, URIRef("https://example.org/A")))
    g.add((URIRef("https://example.org/p"), RDFS.range, URIRef("https://example.org/B")))
    g.add((URIRef("https://example.org/ClassUsedAsPredicate"), RDF.type, OWL.Class))
    return g


def test_usage_resolution_and_role_realization():
    patterns = [
        {
            "subject_class": "https://example.org/A",
            "property_uri": "https://example.org/p",
            "object_class": "https://example.org/B",
        },
        {
            "subject_class": "https://example.org/A",
            "property_uri": "https://example.org/ClassUsedAsPredicate",
            "object_class": "Literal",
        },
        {
            "subject_class": "https://example.org/Unknown",
            "property_uri": "https://example.org/unknownProperty",
            "object_class": "Literal",
        },
    ]
    usage = assess_ontology_usage(
        patterns, _ontology(), dataset_id="kg-a", ontology_artifact_id="onto-v1"
    )
    assert usage.resolved_classes == ["https://example.org/A", "https://example.org/B"]
    assert usage.unresolved_classes == ["https://example.org/Unknown"]
    assert usage.resolved_properties == [
        "https://example.org/ClassUsedAsPredicate",
        "https://example.org/p",
    ]
    assert usage.unresolved_properties == ["https://example.org/unknownProperty"]
    by_term = {item.term_iri: item for item in usage.term_usage}
    assert by_term["https://example.org/p"].role_divergence is False
    assert by_term["https://example.org/ClassUsedAsPredicate"].declared_roles == ["class"]
    assert by_term["https://example.org/ClassUsedAsPredicate"].observed_roles == ["predicate"]
    assert by_term["https://example.org/ClassUsedAsPredicate"].role_divergence is True
