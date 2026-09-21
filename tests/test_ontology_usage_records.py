"""Operational ontology usage is measured separately from ontology discovery."""

from rdflib import Graph, OWL, RDF, RDFS, Literal, URIRef

from rdfsolve.evidence.ontology import (
    OntologyUsage,
    assess_ontology_usage,
    compare_ontology_usage,
    ontology_usage_slice,
)


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
        patterns,
        _ontology(),
        dataset_id="kg-a",
        ontology_artifact_id="onto-v1",
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


def test_usage_slice_keeps_used_terms_and_named_ancestors():
    graph = _ontology()
    sliced = ontology_usage_slice(graph, ["https://example.org/B", "https://example.org/p"])
    assert (URIRef("https://example.org/B"), RDFS.subClassOf, URIRef("https://example.org/A")) in sliced
    assert (URIRef("https://example.org/A"), RDF.type, OWL.Class) in sliced
    assert (URIRef("https://example.org/p"), RDFS.range, URIRef("https://example.org/B")) in sliced
    assert (URIRef("https://example.org/A"), RDFS.label, Literal("A")) in sliced


def test_pairwise_usage_overlap_is_directional_and_exact():
    left = OntologyUsage(
        dataset_id="a",
        ontology_artifact_id="onto-v1",
        resolved_classes=["urn:A", "urn:B"],
    )
    right = OntologyUsage(
        dataset_id="b",
        ontology_artifact_id="onto-v1",
        resolved_classes=["urn:B", "urn:C", "urn:D"],
    )
    overlap = compare_ontology_usage(left, right, kind="class")
    assert overlap.intersection == 1
    assert overlap.union == 4
    assert overlap.jaccard == 0.25
    assert overlap.left_containment == 0.5
    assert overlap.right_containment == 1 / 3
