from rdflib import OWL, RDF, RDFS, Graph, URIRef

from rdfsolve.analysis.ontology_resolvability import compare_usage_resolvability
from rdfsolve.evidence.ontology import OntologyUsage


def usage(dataset, classes=(), properties=()):
    return OntologyUsage(
        dataset_id=dataset,
        ontology_artifact_id="sha256:onto",
        resolved_classes=list(classes),
        resolved_properties=list(properties),
    )


def test_class_resolvability_keeps_relation_categories_separate():
    g = Graph()
    a, b, c, d, root = map(URIRef, ["urn:A", "urn:B", "urn:C", "urn:D", "urn:Root"])
    for cls in (a, b, c, d, root):
        g.add((cls, RDF.type, OWL.Class))
    g.add((a, OWL.equivalentClass, b))
    g.add((c, RDFS.subClassOf, root))
    g.add((d, RDFS.subClassOf, root))

    source = usage("left", classes=["urn:A", "urn:C", "urn:D"])
    target = usage("right", classes=["urn:B", "urn:Root"])
    result = compare_usage_resolvability(source, target, g, kind="class")
    assert result.by_kind == {"equivalent": 1, "source_subsumed_by_target": 2}
    assert result.resolved_fraction == 1.0


def test_shared_ancestor_is_reported_but_not_resolved_by_default():
    g = Graph()
    left, right, root = map(URIRef, ["urn:Left", "urn:Right", "urn:Root"])
    for cls in (left, right, root):
        g.add((cls, RDF.type, OWL.Class))
    g.add((left, RDFS.subClassOf, root))
    g.add((right, RDFS.subClassOf, root))
    result = compare_usage_resolvability(
        usage("left", classes=["urn:Left"]),
        usage("right", classes=["urn:Right"]),
        g,
        kind="class",
    )
    assert result.by_kind == {"shared_named_ancestor": 1}
    assert result.resolved_fraction == 0.0


def test_property_subsumption_is_supported_separately_from_classes():
    g = Graph()
    p, q = URIRef("urn:p"), URIRef("urn:q")
    g.add((p, RDF.type, OWL.ObjectProperty))
    g.add((q, RDF.type, OWL.ObjectProperty))
    g.add((p, RDFS.subPropertyOf, q))
    result = compare_usage_resolvability(
        usage("left", properties=["urn:p"]),
        usage("right", properties=["urn:q"]),
        g,
        kind="property",
    )
    assert result.by_kind == {"source_subsumed_by_target": 1}
    assert result.resolved_fraction == 1.0
