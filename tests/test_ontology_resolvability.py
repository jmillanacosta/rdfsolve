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
