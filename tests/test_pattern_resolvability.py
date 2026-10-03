from rdflib import OWL, RDF, RDFS, Graph, URIRef
from rdfsolve.analysis.pattern_resolvability import compare_pattern_resolvability


def test_pattern_resolution_uses_class_and_property_semantics():
    g = Graph()
    for cls in ("urn:A", "urn:B", "urn:X", "urn:Y"):
        g.add((URIRef(cls), RDF.type, OWL.Class))
    for prop in ("urn:p", "urn:q"):
        g.add((URIRef(prop), RDF.type, OWL.ObjectProperty))
    g.add((URIRef("urn:A"), OWL.equivalentClass, URIRef("urn:B")))
    g.add((URIRef("urn:X"), RDFS.subClassOf, URIRef("urn:Y")))
    g.add((URIRef("urn:p"), RDFS.subPropertyOf, URIRef("urn:q")))
    source = [{"subject_class": "urn:A", "property_uri": "urn:p", "object_class": "urn:X"}]
    target = [{"subject_class": "urn:B", "property_uri": "urn:q", "object_class": "urn:Y"}]
    result = compare_pattern_resolvability(
        source, target, g, source_dataset="s", target_dataset="t"
    )
    assert result.by_kind == {"ontology_resolvable": 1}
    assert result.strongly_resolved_fraction == 1.0
    row = result.patterns[0]
    assert row.subject_relation.kind == "equivalent"
    assert row.property_relation.kind == "source_subsumed_by_target"
    assert row.object_relation.kind == "source_subsumed_by_target"
