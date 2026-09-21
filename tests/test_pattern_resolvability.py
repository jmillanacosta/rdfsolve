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
    result = compare_pattern_resolvability(source, target, g, source_dataset="s", target_dataset="t")
    assert result.by_kind == {"ontology_resolvable": 1}
    assert result.strongly_resolved_fraction == 1.0
    row = result.patterns[0]
    assert row.subject_relation.kind == "equivalent"
    assert row.property_relation.kind == "source_subsumed_by_target"
    assert row.object_relation.kind == "source_subsumed_by_target"


def test_literal_datatype_mismatch_prevents_full_resolution():
    g = Graph()
    g.add((URIRef("urn:A"), RDF.type, OWL.Class))
    g.add((URIRef("urn:p"), RDF.type, OWL.DatatypeProperty))
    source = [{
        "subject_class": "urn:A", "property_uri": "urn:p", "object_class": "Literal",
        "datatype": "http://www.w3.org/2001/XMLSchema#string",
    }]
    target = [{
        "subject_class": "urn:A", "property_uri": "urn:p", "object_class": "Literal",
        "datatype": "http://www.w3.org/2001/XMLSchema#integer",
    }]
    result = compare_pattern_resolvability(source, target, g, source_dataset="s", target_dataset="t")
    assert result.by_kind == {"partial": 1}
    assert result.patterns[0].datatype_match is False


def test_exact_pattern_is_not_relabelled_as_semantic_match():
    g = Graph()
    source = [{"subject_class": "urn:A", "property_uri": "urn:p", "object_class": "urn:B"}]
    result = compare_pattern_resolvability(source, source, g, source_dataset="s", target_dataset="t")
    assert result.by_kind == {"exact": 1}
