from rdflib import RDF, RDFS, Literal, URIRef


def test_scoped_ontology_keeps_named_equivalence_property_hierarchy_disjointness_and_deprecation():
    import json
    from unittest.mock import Mock

    from rdflib import OWL, Dataset
    from rdfsolve.mining.ontology_extraction import OntologyMiner

    dataset = Dataset()
    graph = dataset.graph(URIRef("urn:chosen"))
    graph.add((URIRef("urn:A"), RDF.type, OWL.Class))
    graph.add((URIRef("urn:A"), OWL.equivalentClass, URIRef("urn:A2")))
    graph.add((URIRef("urn:A"), OWL.disjointWith, URIRef("urn:Other")))
    graph.add((URIRef("urn:p"), RDF.type, OWL.ObjectProperty))
    graph.add((URIRef("urn:p"), RDFS.subPropertyOf, URIRef("urn:super")))
    graph.add((URIRef("urn:p"), OWL.equivalentProperty, URIRef("urn:p2")))
    graph.add((URIRef("urn:p"), OWL.deprecated, Literal(True)))
    helper = Mock()
    helper.select.side_effect = lambda query, **kwargs: json.loads(
        dataset.query(query).serialize(format="json")
    )
    result = OntologyMiner(
        helper, ["urn:chosen"], class_iris=["urn:A"], property_iris=["urn:p"], batch_size=20
    ).mine()
    assert [(r.class1, r.class2) for r in result.equivalent_classes] == [("urn:A", "urn:A2")]
    assert [(r.class1, r.class2) for r in result.disjoint_classes] == [("urn:A", "urn:Other")]
    assert [(r.child, r.parent) for r in result.subproperty_relations] == [("urn:p", "urn:super")]
    assert [(r.property1, r.property2) for r in result.equivalent_properties] == [
        ("urn:p", "urn:p2")
    ]
    assert result.deprecated_terms == ["urn:p"]
    out = result.to_rdf_graph()
    assert (URIRef("urn:p"), RDFS.subPropertyOf, URIRef("urn:super")) in out
    assert (URIRef("urn:A"), OWL.equivalentClass, URIRef("urn:A2")) in out
    assert (URIRef("urn:p"), OWL.deprecated, Literal(True)) in out
