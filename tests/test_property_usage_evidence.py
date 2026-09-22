from rdflib import RDF, Dataset, URIRef
from rdfsolve.evidence.observed import collect_property_usage_evidence
from rdfsolve.mining.local_graph import LocalGraphHelper


def test_property_usage_uses_subject_denominator_and_union_scope():
    ds = Dataset()
    A = URIRef("urn:A")
    p = URIRef("urn:p")
    q = URIRef("urn:q")
    s1 = URIRef("urn:s1")
    s2 = URIRef("urn:s2")
    o1 = URIRef("urn:o1")
    o2 = URIRef("urn:o2")
    g1 = ds.graph(URIRef("urn:g1"))
    g2 = ds.graph(URIRef("urn:g2"))
    for graph in (g1, g2):
        graph.add((s1, RDF.type, A))
        graph.add((s1, p, o1))
    g2.add((s2, RDF.type, A))
    g2.add((s2, q, o2))
    helper = LocalGraphHelper("local", ds)
    result = collect_property_usage_evidence(
        dataset_id="demo",
        classes=[str(A)],
        class_entity_counts={str(A): 2},
        helper=helper,
        graph_uris=["urn:g1", "urn:g2"],
    )
    by_property = {row.property_uri: row for row in result.records}
    p_row = by_property[str(p)]
    assert p_row.scope_semantics == "rdf_merge_selected_graphs"
    assert p_row.eligible_subjects == 2
    assert p_row.subjects_with_property == 1
    assert p_row.triple_count == 1
    assert p_row.distinct_objects == 1
    assert p_row.support_fraction == 0.5
    q_row = by_property[str(q)]
    assert q_row.subjects_with_property == 1
    assert q_row.support_fraction == 0.5
