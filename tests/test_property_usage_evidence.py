from rdflib import Dataset, RDF, URIRef

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
    # Same p triple appears in both graphs. Dataset-scope support must count it once.
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


def test_property_usage_default_graph_semantics():
    ds = Dataset()
    A = URIRef("urn:A")
    p = URIRef("urn:p")
    s = URIRef("urn:s")
    o = URIRef("urn:o")
    ds.default_context.add((s, RDF.type, A))
    ds.default_context.add((s, p, o))
    helper = LocalGraphHelper("local", ds)
    result = collect_property_usage_evidence(
        dataset_id="demo",
        classes=[str(A)],
        class_entity_counts={str(A): 1},
        helper=helper,
        graph_uris=None,
    )
    assert result.scope_semantics == "endpoint_default_graph"
    assert result.records[0].support_fraction == 1.0


def test_property_usage_profiles_node_kinds_datatypes_languages_and_bounded_histogram():
    from rdflib import BNode, Literal, Namespace
    from rdflib.namespace import XSD

    ds = Dataset()
    EX = Namespace("urn:ex:")
    g = ds.graph(URIRef("urn:g"))
    subjects = [EX[f"s{i}"] for i in range(1, 5)]
    for subject in subjects:
        g.add((subject, RDF.type, EX.A))
    # s1 has one IRI; s2 has two IRIs; s3 has six literal values; s4 has no p.
    g.add((subjects[0], EX.p, EX.o1))
    g.add((subjects[1], EX.p, EX.o1))
    g.add((subjects[1], EX.p, EX.o2))
    for i in range(6):
        g.add((subjects[2], EX.p, Literal(str(i), datatype=XSD.integer)))
    g.add((subjects[0], EX.label, Literal("hello", lang="en")))
    g.add((subjects[1], EX.blank, BNode("b1")))

    helper = LocalGraphHelper("local", ds)
    result = collect_property_usage_evidence(
        dataset_id="demo",
        classes=[str(EX.A)],
        class_entity_counts={str(EX.A): 4},
        helper=helper,
        graph_uris=["urn:g"],
        collect_histograms=True,
    )
    by_property = {row.property_uri: row for row in result.records}

    p_row = by_property[str(EX.p)]
    assert p_row.node_kind_counts == {"IRI": 3, "Literal": 6}
    assert p_row.datatype_counts[str(XSD.integer)] == 6
    assert p_row.value_count_histogram == {"1": 1, "2": 1, "6-10": 1, "0": 1}
    assert p_row.summary_state.status == "complete"
    assert p_row.node_kind_state.status == "complete"
    assert p_row.datatype_state.status == "complete"
    assert p_row.histogram_state.status == "complete"

    label_row = by_property[str(EX.label)]
    assert label_row.node_kind_counts == {"Literal": 1}
    assert label_row.language_counts == {"en": 1}

    blank_row = by_property[str(EX.blank)]
    assert blank_row.node_kind_counts == {"BlankNode": 1}


def test_histogram_zero_bin_not_claimed_without_denominator():
    from rdflib import Namespace

    ds = Dataset()
    EX = Namespace("urn:ex:")
    g = ds.graph(URIRef("urn:g"))
    g.add((EX.s, RDF.type, EX.A))
    g.add((EX.s, EX.p, EX.o))
    result = collect_property_usage_evidence(
        dataset_id="demo",
        classes=[str(EX.A)],
        class_entity_counts=None,
        helper=LocalGraphHelper("local", ds),
        graph_uris=["urn:g"],
        collect_histograms=True,
    )
    row = result.records[0]
    assert row.denominator_state == "not_run"
    assert row.value_count_histogram == {"1": 1}
    assert "0" not in row.value_count_histogram


def test_property_usage_artifact_retains_class_population_denominators():
    ds = Dataset()
    A = URIRef("urn:A")
    B = URIRef("urn:B")
    ds.default_context.add((URIRef("urn:a"), RDF.type, A))
    helper = LocalGraphHelper("local", ds)
    result = collect_property_usage_evidence(
        dataset_id="demo",
        classes=[str(A), str(B)],
        class_entity_counts={str(A): 1},
        helper=helper,
        graph_uris=None,
        collect_node_kinds=False,
        collect_datatypes=False,
    )
    by_class = {row.class_iri: row for row in result.class_populations}
    assert by_class[str(A)].subject_count == 1
    assert by_class[str(A)].count_status == "complete"
    assert by_class[str(B)].subject_count is None
    assert by_class[str(B)].count_status == "not_run"
