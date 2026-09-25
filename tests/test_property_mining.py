"""Mine and count mixed fields through bounded property queries."""

from rdflib import Graph

from rdfsolve import SchemaMiner


def test_property_queries_preserve_typed_literal_and_untyped_counts():
    graph = Graph().parse(
        data="""
        @prefix e: <urn:e:> .
        e:a a e:A; e:link e:b; e:text "one", "two"; e:ref e:untyped .
        e:c a e:A; e:link e:b; e:text "one" .
        e:b a e:B; e:text "target" .
    """,
        format="turtle",
    )
    with SchemaMiner.from_graph(graph, class_batch_size=1, delay=0) as miner:
        miner.helper.sparql_engine = "qlever"
        schema = miner.mine("mixed-fields")
        assert miner.last_report.completion_state == "complete"
    rows = {(p.subject_class, p.property_uri, p.object_class): p for p in schema.patterns}
    link = rows["urn:e:A", "urn:e:link", "urn:e:B"]
    text = rows["urn:e:A", "urn:e:text", "Literal"]
    ref = rows["urn:e:A", "urn:e:ref", "Resource"]
    assert (link.count, link.distinct_subjects, link.distinct_objects) == (2, 2, 1)
    assert (text.count, text.distinct_subjects, text.distinct_objects) == (3, 2, 2)
    assert ref.count == 1
    assert not schema.structural_patterns, "Typed discovery covers every subject edge"
