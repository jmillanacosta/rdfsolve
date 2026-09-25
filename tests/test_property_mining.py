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
        select, sent = miner.helper.select, []
        miner.helper.select = lambda query, **kw: sent.append(kw.get("purpose")) or select(query, **kw)
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
    for purpose in ("two-phase/literal", "counts/literal", "two-phase/blank-node"):
        assert f"{purpose}/property/urn:e:link" not in sent, "IRI-only fields need no literal/blank scans"
    for purpose in ("two-phase/typed-object", "two-phase/untyped-uri", "counts/typed-object"):
        assert f"{purpose}/property/urn:e:text" not in sent, "Literal-only fields need no object scans"
    assert "two-phase/literal/property/urn:e:text" in sent and "two-phase/typed-object/property/urn:e:link" in sent

    from rdfsolve.sparql_helper import EndpointTimeoutError

    with SchemaMiner.from_graph(graph, class_batch_size=1, delay=0) as miner:
        miner.helper.sparql_engine, select = "qlever", miner.helper.select

        def slow_subjects(query, **kw):
            if "?subjects" in query and kw.get("purpose", "").startswith("counts/"):
                raise EndpointTimeoutError("Operation timed out")  # distinct subjects too costly
            return select(query, **kw)

        miner.helper.select = slow_subjects
        limited = {(p.subject_class, p.property_uri, p.object_class): p for p in miner.mine("x").patterns}
        assert miner.last_report.completion_state == "partial", "Unmeasured subjects stay visible"
    link, text = limited["urn:e:A", "urn:e:link", "urn:e:B"], limited["urn:e:A", "urn:e:text", "Literal"]
    assert (link.count, link.distinct_subjects, link.distinct_objects) == (2, None, 1)
    assert (text.count, text.distinct_subjects) == (3, None), "Keep triples when subjects time out"
