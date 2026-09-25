from rdflib import RDF, Dataset, URIRef
from rdfsolve.evidence.observed import collect_property_usage_evidence
from rdfsolve.mining.local_graph import LocalGraphHelper
from rdfsolve.sparql_helper import EndpointTimeoutError


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

    from rdflib import Literal

    g2.add((s1, URIRef("urn:r"), Literal("x")))
    g2.add((s1, URIRef("urn:r"), Literal("y")))
    fields = ("subjects_with_property", "triple_count", "distinct_objects", "node_kind_counts",
              "datatype_counts", "value_count_histogram")

    def evidence(engine, slow=""):
        helper, sent = LocalGraphHelper("local", ds), []
        helper.sparql_engine, select = engine, helper.select

        def record(query, **kw):
            sent.append(kw.get("purpose", ""))
            if slow and slow in query:
                raise EndpointTimeoutError("Operation timed out")
            return select(query, **kw)

        helper.select = record
        found = collect_property_usage_evidence(
            dataset_id="demo", classes=[str(A)], class_entity_counts={str(A): 2}, helper=helper,
            graph_uris=["urn:g1", "urn:g2"], batch_size=1, collect_histograms=True)
        states = {state.status for state in found.batch_states}
        return {r.property_uri: [getattr(r, f) for f in fields] for r in found.records}, sent, states

    batch, _, _ = evidence("generic")
    per_property, sent, _ = evidence("qlever")
    assert per_property == batch, "Property decomposition must not change measurements"
    assert batch["urn:r"][-1] == {"2": 1, "0": 1} and batch[str(p)][-1] == {"1": 1, "0": 1}
    scoped = {kind: [s for s in sent if kind in s] for kind in ("histogram", "literal-profile")}
    assert scoped == {
        "histogram": ["evidence/property-value-count-histogram/property/urn:r"],  # others: one value
        "literal-profile": ["evidence/property-literal-profile/property/urn:r"],  # others: IRIs
    }
    limited, _, states = evidence("qlever", slow="COUNT(DISTINCT ?s)")
    assert limited["urn:r"][:3] == [None, 2, 2] and "partial" in states, "Keep triples, flag subjects"
