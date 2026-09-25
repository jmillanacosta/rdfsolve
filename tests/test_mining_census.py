"""Class census preserves RDF set semantics and explicit companion scope."""

from rdflib import Dataset, Graph

from rdfsolve import SchemaMiner
from rdfsolve.mining.query_builders import (
    _build_class_weight_query,
    _build_properties_for_class_query,
)


def test_census_counts_data_subjects_in_each_scope():
    plain = Graph().parse(
        data="""
        <urn:a> a <urn:A>, <urn:B>; <urn:p> "one", "two" .
        <urn:b> a <urn:A>; <urn:p> "three" .
    """,
        format="turtle",
    )
    scoped = Dataset().parse(
        data="""
        <urn:data> { <urn:a> <urn:p> "one", "two" . <urn:b> <urn:p> "three" . }
        <urn:types> { <urn:a> a <urn:A>, <urn:B> . <urn:b> a <urn:A> .
                      <urn:outside> a <urn:A> . }
    """,
        format="trig",
    )
    for data, graphs, context in ((plain, None, None), (scoped, ["urn:data"], ["urn:types"])):
        with SchemaMiner.from_graph(
            data, graph_uris=graphs, type_context_graph_uris=context, delay=0
        ) as miner:
            query = _build_class_weight_query(graphs, context).format(offset=0, limit=100)
            rows = miner.helper.select(query)["results"]["bindings"]
            assert {r["class"]["value"]: int(r["n"]["value"]) for r in rows} == {
                "urn:A": 2,
                "urn:B": 1,
            }, "Counts exclude companion-only subjects and retain multiple types"

            properties = miner.helper.select(
                _build_properties_for_class_query("urn:A", graphs, type_context_graph_uris=context)
            )["results"]["bindings"]
            expected = (
                {"urn:p"}
                if context
                else {"urn:p", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}
            )
            assert {r["p"]["value"] for r in properties} == expected
            assert len(properties) == len(expected), "One row per observed scoped property"
