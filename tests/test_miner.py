from rdflib import Graph
from rdfsolve.mining.miner import SchemaMiner


def test_mine_graph_preserves_pattern_kinds():
    graph = Graph().parse(
        data='@prefix e: <urn:kind:> .\n        e:a a e:A; e:link e:b; e:text "value"; e:unknown e:c; e:node [e:p "x"] .\n        e:b a e:B .',
        format="turtle",
    )
    with SchemaMiner.from_graph(graph, counts=False, delay=0) as miner:
        schema = miner.mine("kinds")
    assert {
        p.property_uri: p.pattern_type.value
        for p in schema.patterns
        if p.property_uri.startswith("urn:kind:")
    } == {
        "urn:kind:link": "object_property",
        "urn:kind:unknown": "object_property",
        "urn:kind:text": "datatype_property",
        "urn:kind:node": "blank_node_property",
    }, "Mined pattern kinds"
    assert miner.last_report.completion_state == "complete", "Finished graph mining"
