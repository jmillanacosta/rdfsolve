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


def test_classes_with_identical_members_are_mined_once():
    graph = Graph().parse(
        data='@prefix e: <urn:same:> .\n        e:a a e:A, e:B, e:C; e:p e:t; e:v "1" .\n        e:b a e:A, e:B; e:p e:t .\n        e:t a e:T .',
        format="turtle",
    )
    with SchemaMiner.from_graph(graph, class_batch_size=1, delay=0) as miner:
        miner.helper.sparql_engine, select, sent = "qlever", miner.helper.select, []
        miner.helper.select = lambda q, **kw: sent.append((kw.get("purpose", ""), q)) or select(q, **kw)
        schema = miner.mine("same")
        report = miner.last_report
    rows = {}
    for p in schema.patterns:
        rows.setdefault(p.subject_class, set()).add((p.property_uri, p.object_class, p.count))
    assert rows["urn:same:B"] == rows["urn:same:A"] and rows["urn:same:C"] != rows["urn:same:A"]
    assert report.config["shared_extensions"] == {"urn:same:B": "urn:same:A"}, "State the reuse"
    checked = "two-phase/same-members"
    reused = [p for p, q in sent if p.startswith(("two-phase/", "counts/")) and p != checked and "<urn:same:B>" in q]
    assert not reused, "Identical member sets give identical rows; B is not queried again"
