import json

from rdflib import Graph
from rdfsolve.mining.miner import SchemaMiner


def test_failed_counts_keep_patterns_and_report_partial(tmp_path, monkeypatch):
    from rdfsolve.sparql_helper import EndpointError

    graph = Graph().parse(data='@prefix e: <urn:count:> . e:a a e:A; e:p "x" .', format="turtle")
    path = tmp_path / "report.json"
    with SchemaMiner.from_graph(graph, delay=0, report_path=path) as miner:
        select = miner.helper.select

        def fail_counts(query, **kwargs):
            if kwargs.get("purpose") == "counts/literal":
                assert json.loads(path.read_text())["completion_state"] == "unfinished"
                raise EndpointError("Count query failed")
            return select(query, **kwargs)

        monkeypatch.setattr(miner.helper, "select", fail_counts)
        schema = miner.mine("count")
    assert schema.patterns
    assert next((p for p in schema.patterns if p.property_uri == "urn:count:p")).count is None
    report = json.loads(path.read_text())
    assert report["completion_state"] == "partial"
    assert report["finished_at"]
    assert report["pattern_count"] == len(schema.patterns)
    assert any((f["purpose"] == "counts/literal" for f in report["query_failures"]))
