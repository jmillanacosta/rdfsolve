import json

from rdflib import RDF, Graph
from rdfsolve.mining.miner import SchemaMiner


def test_failed_counts_keep_patterns_and_report_partial(tmp_path, monkeypatch):
    from rdfsolve.sparql_helper import EndpointError

    graph = Graph().parse(data='@prefix e: <urn:count:> . e:a a e:A; e:p "x" .', format="turtle")
    path = tmp_path / "report.json"
    with SchemaMiner.from_graph(graph, delay=0, report_path=path) as miner:
        select = miner.helper.select

        def fail_counts(query, **kwargs):
            if kwargs.get("purpose") == "counts/literal":
                assert json.loads(path.read_text())["completion_state"] != "complete"
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


def test_interrupted_mining_keeps_completed_class_batches(tmp_path, monkeypatch):
    import pytest

    graph = Graph().parse(
        data='@prefix e: <urn:stop:> . e:a a e:A; e:p "x" . e:b a e:B; e:q e:a .', format="turtle"
    )
    path = tmp_path / "report.json"
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=1, report_path=path) as miner:
        select, seen = miner.helper.select, set()

        def stop(query, **kwargs):
            seen.add(kwargs.get("purpose", "").split("/")[1])
            if kwargs.get("purpose", "").startswith("two-phase/typed-object") and "blank-node" in seen:
                raise SystemExit(143)  # SLURM time limit after the first batch
            return select(query, **kwargs)

        monkeypatch.setattr(miner.helper, "select", stop)
        with pytest.raises(SystemExit):
            miner.mine("stop")
    saved = [json.loads(line) for line in path.with_suffix(".checkpoint.jsonl").read_text().splitlines()]
    typed = (str(RDF.type), "Resource")
    expected = {"urn:stop:A": {("urn:stop:p", "Literal"), typed}, "urn:stop:B": {("urn:stop:q", "urn:stop:A"), typed}}
    assert len(saved) == 1 and saved[0]["phase"] == "patterns", "Only the completed batch is kept"
    (cls,) = saved[0]["classes"]
    assert {(r["property_uri"], r["object_class"]) for r in saved[0]["rows"]} == expected[cls]
    assert json.loads(path.read_text())["completion_state"] != "complete"
    checkpoint = path.with_suffix(".checkpoint.jsonl")
    kept = tmp_path / "previous.checkpoint.jsonl"
    kept.write_text(checkpoint.read_text())
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=1, report_path=path,
                                resume_checkpoint=kept) as miner:
        select, sent = miner.helper.select, []
        monkeypatch.setattr(
            miner.helper, "select", lambda q, **kw: sent.append((kw.get("purpose", ""), q)) or select(q, **kw)
        )
        resumed = miner.mine("stop")
        report = miner.last_report
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=1) as fresh:
        whole = fresh.mine("stop")
    assert sorted(map(repr, resumed.patterns)) == sorted(map(repr, whole.patterns))
    assert not [q for p, q in sent if p.startswith("two-phase/") and f"<{cls}>" in q], "Reused, not re-queried"
    assert report.config["resumed_batches"] == [[cls]], "Record which evidence was reused"
    assert len(checkpoint.read_text().splitlines()) == 2, "The new checkpoint holds every batch"
