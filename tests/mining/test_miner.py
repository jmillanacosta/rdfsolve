"""rdfsolve.mining.miner: a mining run, its lifecycle and report, the queries of each phase recorded,
and the type rows that say nothing about the schema left out."""

import json

from rdflib import RDF, Dataset, Graph

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
        miner.helper.select = lambda q, **kw: (
            sent.append((kw.get("purpose", ""), q)) or select(q, **kw)
        )
        schema = miner.mine("same")
        report = miner.last_report
    rows = {}
    for p in schema.patterns:
        rows.setdefault(p.subject_class, set()).add((p.property_uri, p.object_class, p.count))
    assert rows["urn:same:B"] == rows["urn:same:A"] and rows["urn:same:C"] != rows["urn:same:A"]
    assert report.config["shared_extensions"] == {"urn:same:B": "urn:same:A"}, "State the reuse"
    checked = "two-phase/same-members"
    reused = [
        p
        for p, q in sent
        if p.startswith(("two-phase/", "counts/")) and p != checked and "<urn:same:B>" in q
    ]
    assert not reused, "Identical member sets give identical rows; B is not queried again"


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
    assert next(p for p in schema.patterns if p.property_uri == "urn:count:p").count is None
    report = json.loads(path.read_text())
    assert report["completion_state"] == "partial"
    assert report["finished_at"]
    assert report["pattern_count"] == len(schema.patterns)
    assert any(f["purpose"] == "counts/literal" for f in report["query_failures"])


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
            if (
                kwargs.get("purpose", "").startswith("two-phase/typed-object")
                and "blank-node" in seen
            ):
                raise SystemExit(143)  # SLURM time limit after the first batch
            return select(query, **kwargs)

        monkeypatch.setattr(miner.helper, "select", stop)
        with pytest.raises(SystemExit):
            miner.mine("stop")
    saved = [
        json.loads(line) for line in path.with_suffix(".checkpoint.jsonl").read_text().splitlines()
    ]
    typed = (str(RDF.type), "Resource")
    expected = {
        "urn:stop:A": {("urn:stop:p", "Literal"), typed},
        "urn:stop:B": {("urn:stop:q", "urn:stop:A"), typed},
    }
    assert len(saved) == 1 and saved[0]["phase"] == "patterns", "Only the completed batch is kept"
    (cls,) = saved[0]["classes"]
    assert {(r["property_uri"], r["object_class"]) for r in saved[0]["rows"]} == expected[cls]
    assert json.loads(path.read_text())["completion_state"] != "complete"
    checkpoint = path.with_suffix(".checkpoint.jsonl")
    kept = tmp_path / "previous.checkpoint.jsonl"
    kept.write_text(checkpoint.read_text())
    with SchemaMiner.from_graph(
        graph, delay=0, class_batch_size=1, report_path=path, resume_checkpoint=kept
    ) as miner:
        select, sent = miner.helper.select, []
        monkeypatch.setattr(
            miner.helper,
            "select",
            lambda q, **kw: sent.append((kw.get("purpose", ""), q)) or select(q, **kw),
        )
        resumed = miner.mine("stop")
        report = miner.last_report
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=1) as fresh:
        whole = fresh.mine("stop")
    assert sorted(map(repr, resumed.patterns)) == sorted(map(repr, whole.patterns))
    assert not [q for p, q in sent if p.startswith("two-phase/") and f"<{cls}>" in q], (
        "Reused, not re-queried"
    )
    assert report.config["resumed_batches"] == [[cls]], "Record which evidence was reused"
    phases = [json.loads(line)["phase"] for line in checkpoint.read_text().splitlines()]
    assert phases.count("patterns") == 2, "The new checkpoint holds every batch"
    assert set(phases) <= {"patterns", "census"}, "and the census counts"


PHASE_QUERIES_RECORDED_DATA = (
    '<urn:a> a <urn:A>, <urn:B> ; <urn:p> "x" .  <urn:c> a <urn:C> ; <urn:q> <urn:a> .'
)


def test_statistics_and_class_relation_queries_are_recorded(monkeypatch):
    with SchemaMiner.from_graph(
        Graph().parse(data=PHASE_QUERIES_RECORDED_DATA, format="turtle"), delay=0
    ) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        miner.mine("recorded")
        stats = miner.last_report.query_stats
    assert stats["dataset-statistics"].sent >= 3 and stats["dataset-statistics"].failed == 0
    assert stats["class-extensions"].sent == 3, "One query for each class"


TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"
MEMBERSHIP_ROWS_DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> .
<urn:c> a <urn:A> ; <urn:q> "x" .
<urn:B> a <http://www.w3.org/2002/07/owl#Class> .
"""


def test_only_uninformative_type_rows_are_left_out():
    with SchemaMiner.from_graph(
        Dataset().parse(data=MEMBERSHIP_ROWS_DATA, format="turtle"), delay=0
    ) as miner:
        schema = miner.mine("membership")
        report = miner.last_report
    rows = {(p.subject_class, p.object_class) for p in schema.patterns if p.property_uri == TYPE}
    assert ("urn:A", "Resource") not in rows, "The class IRI urn:A has no type"
    assert ("urn:B", OWL_CLASS) in rows, "The class IRI urn:B is declared owl:Class"
    assert schema.about.class_entity_counts["urn:A"] == 2
    assert report.config["membership_rows_left_out"] >= 1
