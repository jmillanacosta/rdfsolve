"""rdfsolve.mining.report_tracking: a run's checkpoint survives its outputs; a resumed run reuses the
completed batches whose classes it still mines, checks reused batches for classes with the same
members, and mines again a batch that ended with an unresolved failure."""

import json

import pytest
from rdflib import Graph

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointError
from scripts.pipeline_stages.config import PipelineConfig
from scripts.pipeline_stages.local import LocalMiningStage

RESUMED_SAME_MEMBERS_DATA = """
<urn:a1> a <urn:A>, <urn:B> ; <urn:p> "x" .  <urn:a2> a <urn:A>, <urn:B> ; <urn:p> "y" .
<urn:c1> a <urn:C> ; <urn:q> <urn:a1> .
"""


def mine(tmp_path, monkeypatch, name, resume=None):
    path = tmp_path / f"{name}.json"
    graph = Graph().parse(data=RESUMED_SAME_MEMBERS_DATA, format="turtle")
    with SchemaMiner.from_graph(
        graph, delay=0, class_batch_size=1, report_path=path, resume_checkpoint=resume
    ) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        schema = miner.mine("same")
        report = miner.last_report
    return schema, report, path.with_suffix(".checkpoint.jsonl")


def test_reused_batches_record_classes_with_the_same_members(tmp_path, monkeypatch):
    fresh, report, checkpoint = mine(tmp_path, monkeypatch, "fresh")
    shared = report.config["shared_extensions"]
    assert sorted(next(iter(shared.items()))) == ["urn:A", "urn:B"]
    kept = tmp_path / "kept.checkpoint.jsonl"
    kept.write_text(checkpoint.read_text())
    resumed, report, _ = mine(tmp_path, monkeypatch, "resumed", resume=kept)
    assert len(report.config["resumed_batches"]) == 3, "Every batch is reused"
    assert report.config.get("shared_extensions") == shared
    assert sorted(map(repr, resumed.patterns)) == sorted(map(repr, fresh.patterns))


def test_the_output_phase_keeps_the_checkpoint(tmp_path):
    data = Graph().parse(data='<urn:s> a <urn:C>; <urn:p> "value" .', format="turtle")
    config = PipelineConfig(
        base_dir=tmp_path, output_dir=tmp_path / "output", enrich=False, navigation_hops=0
    )
    report_path = config.output_dir / "fixture" / "fixture_report.json"
    checkpoint = report_path.with_suffix(".checkpoint.jsonl")
    with SchemaMiner.from_graph(data, report_path=report_path, delay=0) as miner:
        miner.mine("fixture")
        assert checkpoint.is_file(), "The mining writes a checkpoint"
        written = checkpoint.read_text()
        with LocalMiningStage(config)._output_phase(miner, report_path):
            pass
    assert checkpoint.is_file() and checkpoint.read_text() == written


def test_a_new_run_starts_a_new_checkpoint(tmp_path):
    data = Graph().parse(data='<urn:s> a <urn:C>; <urn:p> "value" .', format="turtle")
    report_path = tmp_path / "fixture_report.json"
    checkpoint = report_path.with_suffix(".checkpoint.jsonl")
    checkpoint.write_text('{"phase": "patterns", "classes": ["urn:Old"], "rows": []}\n')
    with SchemaMiner.from_graph(data, report_path=report_path, delay=0) as miner:
        miner.mine("fixture")
    assert "urn:Old" not in checkpoint.read_text()


RESUME_OTHER_PLAN_DATA = (
    '@prefix e: <urn:stop:> . e:a a e:A; e:p "x" . e:b a e:B; e:q e:a . e:c a e:C; e:p "y" .'
)


def test_completed_batches_are_reused_under_another_plan(tmp_path, monkeypatch):
    graph = Graph().parse(data=RESUME_OTHER_PLAN_DATA, format="turtle")
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
    kept = tmp_path / "previous.checkpoint.jsonl"
    kept.write_text(path.with_suffix(".checkpoint.jsonl").read_text())
    (cls,) = json.loads(kept.read_text().splitlines()[0])["classes"]
    with SchemaMiner.from_graph(
        graph, delay=0, class_batch_size=2, report_path=path, resume_checkpoint=kept
    ) as miner:
        select, sent = miner.helper.select, []
        monkeypatch.setattr(
            miner.helper,
            "select",
            lambda q, **kw: sent.append((kw.get("purpose", ""), q)) or select(q, **kw),
        )
        resumed = miner.mine("stop")
        report = miner.last_report
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=2) as fresh:
        whole = fresh.mine("stop")
    assert sorted(map(repr, resumed.patterns)) == sorted(map(repr, whole.patterns))
    assert not [q for p, q in sent if p.startswith("two-phase/") and f"<{cls}>" in q], (
        "Reused, not re-queried"
    )
    assert report.config["resumed_batches"] == [[cls]]
    assert report.completion_state == "complete"


DATA = '<urn:a> a <urn:A> ; <urn:p> "x" .  <urn:b> a <urn:B> ; <urn:q> "y" ; <urn:r> <urn:a> .'


def mine_partial(path, monkeypatch, *, fail=False, resume=None):
    graph = Graph().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(
        graph, delay=0, class_batch_size=1, report_path=path, resume_checkpoint=resume
    ) as miner:
        select = miner.helper.select

        def refuse(query, *args, purpose="", **kwargs):
            if fail and purpose.startswith("two-phase/literal") and "<urn:B>" in query:
                raise EndpointError("HTTP 500: fixture failure")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", refuse)
        schema = miner.mine("partial")
        return schema, miner.last_report


def rows(schema):
    return sorted((p.subject_class, p.property_uri, p.object_class) for p in schema.patterns)


def test_a_partial_batch_is_mined_again_on_resume(tmp_path, monkeypatch):
    first = tmp_path / "first_report.json"
    _, report = mine_partial(first, monkeypatch, fail=True)
    assert report.completion_state != "complete"
    lines = [
        json.loads(line) for line in first.with_suffix(".checkpoint.jsonl").read_text().splitlines()
    ]
    patterns = [line for line in lines if line["phase"] == "patterns"]
    assert {line["classes"][0]: line["state"] for line in patterns} == {
        "urn:A": "complete",
        "urn:B": "partial",
    }
    whole, _ = mine_partial(tmp_path / "whole_report.json", monkeypatch)
    for kept in (lines, [{k: v for k, v in line.items() if k != "state"} for line in lines]):
        resume = tmp_path / "first_report.checkpoint.jsonl"
        resume.write_text("".join(json.dumps(line) + "\n" for line in kept))
        schema, report = mine_partial(tmp_path / "second_report.json", monkeypatch, resume=resume)
        assert report.config["resumed_batches"] == [["urn:A"]], "Only the complete batch is reused"
        assert report.completion_state == "complete" and rows(schema) == rows(whole)
