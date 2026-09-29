"""A class batch with an unresolved query failure is checkpointed as partial and is mined again
on resume, so that a resumed run does not report missing patterns as complete (PubChem run 6:
the property listing of one class failed three times). A checkpoint written before the state
was recorded is read with the report beside it: a batch with a class that has a failure there
is mined again."""

import json

from rdflib import Graph

from rdfsolve import SchemaMiner
from rdfsolve.sparql_helper import EndpointError

DATA = '<urn:a> a <urn:A> ; <urn:p> "x" .  <urn:b> a <urn:B> ; <urn:q> "y" ; <urn:r> <urn:a> .'


def mine(path, monkeypatch, *, fail=False, resume=None):
    graph = Graph().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=1, report_path=path, resume_checkpoint=resume) as miner:
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
    _, report = mine(first, monkeypatch, fail=True)
    assert report.completion_state != "complete"
    lines = [json.loads(line) for line in first.with_suffix(".checkpoint.jsonl").read_text().splitlines()]
    assert {line["classes"][0]: line["state"] for line in lines} == {"urn:A": "complete", "urn:B": "partial"}
    whole, _ = mine(tmp_path / "whole_report.json", monkeypatch)
    for kept in (lines, [{k: v for k, v in line.items() if k != "state"} for line in lines]):
        resume = tmp_path / "first_report.checkpoint.jsonl"
        resume.write_text("".join(json.dumps(line) + "\n" for line in kept))
        schema, report = mine(tmp_path / "second_report.json", monkeypatch, resume=resume)
        assert report.config["resumed_batches"] == [["urn:A"]], "Only the complete batch is reused"
        assert report.completion_state == "complete" and rows(schema) == rows(whole)
