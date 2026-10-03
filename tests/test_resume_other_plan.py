"""A resumed run reuses each completed batch of the earlier run whose classes are all classes of
this run, and plans only the other classes, also when its plan differs: a pattern row belongs to
one subject class. PubChem run 6 was planned in fixed batches of 15 classes after its weight
count was refused; a run with weighted batches then keeps its completed batches."""

import json

import pytest
from rdflib import Graph

from rdfsolve import SchemaMiner

DATA = '@prefix e: <urn:stop:> . e:a a e:A; e:p "x" . e:b a e:B; e:q e:a . e:c a e:C; e:p "y" .'


def test_completed_batches_are_reused_under_another_plan(tmp_path, monkeypatch):
    graph = Graph().parse(data=DATA, format="turtle")
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
    kept = tmp_path / "previous.checkpoint.jsonl"
    kept.write_text(path.with_suffix(".checkpoint.jsonl").read_text())
    (cls,) = json.loads(kept.read_text().splitlines()[0])["classes"]
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=2, report_path=path,
                                resume_checkpoint=kept) as miner:
        select, sent = miner.helper.select, []
        monkeypatch.setattr(
            miner.helper, "select", lambda q, **kw: sent.append((kw.get("purpose", ""), q)) or select(q, **kw)
        )
        resumed = miner.mine("stop")
        report = miner.last_report
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=2) as fresh:
        whole = fresh.mine("stop")
    assert sorted(map(repr, resumed.patterns)) == sorted(map(repr, whole.patterns))
    assert not [q for p, q in sent if p.startswith("two-phase/") and f"<{cls}>" in q], "Reused, not re-queried"
    assert report.config["resumed_batches"] == [[cls]]
    assert report.completion_state == "complete"
