"""Each count of the census is kept in the checkpoint of the run, and a resumed run takes the
counts of the same queries from it instead of asking again: the census of Bgee RO_0002206 took
about 6.5 h, and run 12 failed after it without keeping it (2026-09-30)."""

from types import SimpleNamespace

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner

QUERIES = ["SELECT (COUNT(*) AS ?triples) WHERE { ?s <urn:p> ?o }"]


def _context(resumed):
    lines = []
    report = SimpleNamespace(checkpoint=lambda phase, classes, rows, state="complete": lines.append((phase, classes, rows)))
    return SimpleNamespace(report=report, resumed=resumed), lines


def test_a_count_is_kept_and_taken_again_when_resumed(monkeypatch):
    sent = []

    def select(context, query, purpose, **options):
        sent.append(query)
        return [{"triples": {"value": "7"}}]

    monkeypatch.setattr(structural_strategy, "_select", select)
    context, lines = _context({})
    assert structural_strategy._count(context, QUERIES) == {"triples": 7}
    ((phase, key, rows),) = lines
    assert phase == "census" and key[0].startswith("census|") and rows == [{"triples": 7}]
    again, kept = _context({tuple(key): rows})
    assert structural_strategy._count(again, QUERIES) == {"triples": 7}
    assert len(sent) == 1, "The resumed count is not asked again"
    assert kept == lines, "The count is kept for the next run too"


def test_the_census_lines_of_a_checkpoint_are_read_on_resume(tmp_path):
    import json

    path = tmp_path / "x_report.checkpoint.jsonl"
    path.write_text(
        json.dumps({"phase": "census", "classes": ["census|abc"], "rows": [{"triples": 7}], "state": "complete"})
        + "\n"
    )
    miner = SchemaMiner.__new__(SchemaMiner)
    miner._resume_failed = set()
    miner._rc = SimpleNamespace(report=SimpleNamespace(config={}))
    batches = miner._resumed_batches(str(path), path.read_text())
    assert batches[("census|abc",)] == [{"triples": 7}]
