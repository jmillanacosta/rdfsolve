"""A remote run reuses the class batches of an earlier run (--resume-from), as a local run does:
SIBiLS run 2 failed after 3 h in the structural discovery, with its class batches in the
checkpoint of its report."""

from datetime import datetime, timezone

import rdfsolve
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.remote import RemoteMiningStage


def test_a_remote_run_resumes_from_the_checkpoint_of_an_earlier_run(tmp_path, monkeypatch):
    checkpoint = tmp_path / "run-2" / "fixture" / "fixture_report.checkpoint.jsonl"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_text("")
    received = []

    def miner(*args, **kwargs):
        received.append(kwargs.get("resume_checkpoint"))
        raise RuntimeError("stop after the miner is made")

    monkeypatch.setattr(rdfsolve, "SchemaMiner", miner)
    now = datetime.now(timezone.utc).isoformat()
    source = Source.from_dict({"name": "fixture", "endpoint": "https://example.invalid/sparql", "last_checked": now})
    for resume, expected in ((tmp_path / "run-2", checkpoint), (None, None), (tmp_path / "none", None)):
        config = PipelineConfig(base_dir=tmp_path, output_dir=tmp_path / "run-3", enrich=False, resume_from=resume)
        assert RemoteMiningStage(config)._mine_single_source(source)["status"] == "failed"
        assert received.pop() == expected
