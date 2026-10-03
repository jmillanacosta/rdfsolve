"""The wait between requests (--delay, 1 s) is for public endpoints. A server that the run
starts itself on the node is asked without a wait: lifesciencedict spent more than 2 h on
46,000 queries of 67 ms each, with 1 s between them (job 114415, 2026-09-30)."""

import rdfsolve
from scripts.pipeline_stages.config import PipelineConfig
from scripts.pipeline_stages.local import LocalMiningStage


def test_the_miner_of_a_local_server_does_not_wait(tmp_path, monkeypatch):
    seen = {}

    def miner(**options):
        seen.update(options)
        return object()

    monkeypatch.setattr(rdfsolve, "SchemaMiner", miner)
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(
        base_dir=tmp_path, repo_dir=tmp_path, sources_file=registry, output_dir=tmp_path / "run"
    )
    assert config.delay > 0, "Public endpoints keep their wait"
    LocalMiningStage(config)._local_miner(7000, None, tmp_path / "report.json")
    assert seen["endpoint_url"] == "http://localhost:7000" and seen["delay"] == 0
