"""The checkpoint of a run survives its output phase, so that --resume-from finds the batches.

Bgee run 13 resumed from run 11 and found no checkpoint: the output phase made a second report
collector for the same report, which removed the checkpoint. Run 13 mined its class batches
and census again (9 h) without notice.
"""

from rdflib import Graph

from rdfsolve.mining.miner import SchemaMiner
from scripts.pipeline_stages.config import PipelineConfig
from scripts.pipeline_stages.local import LocalMiningStage


def test_the_output_phase_keeps_the_checkpoint(tmp_path):
    data = Graph().parse(data='<urn:s> a <urn:C>; <urn:p> "value" .', format="turtle")
    config = PipelineConfig(base_dir=tmp_path, output_dir=tmp_path / "output", enrich=False, navigation_hops=0)
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
