"""A stage in which some sources fail is partial: the next stages still run and the run ends
without an error. Only a stage that produces nothing stops the pipeline and fails the run."""

from scripts.pipeline_stages.base import Stage
from scripts.pipeline_stages.cli import Pipeline, exit_code
from scripts.pipeline_stages.config import PipelineConfig


class Partial(Stage):
    name = "partial"

    def _execute(self):
        return {"mined": ["a"], "failed": [{"name": "b", "error": "endpoint down"}]}


class Failed(Stage):
    name = "failed"

    def _execute(self):
        return {"mined": [], "failed": [{"name": "b", "error": "endpoint down"}]}


class After(Stage):
    name = "after"

    def _execute(self):
        return {"mined": ["c"]}


def _config(tmp_path):
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(base_dir=tmp_path, repo_dir=tmp_path, sources_file=registry, output_dir=tmp_path / "run")
    config.output_dir.mkdir()
    return config


def test_a_partial_stage_does_not_stop_the_run(tmp_path):
    results = Pipeline(_config(tmp_path)).add_stage(Partial).add_stage(After).run()
    assert results["partial"]["state"] == "partial" and results["partial"]["success"] is False
    assert results["after"]["state"] == "complete", "The next stage runs after a partial one"
    assert exit_code(results) == 0, "A partial run is reported as partial, not as a failure"


def test_a_failed_stage_stops_the_run(tmp_path):
    results = Pipeline(_config(tmp_path)).add_stage(Failed).add_stage(After).run()
    assert results["failed"]["state"] == "failed"
    assert "after" not in results
    assert exit_code(results) == 1
