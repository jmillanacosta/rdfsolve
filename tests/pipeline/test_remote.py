"""scripts.pipeline_stages.remote: an endpoint is checked against the local record of its source
first, and a remote run resumes."""

import json
from datetime import datetime, timezone

import pytest
import yaml
from rdflib import Graph

import rdfsolve
from rdfsolve import SchemaMiner
from rdfsolve.schema_models import AboutMetadata, MinedSchema
from scripts.pipeline_stages.cli import Pipeline
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.remote import RemoteMiningStage

DATA = """@prefix e: <urn:ex:> .
e:a a e:A ; e:p e:b . e:b a e:B ."""
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _run(tmp_path, monkeypatch, p_count):
    monkeypatch.chdir(tmp_path)
    graph = Graph().parse(data=DATA, format="turtle")
    monkeypatch.setattr("rdfsolve.SchemaMiner", lambda **kwargs: SchemaMiner.from_graph(graph))
    record = tmp_path / "local" / "fixture" / "fixture_local_schema.json"
    record.parent.mkdir(parents=True)
    about = AboutMetadata.build(
        dataset_name="fixture",
        property_partitions={"urn:ex:p": {"triples": p_count}, TYPE: {"triples": 2}},
    )
    record.write_text(json.dumps(MinedSchema(about=about, patterns=[]).to_dict()))
    registry = tmp_path / "sources.yaml"
    registry.write_text(
        yaml.safe_dump(
            [
                {
                    "name": "fixture",
                    "endpoint": "https://fixture.invalid/sparql",
                    "delay": 0,
                    "last_checked": datetime.now(timezone.utc).isoformat(),
                }
            ]
        )
    )
    config = PipelineConfig(
        base_dir=tmp_path,
        repo_dir=tmp_path,
        sources_file=registry,
        output_dir=tmp_path / "run",
        output_suffix="_remote",
        output_formats=["void"],
        enrich=False,
        delay=0,
        navigation_hops=0,
        no_index=True,
        no_download=True,
        local_records=tmp_path / "local",
    )
    config.load_sources()
    results = Pipeline(config).add_stage(RemoteMiningStage).run()
    out = tmp_path / "run" / "fixture"
    match = json.loads((out / "fixture_remote_endpoint_match.json").read_text())
    return results, match, (out / "fixture_remote_schema.json").exists()


def test_an_endpoint_with_the_local_data_is_not_mined_again(tmp_path, monkeypatch):
    from rdfsolve.release.build import inventory_artifacts

    results, match, mined = _run(tmp_path, monkeypatch, p_count=1)
    assert match["state"] == "equal" and not mined
    assert "fixture" in json.dumps(results)
    roles = {a.role for a in inventory_artifacts(tmp_path / "run")}
    assert "endpoint_match" in roles, "The check is listed in the release"


def test_an_endpoint_with_other_data_is_mined(tmp_path, monkeypatch):
    _, match, mined = _run(tmp_path, monkeypatch, p_count=5)
    assert match["state"] == "differs" and mined


def test_the_cli_takes_the_local_records(monkeypatch, tmp_path):
    import sys

    from scripts.pipeline_stages import cli

    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(
        sys, "argv", ["pipeline.py", "--preflight", "--local-records", str(tmp_path)]
    )
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].local_records == tmp_path


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
    source = Source.from_dict(
        {"name": "fixture", "endpoint": "https://example.invalid/sparql", "last_checked": now}
    )
    for resume, expected in (
        (tmp_path / "run-2", checkpoint),
        (None, None),
        (tmp_path / "none", None),
    ):
        config = PipelineConfig(
            base_dir=tmp_path, output_dir=tmp_path / "run-3", enrich=False, resume_from=resume
        )
        assert RemoteMiningStage(config)._mine_single_source(source)["status"] == "failed"
        assert received.pop() == expected


def test_a_source_mined_void_first_tests_its_paths_within_its_own_budget(tmp_path):
    """--void-first-navigation-budget applies only to sources read from their VoID; the
    choice is made per source when it is mined. Unset, the global budget applies."""
    config = PipelineConfig(base_dir=tmp_path, repo_dir=tmp_path, navigation_budget=1800.0)
    stage = RemoteMiningStage(config)
    assert stage._navigation_budget(True) == stage._navigation_budget(False) == 1800.0
    config.void_first_navigation_budget = 300.0
    assert stage._navigation_budget(True) == 300.0
    assert stage._navigation_budget(False) == 1800.0
