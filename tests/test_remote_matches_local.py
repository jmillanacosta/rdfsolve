"""With --local-records, the remote stage checks each endpoint against the local record of its
source first. An endpoint that serves the same data is not mined again; the check is written
beside the other outputs. An endpoint that differs is mined as before."""

import json
from datetime import datetime, timezone

import pytest
import yaml
from rdflib import Graph

from rdfsolve import SchemaMiner
from rdfsolve.schema_models import AboutMetadata, MinedSchema
from scripts.pipeline_stages.cli import Pipeline
from scripts.pipeline_stages.config import PipelineConfig
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
    registry.write_text(yaml.safe_dump([{
        "name": "fixture", "endpoint": "https://fixture.invalid/sparql", "delay": 0,
        "last_checked": datetime.now(timezone.utc).isoformat(),
    }]))
    config = PipelineConfig(
        base_dir=tmp_path, repo_dir=tmp_path, sources_file=registry, output_dir=tmp_path / "run",
        output_suffix="_remote", output_formats=["void"], enrich=False, delay=0,
        navigation_hops=0, no_index=True, no_download=True, local_records=tmp_path / "local",
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
    results, match, mined = _run(tmp_path, monkeypatch, p_count=5)
    assert match["state"] == "differs" and mined


def test_the_cli_takes_the_local_records(monkeypatch, tmp_path):
    import sys

    from scripts.pipeline_stages import cli

    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--local-records", str(tmp_path)])
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].local_records == tmp_path
