"""With --restriction-patterns, a pipeline stage mines the relations that the data states with
OWL restrictions and writes them into the schema file. The scope is the data graphs of the
schema together with its ontology graphs, where the restrictions of a source are loaded."""

import json
import sys

import pytest
from rdflib import Graph

from rdfsolve import SchemaMiner
from rdfsolve.schema_models import AboutMetadata, MinedSchema
from scripts.pipeline_stages import cli
from scripts.pipeline_stages.base import Stage, restriction_scope
from scripts.pipeline_stages.config import PipelineConfig
from tests.test_restriction_patterns import DATA


class Any_(Stage):
    name = "any"

    def _execute(self):
        return {}


def test_the_stage_writes_the_restriction_patterns(tmp_path):
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(
        base_dir=tmp_path, repo_dir=tmp_path, sources_file=registry, output_dir=tmp_path / "run",
        restriction_patterns=True, navigation_hops=0, output_formats=["json"],
    )
    schema = MinedSchema(about=AboutMetadata.build(dataset_name="x"), patterns=[])
    with SchemaMiner.from_graph(Graph().parse(data=DATA, format="turtle"), delay=0) as miner:
        Any_(config)._save_schema_outputs(schema, tmp_path, "x", "_local", helper=miner.helper)
    written = json.loads((tmp_path / "x_local_schema.json").read_text())
    written = written.get("schema", written)
    assert written["restriction_patterns"]["state"] == "complete"
    assert len(written["restriction_patterns"]["patterns"]) == 5


def test_the_scope_has_the_data_graphs_and_the_ontology_graphs():
    about = AboutMetadata.build(dataset_name="x").model_copy(
        update={"graph_uris": ["urn:g:data"], "ontology_graph_uris": ["urn:g:ontology"]}
    )
    assert restriction_scope(about) == ["urn:g:data", "urn:g:ontology"]
    assert restriction_scope(AboutMetadata.build(dataset_name="x")) is None, "The whole dataset"


def test_the_option_is_read_from_the_command_line(monkeypatch):
    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--restriction-patterns"])
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].restriction_patterns is True
