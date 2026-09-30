"""With --clean-service-data, a pipeline stage removes the engine and service data of an endpoint
from a mined schema before the schema, its views and its paths are written (the owner decision
of 2026-09-30). Without the option, nothing is removed."""

import sys

import pytest

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from scripts.pipeline_stages import cli
from scripts.pipeline_stages.base import Stage
from scripts.pipeline_stages.config import PipelineConfig

V = "http://www.openlinksw.com/schemas/virtrdf#"
SCHEMA = MinedSchema(
    about=AboutMetadata.build(dataset_name="x"),
    patterns=[
        SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="Literal"),
        SchemaPattern(subject_class=V + "QuadMap", property_uri=V + "item", object_class="Literal"),
    ],
)


class Any_(Stage):
    name = "any"

    def _execute(self):
        return {}


def _stage(tmp_path, clean):
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(
        base_dir=tmp_path, repo_dir=tmp_path, sources_file=registry, output_dir=tmp_path / "run",
        clean_service_data=clean,
    )
    return Any_(config)


def test_the_stage_removes_service_data_when_asked(tmp_path):
    cleaned = _stage(tmp_path, True)._without_service_data(SCHEMA)
    assert [p.property_uri for p in cleaned.patterns] == ["urn:p"]
    assert cleaned.about.cleaned["patterns_removed"] == 1
    kept = _stage(tmp_path, False)._without_service_data(SCHEMA)
    assert kept is SCHEMA and kept.about.cleaned is None


def test_the_option_is_read_from_the_command_line(monkeypatch):
    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--clean-service-data"])
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].clean_service_data is True
