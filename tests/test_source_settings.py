"""Check source settings at the pipeline and miner boundaries."""

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from rdfsolve.endpoint_health import get_polite_delay
from rdfsolve.miner import SchemaMiner
from rdfsolve.models.source_model import SourceModel
from rdfsolve.mining.strategy import MiningContext
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy


@pytest.fixture
def pipeline():
    path = Path(__file__).resolve().parents[1] / "scripts" / "pipeline.py"
    spec = importlib.util.spec_from_file_location("rdfsolve_test_pipeline", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("override,expected", [(None, 17.0), (29.0, 29.0)])
def test_remote_pipeline_forwards_source_settings(
    pipeline, tmp_path, monkeypatch, override, expected
):
    source = pipeline.Source.from_dict(
        {
            "name": "test",
            "endpoint": "https://example.org/sparql",
            "graph_uris": ["urn:selected"],
            "timeout": 17.0,
            "delay": 0.0,
            "sparql_engine": "virtuoso",
            "sparql_strategy": "post+raw",
            "last_checked": datetime.now(timezone.utc).isoformat(),
        }
    )
    config = pipeline.PipelineConfig(base_dir=tmp_path, timeout=override)
    factory = Mock(side_effect=RuntimeError("Stop before mining"))
    monkeypatch.setattr("rdfsolve.SchemaMiner", factory)
    pipeline.RemoteMiningStage(config)._mine_single_source(source)
    settings = factory.call_args.kwargs
    assert settings["graph_uris"] == ["urn:selected"]
    assert settings["timeout"] == expected
    assert settings["delay"] == 0.0
    assert settings["sparql_engine"] == "virtuoso"
    assert settings["sparql_strategy"] == "post+raw"
    assert settings["chunk_size"] == config.chunk_size
    assert settings["class_batch_size"] == config.class_batch_size


def test_default_pipeline_timeout_does_not_override_registry(pipeline):
    assert pipeline.PipelineConfig().timeout is None


@pytest.mark.parametrize(
    "value,expected", [(None, []), ("urn:g", ["urn:g"]), (["urn:g"], ["urn:g"])]
)
def test_pipeline_normalizes_graph_scope(pipeline, value, expected):
    assert pipeline.Source.from_dict({"name": "test", "graph_uris": value}).graph_uris == expected


def test_report_records_effective_settings():
    miner = SchemaMiner(
        "https://example.org/sparql",
        graph_uris="urn:selected",
        timeout=17.0,
        delay=0.0,
        sparql_engine="virtuoso",
        sparql_strategy="post+raw",
        source_name="test",
        strategy="one-shot",
        chunk_size=123,
        class_batch_size=7,
        unsafe_paging=True,
        filter_service_namespaces=False,
    )
    miner._init_report("test", miner._build_strategy_string(), "2026-09-07T00:00:00+00:00")
    config = miner._report.report.model_dump()["config"]
    assert config["graph_uris"] == ["urn:selected"]
    assert config["graph_scope"] == "within_named_graphs"
    assert config["timeout"] == 17.0
    assert config["delay"] == 0.0
    assert config["sparql_engine"] == "virtuoso"
    assert config["sparql_strategy"] == "post+raw"
    assert config["strategy"] == "one-shot"
    assert config["class_batch_size"] == 7
    assert config["unsafe_paging"] is True
    assert config["filter_service_namespaces"] is False
    assert config["source_name"] == "test"


def test_empty_scoped_results_do_not_trigger_unscoped_queries():
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    helper = Mock()
    helper.select.side_effect = [
        {"results": {"bindings": [{"class": {"type": "uri", "value": "urn:A"}}]}},
        *[{"results": {"bindings": []}} for _ in range(8)],
    ]
    context = MiningContext(helper, ["urn:selected"], miner._report, Mock())
    assert TwoPhaseStrategy().mine(context) == []
    assert helper.select.call_count == 5
    assert all("urn:selected" in call.args[0] for call in helper.select.call_args_list)


def test_explicit_zero_delay_is_preserved():
    assert (
        get_polite_delay(SourceModel(name="test", endpoint="https://example.org/sparql", delay=0))
        == 0
    )


def test_remote_endpoint_with_local_downloads_still_gets_remote_delay():
    source = SourceModel(name="test", endpoint="https://example.org/sparql", local_provider="test")
    assert get_polite_delay(source) > 0


@pytest.mark.parametrize("mode", ["local", "grouped", "cloud"])
def test_local_mining_uses_local_engine_and_explicit_timeout(pipeline, tmp_path, monkeypatch, mode):
    source = pipeline.Source(name="test", sparql_engine="virtuoso", sparql_strategy="post+raw")
    config = pipeline.PipelineConfig(base_dir=tmp_path, timeout=19.0)
    factory = Mock(side_effect=RuntimeError("Stop before mining"))
    monkeypatch.setattr("rdfsolve.SchemaMiner", factory)
    with pytest.raises(RuntimeError, match="Stop before mining"):
        if mode == "local":
            pipeline.LocalMiningStage(config)._mine_local(source, 7019)
        elif mode == "grouped":
            pipeline.GroupedMiningStage(config)._mine_grouped("test", [source], 7019)
        else:
            pipeline.LsLodCloudStage(config)._mine_cloud([(source, tmp_path)], 7019)
    settings = factory.call_args.kwargs
    assert settings["sparql_engine"] == "qlever"
    assert not settings.get("sparql_strategy")
    assert settings["timeout"] == 19.0
    assert settings["chunk_size"] == config.chunk_size
    assert settings["class_batch_size"] == config.class_batch_size


def test_pattern_fallback_uses_pattern_page_settings(monkeypatch):
    from rdfsolve._outcomes import QueryOutcome

    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    context = MiningContext(
        Mock(), None, miner._report, Mock(), class_chunk_size=3, chunk_size=17, unsafe_paging=True
    )
    fallback = Mock(return_value=QueryOutcome())
    monkeypatch.setattr("rdfsolve.mining.two_phase_strategy.query_with_bisect", fallback)
    TwoPhaseStrategy()._run_phase2_batches(["urn:A"], None, context)
    assert fallback.call_count == 4
    assert all(call.args[6] == 17 for call in fallback.call_args_list)
    assert all(call.kwargs["unsafe_paging"] for call in fallback.call_args_list)


@pytest.mark.parametrize("mode", ["export", "local", "grouped", "cloud"])
def test_pipeline_always_saves_canonical_schema(pipeline, tmp_path, monkeypatch, mode):
    monkeypatch.setattr("rdfsolve.qlever.index_check.verify_named_graphs", Mock())
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    schema = MinedSchema(
        patterns=[
            SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="Resource")
        ],
        about=AboutMetadata(dataset_name="test", graph_uris=["urn:g"]),
    )
    source = pipeline.Source(name="test")
    config = pipeline.PipelineConfig(base_dir=tmp_path, output_formats=[])
    miner = Mock()
    miner.mine.return_value = schema
    miner.last_report.completion_state = "complete"
    monkeypatch.setattr("rdfsolve.SchemaMiner", Mock(return_value=miner))
    if mode == "export":
        pipeline.Stage(config)._save_schema_outputs(schema, tmp_path, "test", "")
        path = tmp_path / "test_schema.json"
    elif mode == "local":
        pipeline.LocalMiningStage(config)._mine_local(source, 7019)
        path = config.output_dir / "test" / "test_schema.json"
    elif mode == "grouped":
        pipeline.GroupedMiningStage(config)._mine_grouped("test", [source], 7019)
        path = config.output_dir / "grouped_test" / "test_schema.json"
    else:
        stage = pipeline.LsLodCloudStage(config)
        monkeypatch.setattr(stage, "_generate_sssom_mappings", Mock())
        stage._mine_cloud([(source, tmp_path)], 7019)
        path = config.output_dir / "lslod_cloud" / "lslod_cloud_schema.json"
    assert MinedSchema.from_json(path) == schema


@pytest.mark.parametrize(
    "field", ["download_ttl", "download_owl", "download_nq", "download_jsonld"]
)
def test_qlever_source_keeps_download_formats(pipeline, field):
    source = pipeline.Source.from_dict({"name": "test", field: "https://example.org/data"})
    assert source.qlever_entry()[field] == "https://example.org/data"
    assert source.download_urls == ["https://example.org/data"]
    assert "download_urls" not in source.qlever_entry()


def test_qlever_source_keeps_tar_scope(pipeline):
    source = pipeline.Source.from_dict(
        {
            "name": "test",
            "local_tar_url": "https://example.org/archive.tar.gz",
            "graph_uris": "urn:g",
        }
    )
    assert source.mode == pipeline.SourceMode.LOCAL
    assert source.qlever_entry()["local_tar_url"] == "https://example.org/archive.tar.gz"
    assert source.qlever_entry()["graph_uris"] == ["urn:g"]


@pytest.mark.parametrize("mode", ["local", "grouped", "cloud"])
def test_existing_qleverfile_is_not_replaced(pipeline, tmp_path, mode):
    config = pipeline.PipelineConfig(base_dir=tmp_path)
    path = tmp_path / "Qleverfile"
    original = b"# Keep this archived source configuration\n"
    path.write_bytes(original)
    source = pipeline.Source(name="offline")
    if mode == "local":
        pipeline.LocalMiningStage(config)._prepare_qleverfile(tmp_path, source, 7019)
    elif mode == "grouped":
        pipeline.GroupedMiningStage(config)._prepare_group_qleverfile(
            tmp_path, "offline", [source], 7019
        )
    else:
        pipeline.LsLodCloudStage(config)._prepare_cloud_qleverfile(tmp_path, [source], 7019)
    assert path.read_bytes() == original


def test_new_group_qleverfile_uses_group_directory(pipeline, tmp_path):
    config = pipeline.PipelineConfig(base_dir=tmp_path)
    source = pipeline.Source.from_dict(
        {"name": "test", "download_ttl": "https://example.org/data.ttl"}
    )
    workdir = tmp_path / "qlever_groups" / "test"
    workdir.mkdir(parents=True)
    pipeline.GroupedMiningStage(config)._prepare_group_qleverfile(workdir, "test", [source], 7019)
    text = (workdir / "Qleverfile").read_text()
    assert str(workdir / "rdf") in text
    assert "qlever_workdirs/test" not in text


def test_input_globs_keep_legacy_and_nested_files(pipeline, tmp_path):
    (tmp_path / "rdf").mkdir()
    nested = tmp_path / "rdf" / "data.ttl"
    legacy = tmp_path / "data.n3"
    nested.touch()
    legacy.touch()
    assert pipeline.LocalMiningStage._qlever_input_files(tmp_path, "rdf/*.ttl rdf/*.n3") == sorted(
        [nested, legacy]
    )


def test_no_download_does_not_run_source_command(pipeline, tmp_path, monkeypatch):
    config = pipeline.PipelineConfig(base_dir=tmp_path, no_download=True)
    (tmp_path / "Qleverfile").write_text(
        "[data]\nFORMAT=ttl\nGET_DATA_CMD=false\n[index]\nINPUT_FILES=rdf/*.ttl\nSETTINGS_JSON={}\n"
    )
    run = Mock(side_effect=AssertionError("Must not launch a command"))
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    with pytest.raises(ValueError, match="No files found"):
        pipeline.LocalMiningStage(config)._execute_qleverfile(
            tmp_path, pipeline.Source(name="test")
        )
    run.assert_not_called()


def test_existing_group_index_needs_no_source_downloads(pipeline, tmp_path, monkeypatch):
    config = pipeline.PipelineConfig(base_dir=tmp_path, no_download=True)
    sources = [pipeline.Source(name="one"), pipeline.Source(name="two")]
    workdir = config.data_dir / "qlever_groups" / "offline"
    workdir.mkdir(parents=True)
    stage = pipeline.GroupedMiningStage(config)
    monkeypatch.setattr(stage, "_has_qlever_index", lambda path, name: path == workdir)
    monkeypatch.setattr(stage, "_identify_groups", lambda _: {"offline": sources})
    monkeypatch.setattr(stage, "_ensure_qlever_image", lambda: None)
    start, stop, mine = Mock(return_value=123), Mock(), Mock()
    monkeypatch.setattr(stage, "_qlever_start", start)
    monkeypatch.setattr(stage, "_qlever_stop", stop)
    monkeypatch.setattr(stage, "_mine_grouped", mine)
    assert stage._execute()["groups_mined"] == ["offline"]
    mine.assert_called_once()
    stop.assert_called_once_with(123)


def test_partial_index_is_not_overwritten(pipeline, tmp_path):
    (tmp_path / "test.index.ops").touch()
    stage = pipeline.LocalMiningStage(pipeline.PipelineConfig(base_dir=tmp_path))
    with pytest.raises(ValueError, match="Incomplete index"):
        stage._has_qlever_index(tmp_path, "test")


def test_registry_rejects_ambiguous_output_names(pipeline, tmp_path):
    registry = tmp_path / "sources.yaml"
    registry.write_text("- name: test\n  endpoint: https://one.test\n- name: test\n  endpoint: https://two.test\n")
    config = pipeline.PipelineConfig(base_dir=tmp_path, sources_file=registry)
    with pytest.raises(ValueError, match="Duplicate source names"):
        config.load_sources(["test"])
    with pytest.raises(ValueError, match="Unknown source names"):
        config.load_sources(["typo"])


@pytest.mark.parametrize("mode", ["remote", "local", "grouped", "cloud"])
def test_pipeline_enables_enrichment_in_every_mining_mode(pipeline, tmp_path, monkeypatch, mode):
    monkeypatch.setattr("rdfsolve.qlever.index_check.verify_named_graphs", Mock())
    config = pipeline.PipelineConfig(base_dir=tmp_path, examples_per_pattern=3)
    source = pipeline.Source(
        name="test",
        endpoint="https://example.org/sparql",
        last_checked=datetime.now(timezone.utc).isoformat(),
    )
    constructor = Mock()
    constructor.return_value.last_report.completion_state = "complete"
    monkeypatch.setattr("rdfsolve.SchemaMiner", constructor)
    monkeypatch.setattr(pipeline.Stage, "_save_schema_outputs", lambda *args: None)
    if mode == "remote":
        pipeline.RemoteMiningStage(config)._mine_single_source(source)
    elif mode == "local":
        pipeline.LocalMiningStage(config)._mine_local(source, 7019)
    elif mode == "grouped":
        pipeline.GroupedMiningStage(config)._mine_grouped("test", [source], 7019)
    else:
        pipeline.LsLodCloudStage(config)._mine_cloud([(source, tmp_path)], 7019)
    assert constructor.call_args.kwargs["enrich"] is True
    assert constructor.call_args.kwargs["examples_per_pattern"] == 3


@pytest.mark.parametrize("mined,state", [([], "failed"), (["ok"], "partial")])
def test_failed_source_prevents_stage_success(pipeline, tmp_path, monkeypatch, mined, state):
    stage = pipeline.RemoteMiningStage(pipeline.PipelineConfig(base_dir=tmp_path))
    monkeypatch.setattr(stage, "_execute", lambda: {"mined": mined, "failed": ["down"]})
    result = stage.run()
    assert result["state"] == state
    assert result["success"] is False


def test_export_failure_keeps_canonical_output_but_fails_stage(pipeline, tmp_path, monkeypatch):
    from rdfsolve.schema_models import AboutMetadata, MinedSchema

    stage = pipeline.Stage(pipeline.PipelineConfig(base_dir=tmp_path, output_formats=["void"]))
    schema = MinedSchema(about=AboutMetadata(dataset_name="test"))
    monkeypatch.setattr(MinedSchema, "to_void_graph", Mock(side_effect=ValueError("bad export")))
    with pytest.raises(RuntimeError, match="bad export"):
        stage._save_schema_outputs(schema, tmp_path, "test", "")
    assert (tmp_path / "test_schema.json").exists()


def test_rdfportal_pubchem_is_not_assigned_to_ftp_group(pipeline, tmp_path):
    stage = pipeline.GroupedMiningStage(pipeline.PipelineConfig(base_dir=tmp_path))
    source = pipeline.Source(
        name="rdfportal.pubchem",
        download_urls=["https://rdfportal.org/ntriples/pubchem/latest/void.nt.gz"],
    )
    assert stage._identify_groups([source]) == {"rdfportal": [source]}
