"""Check source settings at the pipeline and miner boundaries."""

import gzip
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from rdfsolve.config import mint
from rdfsolve.endpoint_health import get_polite_delay
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.strategy import MiningContext
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
from rdfsolve.models.source_model import SourceModel


@pytest.fixture
def pipeline():
    import subprocess
    from importlib import import_module
    from types import SimpleNamespace

    scripts = str(Path(__file__).resolve().parents[1] / "scripts")
    sys.path.insert(0, scripts)
    modules = [
        import_module("pipeline_stages." + name)
        for name in ("config", "base", "remote", "local", "grouped", "cloud", "cli")
    ]
    return SimpleNamespace(
        subprocess=subprocess,
        **{
            name: getattr(module, name)
            for module in modules
            for name in (
                "Source",
                "SourceMode",
                "PipelineConfig",
                "Stage",
                "RemoteMiningStage",
                "LocalMiningStage",
                "GroupedMiningStage",
                "LsLodCloudStage",
                "Pipeline",
                "preflight",
            )
            if hasattr(module, name)
        },
    )


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
        miner.declared_classes = frozenset()
        miner.subsumed_classes = frozenset()
        miner.count_class_entities.return_value = ({}, {})
        pipeline.GroupedMiningStage(config)._mine_grouped("test", [source], 7019)
        path = config.output_dir / "grouped_test" / "test_schema.json"
    else:
        stage = pipeline.LsLodCloudStage(config)
        monkeypatch.setattr(stage, "_save_schema_connectivity", Mock())
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
def test_unbuildable_qleverfile_is_kept(pipeline, tmp_path, mode):
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


@pytest.mark.parametrize("mode", ["local", "grouped"])
def test_stale_qleverfile_is_regenerated_without_an_index(pipeline, tmp_path, mode):
    config = pipeline.PipelineConfig(base_dir=tmp_path)
    path = tmp_path / "Qleverfile"
    path.write_bytes(b"[index]\nINPUT_FILES = rdf/*.ttl\n")
    source = pipeline.Source.from_dict(
        {"name": "offline", "download_nq": "https://example.org/data.nq.gz"}
    )
    if mode == "local":
        pipeline.LocalMiningStage(config)._prepare_qleverfile(tmp_path, source, 7019)
    else:
        pipeline.GroupedMiningStage(config)._prepare_group_qleverfile(
            tmp_path, "offline", [source], 7019
        )
    content = path.read_text()
    assert "INPUT_FILES          = rdf/*.nq" in content
    assert "gunzip" in content


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


def test_cached_archives_are_expanded_before_indexing(pipeline, tmp_path, monkeypatch):
    from rdfsolve.qlever.inputs import rdf_input_files

    config = pipeline.PipelineConfig(base_dir=tmp_path, no_download=True)
    (tmp_path / "Qleverfile").write_text(
        "[data]\nNAME=test\nFORMAT=ttl\nGET_DATA_CMD=false\n"
        "[index]\nINPUT_FILES=rdf/*.ttl\nSETTINGS_JSON={}\nPARALLEL_PARSING=false\n"
    )
    (tmp_path / "rdf").mkdir()
    (tmp_path / "rdf" / "data.ttl.gz").write_bytes(
        gzip.compress(b"<http://a/s> <http://a/p> <http://a/o> .\n")
    )
    indexed = {}

    def run(cmd, **kwargs):
        indexed["files"] = [arg for arg in cmd if str(arg).endswith(".ttl")]
        indexed["present"] = rdf_input_files(tmp_path)
        return Mock(returncode=0)

    monkeypatch.setattr(pipeline.subprocess, "run", run)
    pipeline.LocalMiningStage(config)._execute_qleverfile(tmp_path, pipeline.Source(name="test"))
    assert indexed["files"] == [str(tmp_path / "rdf" / "data.ttl")]
    assert indexed["present"] == [tmp_path / "rdf" / "data.ttl"]
    assert not (tmp_path / "rdf" / "data.ttl").exists()


def test_no_download_does_not_run_source_command(pipeline, tmp_path, monkeypatch):
    config = pipeline.PipelineConfig(base_dir=tmp_path, no_download=True)
    (tmp_path / "Qleverfile").write_text(
        "[data]\nFORMAT=ttl\nGET_DATA_CMD=false\n[index]\nINPUT_FILES=rdf/*.ttl\nSETTINGS_JSON={}\n"
    )
    run = Mock(side_effect=AssertionError("Must not launch a command"))
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    with pytest.raises(ValueError, match="No prepared RDF inputs"):
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
    registry.write_text(
        "- name: test\n  endpoint: https://one.test\n- name: test\n  endpoint: https://two.test\n"
    )
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
    monkeypatch.setattr(pipeline.Stage, "_save_schema_outputs", lambda *args, **kwargs: None)
    if mode == "remote":
        pipeline.RemoteMiningStage(config)._mine_single_source(source)
    elif mode == "local":
        pipeline.LocalMiningStage(config)._mine_local(source, 7019)
    elif mode == "grouped":
        from rdfsolve.schema_models import AboutMetadata, MinedSchema

        miner = constructor.return_value
        miner.mine.return_value = MinedSchema(patterns=[], about=AboutMetadata(dataset_name="test"))
        miner.declared_classes = frozenset()
        miner.subsumed_classes = frozenset()
        miner.count_class_entities.return_value = ({}, {})
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


def test_group_index_reads_nested_inputs_and_forwards_budgets(pipeline, tmp_path, monkeypatch):
    import configparser
    import shutil

    config = pipeline.PipelineConfig(base_dir=tmp_path)
    stage = pipeline.GroupedMiningStage(config)
    source = pipeline.Source(
        name="aopwikirdf", download_fields={"download_ttl": "https://aopwiki.org/data.ttl"}
    )
    workdir = tmp_path / "group"
    workdir.mkdir()
    stage._prepare_group_qleverfile(workdir, "aop-group", [source], 8019)
    settings = configparser.ConfigParser(interpolation=None)
    settings.read(workdir / "Qleverfile")
    settings["index"]["STXXL_MEMORY"] = "32GB"
    settings["index"]["PARSER_BUFFER_SIZE"] = "1GB"
    with (workdir / "Qleverfile").open("w") as stream:
        settings.write(stream)
    source_dir = tmp_path / "aopwikirdf"
    (source_dir / "rdf").mkdir(parents=True)
    target = source_dir / "rdf/aop.ttl"
    shutil.copyfile(Path(__file__).parent / "test_data/aopwikirdf_generated_void.ttl", target)
    run = Mock()
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    stage._execute_group_qleverfile(workdir, "aop-group", [(source, source_dir)])
    command = run.call_args.args[0]
    assert command[command.index("-f") + 1] == str(target)
    assert command[command.index("-F") + 1] == "ttl"
    assert command[command.index("-g") + 1] == "https://w3id.org/rdfsolve/graph/aopwikirdf"
    assert command[command.index("-m") + 1] == "32GB"
    assert command[command.index("-b") + 1] == "1GB"
    run.reset_mock()
    with pytest.raises(ValueError, match="No prepared RDF"):
        stage._execute_group_qleverfile(
            workdir, "aop-group", [(source, source_dir), (source, tmp_path / "missing")]
        )
    run.assert_not_called()


def test_preflight_reports_unusable_inputs(pipeline, tmp_path, monkeypatch):
    import shutil

    config = pipeline.PipelineConfig(base_dir=tmp_path, no_index=True)
    config.sources = [
        pipeline.Source.from_dict(
            {"name": "broken", "download_ttl": "https://example.org/a.ttl.gz"}
        )
    ]
    workdir = config.data_dir / "qlever_workdirs" / "broken" / "rdf"
    workdir.mkdir(parents=True)
    (workdir / "a.ttl.gz").write_bytes(b'<?xml version="1.0"?><html>Object not found!</html>')
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(
        pipeline.LocalMiningStage, "_ensure_qlever_image", lambda self: None, raising=True
    )
    with pytest.raises(FileNotFoundError, match="1 unusable inputs"):
        pipeline.preflight(config, grouped=False, remote=False)


def test_each_source_gets_its_own_port(pipeline, tmp_path, monkeypatch):
    config = pipeline.PipelineConfig(base_dir=tmp_path, base_port=7019)
    config.sources = [
        pipeline.Source.from_dict({"name": name, "download_ttl": "https://example.org/a.ttl"})
        for name in ("one", "two", "three")
    ]
    stage = pipeline.LocalMiningStage(config)
    ports = []

    def start(workdir, name, port):
        ports.append(port)
        if name == "one":
            raise RuntimeError(f"Port {port} is in use")
        return 123

    monkeypatch.setattr(stage, "_ensure_qlever_image", lambda: None)
    monkeypatch.setattr(stage, "_has_qlever_index", lambda path, name: True)
    monkeypatch.setattr(stage, "_qlever_start", start)
    monkeypatch.setattr(stage, "_qlever_stop", Mock())
    monkeypatch.setattr(stage, "_mine_local", Mock())
    results = stage._execute()
    assert ports == [7019, 7020, 7021]
    assert results["mined"] == ["two", "three"]


def test_turtle_served_from_an_owl_url_is_renamed_by_content(pipeline, tmp_path):
    from rdfsolve.qlever import build_qleverfile

    qleverfile = build_qleverfile(
        {"name": "glycoepitope", "download_ttl": "http://example.org/epitopes/glycoepitope.owl"},
        tmp_path,
        7019,
        "singularity",
    )
    assert 'mv -f "glycoepitope.owl" "glycoepitope.ttl"' in qleverfile
    assert "INPUT_FILES          = rdf/*.ttl" in qleverfile


def test_a_turtle_url_is_left_alone(pipeline, tmp_path):
    from rdfsolve.qlever import build_qleverfile

    qleverfile = build_qleverfile(
        {"name": "plain", "download_ttl": "http://example.org/data.ttl"},
        tmp_path,
        7019,
        "singularity",
    )
    assert "mv -f" not in qleverfile


def test_grouped_mining_mines_all_graphs_once_and_splits_per_dataset(
    pipeline, tmp_path, monkeypatch
):
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    one, two = mint("graph", "one"), mint("graph", "two")
    config = pipeline.PipelineConfig(base_dir=tmp_path)
    stage = pipeline.GroupedMiningStage(config)
    sources = [pipeline.Source(name="one"), pipeline.Source(name="two")]
    miners = []
    miner = Mock(declared_classes=frozenset(), subsumed_classes=frozenset())
    miner.count_class_entities.return_value = ({"urn:A": 2}, {"urn:A": "complete"})
    miner.last_report.completion_state = "complete"

    def local_miner(port, graph_uris, report_path):
        miners.append(graph_uris)
        return miner

    # The edge lives in graph one; its object is typed in graph two.
    group = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri="urn:p",
                object_class="urn:B",
                count=3,
                graphs={one: 3},
            ),
            SchemaPattern(
                subject_class="urn:B",
                property_uri="urn:q",
                object_class="Literal",
                count=1,
                graphs={two: 1},
            ),
        ],
        about=AboutMetadata(dataset_name="provider", graph_uris=[one, two]),
    )
    saved = {}
    monkeypatch.setattr(stage, "_local_miner", local_miner)
    monkeypatch.setattr(stage, "_mine_schema", lambda miner, name, output_dir: group)
    monkeypatch.setattr(stage, "_save_schema_outputs", Mock())
    monkeypatch.setattr(
        stage,
        "_save_dataset_outputs",
        lambda source, part, output_dir, helper, context: saved.setdefault(
            source.name, (part, context)
        ),
    )
    assert stage._mine_grouped("provider", sources, 7019) == ["one", "two"]
    assert miners == [[one, two]]
    part, context = saved["one"]
    assert context == "grouped_local_distribution"
    assert [(p.subject_class, p.object_class, p.count) for p in part.patterns] == [
        ("urn:A", "urn:B", 3)
    ]
    assert part.about.dataset_name == "one" and part.about.graph_uris == [one]
    assert part.about.class_entity_counts == {"urn:A": 2}
    miner.count_class_entities.assert_any_call(["urn:A", "urn:B"], [one])
    assert [p.property_uri for p in saved["two"][0].patterns] == ["urn:q"]
