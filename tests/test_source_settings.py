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
