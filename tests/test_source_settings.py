import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from rdfsolve.config import mint


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


def test_grouped_mining_mines_all_graphs_once_and_splits_per_dataset(
    pipeline, tmp_path, monkeypatch
):
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    one, two = (mint("graph", "one"), mint("graph", "two"))
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
