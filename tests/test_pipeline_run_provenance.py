from pathlib import Path

import pytest
import yaml
from scripts.pipeline_stages.config import PipelineConfig


def test_archive_run_inputs_freezes_registry_config_and_identity_overrides(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    data = repo / "data"
    output = tmp_path / "run"
    data.mkdir(parents=True)
    sources = data / "sources.yaml"
    sources.write_text(
        yaml.safe_dump(
            [
                {
                    "name": "demo",
                    "endpoint": "https://example.org/sparql",
                    "local_provider": "demo",
                },
                {
                    "name": "service",
                    "endpoint": "https://example.org/sparql",
                    "local_provider": "demo",
                    "source_role": "service",
                },
                {
                    "name": "unresolved",
                    "endpoint": "https://example.org/sparql",
                    "local_provider": "demo",
                    "skip_mining": True,
                },
            ]
        ),
        encoding="utf-8",
    )
    (data / "sssom_sources.yaml").write_text("sources: []\n", encoding="utf-8")
    (data / "identity_overrides.yaml").write_text(
        "- left: a\n  right: b\n  relation: distinct\n", encoding="utf-8"
    )
    config = PipelineConfig(base_dir=tmp_path, repo_dir=repo, output_dir=output)
    config.sources_file = sources
    config.sssom_sources_file = data / "sssom_sources.yaml"
    assert [s.name for s in config.load_sources()] == ["demo"], "Eligible selection"
    assert [s.name for s in config.get_remote_sources()] == ["demo"]
    assert [s.name for s in config.get_local_sources()] == ["demo"]
    with pytest.raises(ValueError, match="not eligible for mining"):
        config.load_sources(["service", "unresolved"])
    config.load_sources()
    config.navigation_hops = 6
    config.collect_property_usage_evidence = True
    config.collect_property_value_profiles = True
    config.collect_property_value_histograms = True
    config.collect_declared_artifacts = True
    config.endpoint_status_file = tmp_path / "health.json"
    config.endpoint_status_file.write_text('{"endpoints": {}}')
    config.download_status_file = tmp_path / "downloads.json"
    config.download_status_file.write_text('{"downloads": {}}')
    written = config.archive_run_inputs()
    assert (output / "sources.yaml").read_text() == sources.read_text()
    assert (output / "sssom_sources.yaml").exists()
    assert (output / "identity_overrides.yaml").exists()
    frozen = yaml.safe_load((output / "pipeline_config.yaml").read_text())
    assert frozen["navigation_hops"] == 6
    for flag in (
        "collect_property_usage_evidence",
        "collect_property_value_profiles",
        "collect_property_value_histograms",
        "collect_declared_artifacts",
    ):
        assert frozen[flag] is True
    assert (
        output / "endpoint_status.json"
    ).read_bytes() == config.endpoint_status_file.read_bytes()
    assert (
        output / "download_status.json"
    ).read_bytes() == config.download_status_file.read_bytes()
    assert frozen["selected_sources"] == ["demo"]
    assert frozen["sources_file"] == str(sources)
    assert "pipeline_config.yaml" in written

    from rdfsolve.qlever import index_check
    from scripts.pipeline_stages.config import Source

    config.sources = [
        Source(name="cached", endpoint="https://example.org/sparql"),
        Source(name="remote", endpoint="https://example.org/other"),
        Source(name="service", source_role="service"),
        Source(name="unresolved", skip_mining=True),
    ]
    def cached_index(workdir, name):
        assert workdir == config.data_dir / "qlever_workdirs" / name
        return name != "remote"

    monkeypatch.setattr(index_check, "has_cached_index", cached_index)
    assert [s.name for s in config.get_local_sources()] == ["cached"], "Use cached local inputs and retain scope exclusions"
