from pathlib import Path

import yaml
from scripts.pipeline_stages.config import PipelineConfig


def test_archive_run_inputs_freezes_registry_config_and_identity_overrides(tmp_path: Path):
    repo = tmp_path / "repo"
    data = repo / "data"
    output = tmp_path / "run"
    data.mkdir(parents=True)
    sources = data / "sources.yaml"
    sources.write_text("- name: demo\n  endpoint: https://example.org/sparql\n", encoding="utf-8")
    (data / "sssom_sources.yaml").write_text("sources: []\n", encoding="utf-8")
    (data / "identity_overrides.yaml").write_text(
        "- left: a\n  right: b\n  relation: distinct\n", encoding="utf-8"
    )
    config = PipelineConfig(base_dir=tmp_path, repo_dir=repo, output_dir=output)
    config.sources_file = sources
    config.sssom_sources_file = data / "sssom_sources.yaml"
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
