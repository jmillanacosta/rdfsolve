"""Test registry writes and metadata failures with saved AOPWiki RDF."""

from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from rdflib import Graph

from rdfsolve import enrich_source
from rdfsolve.endpoint_health import EndpointHealthCheck
from rdfsolve.schema_models.metadata import MetadataDocument
from rdfsolve.sources_updater import _read_sources, _write_sources

ENDPOINT = "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql"


@pytest.fixture
def responses(monkeypatch):
    graph = Graph().parse(Path(__file__).parent / "test_data/aopwikirdf_metadata_excerpt.ttl")
    metadata = Mock(return_value=MetadataDocument(graph=graph, endpoint=ENDPOINT))
    health = Mock(return_value=EndpointHealthCheck(
        ENDPOINT, "up", 0.1, "", "2026-09-08T10:00:00+00:00"
    ))
    monkeypatch.setattr("rdfsolve.source_enrichment.check_endpoint_health", health)
    monkeypatch.setattr("rdfsolve.api.query_metadata", metadata)
    return health, metadata


def test_enrichment_preserves_settings_and_makes_distinct_backups(tmp_path, responses):
    path = tmp_path / "sources.yaml"
    initial = [{"name": "aopwikirdf", "endpoint": ENDPOINT, "download_nt": ["https://example.org/data.nt"],
                "graph_uris": ["urn:instance-scope"], "delay": 2, "my_notes": "Keep this"}]
    path.write_text(yaml.safe_dump(initial))
    for _ in range(2):
        entry = enrich_source("aopwikirdf", ENDPOINT, sources_file=path)
    saved = yaml.safe_load(path.read_text())[0]
    assert saved["download_nt"] == initial[0]["download_nt"]
    assert saved["my_notes"] == "Keep this"
    assert saved["graph_uris"] == ["urn:instance-scope"]
    assert saved["delay"] == 2
    assert entry["dataset_metadata"]["source_version"] == "2026.09.05"
    assert "source_issued" not in entry["dataset_metadata"]
    assert saved["enrichment"]["phases"]["void"] == "skipped"
    assert len(list(tmp_path.glob("sources.yaml.*.bak"))) == 2


def test_metadata_failure_keeps_previous_values(tmp_path, responses):
    path = tmp_path / "sources.yaml"
    previous = {"title": "Previously retrieved"}
    path.write_text(yaml.safe_dump([{"name": "aopwikirdf", "endpoint": ENDPOINT,
                                    "dataset_metadata": previous}]))
    responses[1].side_effect = RuntimeError("Response limit exceeded")
    entry = enrich_source("aopwikirdf", ENDPOINT, sources_file=path)
    assert entry["dataset_metadata"] == previous
    assert entry["enrichment"]["state"] == "partial"
    assert "metadata" in entry["enrichment"]["errors"]


def test_duplicate_names_and_endpoint_changes_fail_before_queries(tmp_path, responses):
    path = tmp_path / "sources.yaml"
    for raw in (
        [{"name": "aopwikirdf"}, {"name": "aopwikirdf"}],
        [{"name": "aopwikirdf", "endpoint": "https://other.example/sparql"}],
    ):
        path.write_text(yaml.safe_dump(raw))
        before = path.read_bytes()
        with pytest.raises(ValueError):
            enrich_source("aopwikirdf", ENDPOINT, sources_file=path)
        assert path.read_bytes() == before
    responses[0].assert_not_called()


def test_registry_write_rejects_changed_files_and_keeps_original_on_failure(tmp_path, monkeypatch):
    path = tmp_path / "sources.yaml"
    path.write_text("- name: original\n")
    original, entries = _read_sources(path)
    path.write_text("- name: user-edit\n")
    with pytest.raises(RuntimeError, match="changed"):
        _write_sources(path, entries, original=original)
    current = path.read_bytes()
    monkeypatch.setattr("rdfsolve.sources_updater.os.replace", Mock(side_effect=OSError("disk error")))
    with pytest.raises(OSError, match="disk error"):
        _write_sources(path, entries, original=current)
    assert path.read_bytes() == current
    assert not list(tmp_path.glob("*.tmp"))
