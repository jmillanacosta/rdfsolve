from unittest.mock import Mock

from rdfsolve.endpoint_health import EndpointHealthCheck
from rdfsolve.source_enrichment import enrich_source


def test_enrichment_returns_observations_without_changing_registry(tmp_path, monkeypatch):
    path = tmp_path / "sources.yaml"
    original = "# Curated\n- name: demo\n  endpoint: https://example.org/sparql\n  notes: keep\n"
    path.write_text(original)
    health = EndpointHealthCheck("https://example.org/sparql", "up", 0.2, "", "2026-09-22")
    monkeypatch.setattr(
        "rdfsolve.source_enrichment.check_endpoint_health", Mock(return_value=health)
    )
    metadata = Mock()
    metadata.project.return_value = {"title": "Demo"}
    query = Mock(side_effect=[metadata, RuntimeError("metadata unavailable")])
    monkeypatch.setattr("rdfsolve.metadata.query_metadata", query)
    complete = enrich_source("demo", health.endpoint_url, sources_file=path)
    partial = enrich_source("demo", health.endpoint_url, sources_file=path)
    assert (complete["dataset_metadata"], complete["enrichment"]["state"]) == (
        {"title": "Demo"},
        "complete",
    ), "Retrieved metadata"
    assert partial["enrichment"]["phases"] == {
        "health": "up",
        "metadata": "failed",
        "void": "skipped",
    }, "Metadata failure does not become success"
    assert partial["enrichment"]["errors"] == {"metadata": "metadata unavailable"}
    assert complete["notes"] == partial["notes"] == "keep", "Curated fields"
    assert path.read_text() == original, "Source specification must not be written"
    assert list(tmp_path.iterdir()) == [path], "Enrichment must not create backup files"
