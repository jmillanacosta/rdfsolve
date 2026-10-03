import json
from pathlib import Path

import yaml
from rdfsolve.release import build_release_manifest, summarize_release


def test_release_links_global_ontology_registry_and_dataset_usage(tmp_path: Path):
    (tmp_path / "sources.yaml").write_text(yaml.safe_dump({"sources": [{"name": "demo"}]}))
    d = tmp_path / "demo"
    d.mkdir()
    (d / "demo_remote_report.json").write_text(json.dumps({"completion_state": "complete"}))
    (d / "demo_remote_schema.json").write_text(json.dumps({"schema": {"patterns": []}}))
    (d / "demo_ontology_acquisition.json").write_text(
        json.dumps(
            {
                "dataset_id": "demo",
                "infrastructure_namespaces": [],
                "candidates": [
                    {
                        "namespace": "http://purl.obolibrary.org/obo/TEST_",
                        "ontology_id": "test",
                        "observed_classes": ["http://purl.obolibrary.org/obo/TEST_1"],
                        "observed_properties": [],
                        "provider_graphs": [],
                        "reference_sources": [{"source_url": "https://example.org/test.owl"}],
                        "identity_basis": "reference_source",
                    }
                ],
            }
        )
    )
    (d / "demo_ontology_usage.json").write_text(
        json.dumps(
            {
                "dataset_id": "demo",
                "usages": [
                    {
                        "dataset_id": "demo",
                        "ontology_artifact_id": "sha256:abc",
                        "ontology_id": "test",
                        "namespace": "http://purl.obolibrary.org/obo/TEST_",
                        "artifact_relation": "reference_release",
                        "version_match_status": "unknown",
                        "resolved_classes": ["http://purl.obolibrary.org/obo/TEST_1"],
                        "unresolved_classes": [],
                        "resolved_properties": [],
                        "unresolved_properties": [],
                        "term_usage": [],
                    }
                ],
            }
        )
    )
    o = tmp_path / "ontologies"
    o.mkdir()
    (o / "registry.json").write_text("{}")
    manifest = build_release_manifest(tmp_path, release_id="test")
    assert all((ds.dataset_id != "ontologies" for ds in manifest.datasets))
    assert manifest.ontology_registry_artifact
    usage = manifest.datasets[0].ontology_usages[0]
    assert usage.ontology_artifact_id == "sha256:abc"
    assert usage.resolved_class_count == 1
    summary = summarize_release(manifest)
    assert summary["assessed_ontology_usages"] == 1
    assert summary["ontology_version_match"] == {"unknown": 1}
