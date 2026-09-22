import json
from pathlib import Path

import yaml
from rdfsolve.release import (
    build_release_manifest,
    summarize_release,
    write_release_manifest,
)


def _fixture(root: Path) -> None:
    (root / "sources.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "name": "demo",
                        "endpoint": "https://example.org/sparql",
                        "download_ttl": ["https://example.org/demo.ttl"],
                        "graph_uris": ["https://example.org/graph"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (root / "code_commit.txt").write_text("abc123\n", encoding="utf-8")
    d = root / "demo"
    d.mkdir()
    (d / "demo_remote_report.json").write_text(
        json.dumps({"completion_state": "complete", "started_at": "2026-09-18T10:00:00Z"}),
        encoding="utf-8",
    )
    (d / "demo_remote_schema.json").write_text(
        json.dumps(
            {
                "schema": {
                    "about": {
                        "source_version": "1.2",
                        "source_version_iri": "https://example.org/v1.2",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (d / "demo_remote_ontology_acquisition.json").write_text(
        json.dumps(
            {
                "dataset_id": "demo",
                "candidates": [
                    {
                        "namespace": "http://purl.obolibrary.org/obo/CHEBI_",
                        "ontology_id": "chebi",
                        "identity_basis": "reference_source",
                        "observed_classes": ["http://purl.obolibrary.org/obo/CHEBI_1"],
                        "observed_properties": [],
                        "provider_graphs": [],
                        "reference_sources": [
                            {"source_url": "https://purl.obolibrary.org/obo/chebi.owl"}
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_release_builder_is_stable_and_excludes_its_own_outputs(tmp_path: Path):
    _fixture(tmp_path)
    first = build_release_manifest(tmp_path, release_id="test", rdfsolve_version="0.3.0")
    write_release_manifest(first, tmp_path)
    second = build_release_manifest(tmp_path, release_id="test", rdfsolve_version="0.3.0")
    assert [(a.path, a.sha256) for a in first.artifacts] == [
        (a.path, a.sha256) for a in second.artifacts
    ]
    assert all((a.path != "release.json" for a in second.artifacts))
    assert second.datasets[0].source_version == "1.2"
    assert second.datasets[0].ontology_usages[0].ontology_id == "chebi"
    summary = summarize_release(second)
    assert summary["registry_entries"] == 1
    assert summary["completion"] == {"complete": 1}
    assert summary["ontology_identity_basis"] == {"reference_source": 1}
    report = tmp_path / "demo/demo_remote_report.json"
    report.write_text(json.dumps({"completion_state": "failed", "finished_at": None}))
    (report.parent / "demo_local_report.json").write_text(
        json.dumps({"completion_state": "complete"})
    )
    dataset = build_release_manifest(tmp_path).datasets[0]
    assert dataset.completion_state == "unfinished", "Provisional failure"
    assert {row.mode: row.completion_state for row in dataset.extractions} == {
        "local": "complete",
        "remote": "unfinished",
    }, "Completion per channel"
