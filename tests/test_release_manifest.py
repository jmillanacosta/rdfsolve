import json
from pathlib import Path

import yaml
import pytest

from rdfsolve.release import build_release_manifest, inventory_artifacts, summarize_release, write_release_manifest


def _fixture(root: Path) -> None:
    (root / "sources.yaml").write_text(
        yaml.safe_dump({
            "sources": [
                {
                    "name": "demo",
                    "endpoint": "https://example.org/sparql",
                    "download_ttl": ["https://example.org/demo.ttl"],
                    "graph_uris": ["https://example.org/graph"],
                }
            ]
        }),
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
        json.dumps({"schema": {"about": {"source_version": "1.2", "source_version_iri": "https://example.org/v1.2"}}}),
        encoding="utf-8",
    )
    (d / "demo_remote_ontology_acquisition.json").write_text(
        json.dumps({
            "dataset_id": "demo",
            "candidates": [
                {
                    "namespace": "http://purl.obolibrary.org/obo/CHEBI_",
                    "ontology_id": "chebi",
                    "identity_basis": "reference_source",
                    "observed_classes": ["http://purl.obolibrary.org/obo/CHEBI_1"],
                    "observed_properties": [],
                    "provider_graphs": [],
                    "reference_sources": [{"source_url": "https://purl.obolibrary.org/obo/chebi.owl"}],
                }
            ],
        }),
        encoding="utf-8",
    )


def test_release_builder_is_stable_and_excludes_its_own_outputs(tmp_path: Path):
    _fixture(tmp_path)
    first = build_release_manifest(tmp_path, release_id="test", rdfsolve_version="0.3.0")
    write_release_manifest(first, tmp_path)
    second = build_release_manifest(tmp_path, release_id="test", rdfsolve_version="0.3.0")
    assert [(a.path, a.sha256) for a in first.artifacts] == [(a.path, a.sha256) for a in second.artifacts]
    assert all(a.path != "release.json" for a in second.artifacts)
    assert second.datasets[0].source_version == "1.2"
    assert second.datasets[0].ontology_usages[0].ontology_id == "chebi"
    summary = summarize_release(second)
    assert summary["registry_entries"] == 1
    assert summary["completion"] == {"complete": 1}
    assert summary["ontology_identity_basis"] == {"reference_source": 1}


def test_inventory_does_not_recurse_on_release_files(tmp_path: Path):
    (tmp_path / "release.json").write_text("{}")
    (tmp_path / "release.ttl").write_text("")
    (tmp_path / "x.txt").write_text("x")
    assert [a.path for a in inventory_artifacts(tmp_path)] == ["x.txt"]


def test_release_reports_identity_review_state_from_frozen_inputs(tmp_path: Path):
    (tmp_path / "sources.yaml").write_text(
        yaml.safe_dump({"sources": [
            {"name": "a", "endpoint": "https://example.org/sparql"},
            {"name": "b", "endpoint": "https://example.org/sparql"},
        ]}), encoding="utf-8"
    )
    manifest = build_release_manifest(tmp_path, release_id="pilot")
    assert manifest.identity_review_complete is False
    assert manifest.identity_candidate_count == 1
    assert manifest.canonical_dataset_count is None
    assert manifest.identity_review_error is None

    (tmp_path / "identity_overrides.yaml").write_text(
        yaml.safe_dump([{"left": "a", "right": "b", "relation": "same_dataset"}]),
        encoding="utf-8",
    )
    reviewed = build_release_manifest(tmp_path, release_id="reviewed")
    assert reviewed.identity_review_complete is True
    assert reviewed.identity_candidate_count == 0
    assert reviewed.canonical_dataset_count == 1
    assert reviewed.identity_overrides_artifact is not None
    summary = summarize_release(reviewed)
    assert summary["registry_entries"] == 2
    assert summary["canonical_dataset_count"] == 1


@pytest.mark.parametrize("state", ["complete", "partial", "failed", "unfinished"])
def test_release_keeps_attempt_completion(tmp_path, state):
    _fixture(tmp_path)
    report = {"completion_state": state, "finished_at": None if state == "unfinished" else "2026-09-22T00:00:00Z"}
    (tmp_path / "demo/demo_remote_report.json").write_text(json.dumps(report))
    manifest = build_release_manifest(tmp_path)
    assert manifest.datasets[0].completion_state == state
    assert manifest.datasets[0].extractions[0].completion_state == state
    assert summarize_release(manifest)["attempted_completion"] == {state: 1}


def test_release_does_not_treat_provisional_failure_as_terminal(tmp_path):
    _fixture(tmp_path)
    (tmp_path / "demo/demo_remote_report.json").write_text(json.dumps({
        "completion_state": "failed", "finished_at": None, "abort_reason": "required query incomplete"
    }))
    (tmp_path / "demo/demo_local_report.json").write_text(json.dumps({"completion_state": "complete"}))
    dataset = build_release_manifest(tmp_path).datasets[0]
    assert dataset.completion_state == "unfinished"
    assert {r.mode: r.completion_state for r in dataset.extractions} == {
        "local": "complete", "remote": "unfinished"
    }
