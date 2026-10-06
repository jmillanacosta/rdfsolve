"""rdfsolve.release.build: the release manifest, its validation, command line and dataset kinds."""

import json
from pathlib import Path
import yaml
from rdfsolve.release import (
    build_release_manifest,
    summarize_release,
    write_release_manifest,
)
from rdfsolve.release import build_release_manifest
from rdfsolve.release.model import DatasetReleaseRecord
from rdfsolve.release.validate import validate_release
from rdfsolve.schema_models import MinedSchema
from click.testing import CliRunner
from rdfsolve.cli import main
from rdfsolve.schema_models import AboutMetadata, MinedSchema
from rdfsolve.analysis.release import analyze_release
from rdfsolve.release.build import build_release_manifest, write_release_manifest


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
                    },
                    {
                        "name": "service",
                        "source_role": "service",
                        "endpoint": "https://example.org/sparql",
                    },
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
    assert summary["registry_entries"] == 2
    assert summary["dataset_entries"] == 1
    assert summary["service_records"] == 1
    assert second.service_records == ["service"]
    assert second.canonical_dataset_count == 1, "Services do not create identity candidates"
    assert [d.dataset_id for d in second.datasets] == ["demo"]
    assert any(a.role == "source_registry" for a in second.artifacts), "Service provenance"
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


def test_release_validation_reports_damaged_files_and_references(tmp_path):
    path = tmp_path / "demo_remote_schema.json"
    path.write_text(json.dumps(MinedSchema(about={}).to_dict()))
    manifest = build_release_manifest(tmp_path, release_id="test")
    assert validate_release(manifest, tmp_path).valid, "Intact release"
    path.write_text('{"schema": {"patterns": "broken"}}')
    manifest.datasets.append(
        DatasetReleaseRecord(
            dataset_id="missing", snapshot_id="snapshot:missing", artifacts=["sha256:missing"]
        )
    )
    result = validate_release(manifest, tmp_path)
    assert not result.valid
    assert {(issue.path, issue.kind) for issue in result.issues} == {
        (path.name, "size"),
        (path.name, "checksum"),
        (path.name, "json_model"),
        (None, "dangling_artifact"),
    }, result.model_dump()


def _run(root: Path):
    (root / "sources.yaml").write_text(
        yaml.safe_dump([{"name": "demo", "endpoint": "https://example.org/sparql"}])
    )
    d = root / "demo"
    d.mkdir()
    (d / "demo_remote_report.json").write_text(json.dumps({"completion_state": "complete"}))
    (d / "demo_remote_schema.json").write_text(
        json.dumps(MinedSchema(about=AboutMetadata.build(dataset_name="demo")).to_dict())
    )


def test_release_cli_build_summarize_validate(tmp_path: Path):
    _run(tmp_path)
    runner = CliRunner()
    built = runner.invoke(main, ["release", "build", str(tmp_path), "--release-id", "test"])
    assert built.exit_code == 0, built.output
    assert (tmp_path / "release.json").exists()
    assert (tmp_path / "release.ttl").exists()
    summary = runner.invoke(main, ["release", "summarize", str(tmp_path)])
    assert summary.exit_code == 0
    assert '"registry_entries": 1' in summary.output
    validated = runner.invoke(main, ["release", "validate", str(tmp_path)])
    assert validated.exit_code == 0, validated.output


def test_release_separates_reference_resources_without_dropping_them(tmp_path):
    (tmp_path / "sources.yaml").write_text(
        "- name: observations\n  dataset_kind: instance\n"
        "- name: vocabulary\n  dataset_kind: ontology\n"
        "- name: undecided\n"
        "- name: service\n  source_role: service\n"
    )
    manifest = build_release_manifest(tmp_path)
    write_release_manifest(manifest, tmp_path)
    result = analyze_release(tmp_path)
    assert result["paper_statistics"]["dataset_kinds"] == {
        "instance": 1,
        "ontology": 1,
        "unknown": 1,
    }, "Ontology and unclassified records cannot inflate instance counts"
    assert result["paper_statistics"]["service_records"] == 1
    assert {row["dataset_id"]: row["dataset_kind"] for row in result["dataset_inventory"]} == {
        "observations": "instance",
        "vocabulary": "ontology",
        "undecided": "unknown",
    }
    saved = json.loads((tmp_path / "release.json").read_text())
    assert {row["dataset_kind"] for row in saved["datasets"]} == {
        "instance",
        "ontology",
        "unknown",
    }, "Classification survives release serialization"


def test_the_release_names_the_pinned_inputs_of_a_local_index(tmp_path):
    _fixture(tmp_path)
    pins = {
        "format": "rdfsolve-inputs/1",
        "source": "demo",
        "recorded": "before_index",
        "urls": ["https://example.org/demo.ttl"],
        "release_versions": ["1.2"],
        "downloads": [
            {
                "url": "https://example.org/demo.ttl",
                "path": "rdf/demo.ttl",
                "bytes": 4,
                "sha256": "b" * 64,
                "etag": '"e"',
                "last_modified": "Wed, 02 Sep 2026 14:00:00 GMT",
                "release": {"metalink": "https://example.org/RELEASE.metalink", "version": "1.2"},
                "publisher_check": "match",
            }
        ],
        "files": [{"path": "rdf/demo.ttl", "bytes": 4, "sha256": "b" * 64, "index_input": True}],
    }
    (tmp_path / "demo" / "demo_local_inputs.json").write_text(json.dumps(pins))
    manifest = build_release_manifest(tmp_path, release_id="test")
    dataset = manifest.datasets[0]
    artifact = next(a for a in manifest.artifacts if a.role == "input_manifest")
    assert dataset.input_manifest_artifact == artifact.artifact_id
    assert dataset.input_manifest_path == "demo/demo_local_inputs.json"
    assert (dataset.input_file_count, dataset.input_byte_size) == (1, 4)
    assert dataset.input_release_versions == ["1.2"]
    [download] = dataset.input_downloads
    assert (download.sha256, download.release_version, download.publisher_check) == (
        "b" * 64,
        "1.2",
        "match",
    )
