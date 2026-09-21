from pathlib import Path

from rdfsolve.release import build_release_manifest
from rdfsolve.release.validate import validate_release


def test_release_validation_detects_hash_change(tmp_path: Path):
    (tmp_path / "x.txt").write_text("before")
    manifest = build_release_manifest(tmp_path, release_id="test")
    assert validate_release(manifest, tmp_path).valid
    (tmp_path / "x.txt").write_text("after")
    result = validate_release(manifest, tmp_path)
    assert not result.valid
    assert any(issue.kind == "checksum" for issue in result.issues)


def test_release_validation_checks_evidence_model_shape(tmp_path: Path):
    import json
    import yaml

    (tmp_path / "sources.yaml").write_text(
        yaml.safe_dump([{"name": "demo"}]), encoding="utf-8"
    )
    dataset = tmp_path / "demo"
    dataset.mkdir()
    (dataset / "demo_remote_report.json").write_text(
        json.dumps({"completion_state": "complete"}), encoding="utf-8"
    )
    (dataset / "demo_remote_schema.json").write_text(
        json.dumps({"schema": {"patterns": []}}), encoding="utf-8"
    )
    # Valid JSON, invalid PropertyUsageCollection: required scope/status fields are absent.
    (dataset / "demo_remote_property_usage.json").write_text(
        json.dumps({"dataset_id": "demo", "records": [{"subject_class": "urn:A"}]}),
        encoding="utf-8",
    )
    manifest = build_release_manifest(tmp_path, release_id="test")
    result = validate_release(manifest, tmp_path, parse_rdf=False)
    assert not result.valid
    assert any(
        issue.kind == "evidence_model" and issue.path.endswith("_property_usage.json")
        for issue in result.issues
    )


def test_release_validation_checks_ontology_artifact_references(tmp_path: Path):
    from rdfsolve.release.model import OntologyUsageReleaseRecord

    (tmp_path / "x.txt").write_text("x")
    manifest = build_release_manifest(tmp_path, release_id="test")
    # Add one release-scoped dataset record directly; this exercises reference
    # integrity independently of the ontology-acquisition parser.
    from rdfsolve.release.model import DatasetReleaseRecord

    manifest.datasets.append(
        DatasetReleaseRecord(
            dataset_id="demo",
            snapshot_id="snapshot:demo:test",
            ontology_usages=[
                OntologyUsageReleaseRecord(
                    namespace="urn:ontology:", ontology_artifact_id="sha256:missing"
                )
            ],
        )
    )
    result = validate_release(manifest, tmp_path, parse_rdf=False)
    assert not result.valid
    assert any(issue.kind == "dangling_ontology_artifact" for issue in result.issues)
