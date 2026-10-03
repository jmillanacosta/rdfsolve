import json

from rdfsolve.release import build_release_manifest
from rdfsolve.release.model import DatasetReleaseRecord
from rdfsolve.release.validate import validate_release
from rdfsolve.schema_models import MinedSchema


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
