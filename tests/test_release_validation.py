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
