from unittest.mock import Mock

import pytest
from rdfsolve.sources_updater import _read_sources, _write_sources


def test_registry_write_rejects_changed_files_and_keeps_original_on_failure(tmp_path, monkeypatch):
    path = tmp_path / "sources.yaml"
    path.write_text("- name: original\n")
    original, entries = _read_sources(path)
    path.write_text("- name: user-edit\n")
    with pytest.raises(RuntimeError, match="changed"):
        _write_sources(path, entries, original=original)
    current = path.read_bytes()
    monkeypatch.setattr(
        "rdfsolve.sources_updater.os.replace", Mock(side_effect=OSError("disk error"))
    )
    with pytest.raises(OSError, match="disk error"):
        _write_sources(path, entries, original=current)
    assert path.read_bytes() == current
    assert not list(tmp_path.glob("*.tmp"))
