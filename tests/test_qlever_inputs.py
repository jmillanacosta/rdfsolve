"""Check input layouts with an actual AOPWiki RDF export."""

import gzip
import hashlib
import shutil
from pathlib import Path

import pytest

from rdfsolve.qlever.inputs import check_cached_input, rdf_input_files


def test_cached_gzip_checks_real_content_and_rejects_truncation(tmp_path):
    raw = (Path(__file__).parent / "test_data/aopwikirdf_generated_void.ttl").read_bytes()
    path = tmp_path / "aop.ttl.gz"
    compressed = gzip.compress(raw)
    path.write_bytes(compressed)
    result = check_cached_input(path)
    assert result.decoded_bytes == len(raw)
    assert result.decoded_sha256 == hashlib.sha256(raw).hexdigest()
    path.write_bytes(compressed[:-4])
    with pytest.raises(EOFError):
        check_cached_input(path)


def test_input_layouts_do_not_omit_or_duplicate_rdf(tmp_path):
    fixture = Path(__file__).parent / "test_data/aopwikirdf_generated_void.ttl"
    nested = tmp_path / "rdf"
    nested.mkdir()
    copied = nested / fixture.name
    shutil.copyfile(fixture, copied)
    assert rdf_input_files(tmp_path) == [copied]
    alias = tmp_path / fixture.name
    alias.symlink_to(copied)
    assert len(rdf_input_files(tmp_path)) == 1
    alias.unlink()
    shutil.copyfile(fixture, alias)
    with pytest.raises(ValueError, match="Ambiguous"):
        rdf_input_files(tmp_path)
    alias.unlink()
    copied.write_bytes(b"")
    with pytest.raises(ValueError, match="empty"):
        rdf_input_files(tmp_path)
