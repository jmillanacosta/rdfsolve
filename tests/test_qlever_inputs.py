"""Check input layouts with an actual AOPWiki RDF export."""

import shutil
from pathlib import Path

import pytest

from rdfsolve.qlever.inputs import rdf_input_files


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
