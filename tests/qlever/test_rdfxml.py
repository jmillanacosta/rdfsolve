"""rdfsolve.qlever.rdfxml: RDF/XML inputs are converted to N-Triples by pyoxigraph, a dependency,
so a job needs no external converter; rdfsolve.qlever.converters names it and checks it first."""

import os
import subprocess
import sys

import pytest

from rdfsolve.qlever.converters import CONVERTER, PYTHON_VARIABLE, converter_command, preflight
from rdfsolve.qlever.rdfxml import convert_rdfxml, main

DOCUMENT = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://ex.org/">
  <rdf:Description rdf:about="http://ex.org/a">
    <ex:p rdf:resource="relative"/>
    <ex:q><rdf:Description><ex:r xml:lang="en">x</ex:r></rdf:Description></ex:q>
  </rdf:Description>
  <rdf:Description rdf:about="http://ex.org/bad iri"><ex:p>y</ex:p></rdf:Description>
</rdf:RDF>
"""


def test_rdfxml_is_written_as_n_triples_with_relative_iris_resolved_against_the_file(tmp_path):
    source = tmp_path / "data.rdf"
    source.write_text(DOCUMENT)
    target = tmp_path / "data.nt"
    assert convert_rdfxml(source, target) == 4
    lines = target.read_text().splitlines()
    assert (
        f"<http://ex.org/a> <http://ex.org/p> <{source.resolve().as_uri()[:-8]}relative> ." in lines
    )
    assert sum(line.startswith("_:") for line in lines) == 1
    assert any(line.endswith('"x"@en .') for line in lines)
    # Lenient by default, as rapper: a term that RDF excludes is kept and reported later.
    assert '<http://ex.org/bad iri> <http://ex.org/p> "y" .' in lines
    assert not list(tmp_path.glob("*.part"))


def test_strict_conversion_refuses_the_file_and_leaves_no_output(tmp_path, capsys):
    source = tmp_path / "data.owl"
    source.write_text(DOCUMENT)
    assert main(["--strict", str(source), str(tmp_path / "data.nq")]) == 1
    assert "RDF/XML conversion failed" in capsys.readouterr().err
    assert not list(tmp_path.glob("data.nq*")), "No output, complete or partial, is left"


def test_the_qleverfile_command_runs_the_converter_with_the_named_interpreter(tmp_path):
    (tmp_path / "in.rdf").write_text(DOCUMENT)
    command = converter_command("in.rdf", "out.nq")
    assert sys.executable in command, "Without the variable, the interpreter that wrote it"
    done = subprocess.run(
        ["/bin/bash", "-c", command],
        cwd=tmp_path,
        env={**os.environ, PYTHON_VARIABLE: sys.executable, "PATH": "/nonexistent"},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr
    assert len((tmp_path / "out.nq").read_text().splitlines()) == 4


def test_preflight_names_the_converter():
    assert preflight().startswith(CONVERTER)


def test_preflight_fails_with_what_to_do_when_the_converter_does_not_work(monkeypatch):
    import pyoxigraph

    def broken(*args, **kwargs):
        raise ImportError("no pyoxigraph")

    monkeypatch.setattr(pyoxigraph, "parse", broken)
    with pytest.raises(RuntimeError, match="Install rdfsolve with its dependencies"):
        preflight()


def test_a_download_command_that_calls_a_missing_program_is_refused_before_it_runs(
    tmp_path, monkeypatch
):
    """A Qleverfile written before pyoxigraph converted RDF/XML calls rapper; a job without it
    failed half-way with exit 127 (index-uniprot 115047). It is refused first, with what to do."""
    from scripts.pipeline_stages.local import _check_tools

    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(RuntimeError, match=r"rapper.*delete the Qleverfile"):
        _check_tools(
            'cd rdf && rapper -q -i rdfxml -o nquads "$f" > "$nq"', tmp_path / "Qleverfile"
        )
    _check_tools("cd rdf && echo 'Converting RDF/XML -> N-Quads (pyoxigraph)'", tmp_path / "Q")
