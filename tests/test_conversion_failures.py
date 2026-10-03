"""A failed conversion of a download stops the download step with a message, so that a file is
not left out of the index without notice (2026-09-30: without rapper and Java on the compute
nodes, the RDF/XML of ChEBI and the OBO of Cellosaurus would have been dropped silently)."""

import os
import subprocess

from rdfsolve.qlever.utils import _convert_obo_steps, _convert_rdfxml_steps


def _run(steps, tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("rapper", "java"):
        (bin_dir / tool).write_text("#!/bin/sh\necho broken >&2\nexit 1\n")
        (bin_dir / tool).chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    return subprocess.run(
        ["bash"], input=" && ".join(steps), text=True, cwd=tmp_path, env=env, capture_output=True
    )


def test_a_failed_rdfxml_conversion_stops_the_step(tmp_path):
    (tmp_path / "data.owl").write_text("<rdf/>")
    done = _run(_convert_rdfxml_steps(), tmp_path)
    assert done.returncode != 0 and "Conversion failed: data.owl" in done.stderr
    assert not (tmp_path / "data.nq").exists()


def test_a_failed_obo_conversion_stops_the_step(tmp_path):
    (tmp_path / "terms.obo").write_text("format-version: 1.2\n")
    (tmp_path / "robot.jar").write_text("")
    done = _run(_convert_obo_steps(), tmp_path)
    assert done.returncode != 0 and "Conversion failed: terms.obo" in done.stderr
    assert not (tmp_path / "terms.ttl").exists()


def test_turtle_in_an_owl_file_is_indexed_as_turtle(tmp_path):
    """GlyCosmos publishes glycovid/sugarbind/ontology.owl in Turtle: it is named .ttl, not
    given to the RDF/XML converter (which refused it and stopped the build, 2026-09-30)."""
    (tmp_path / "ontology.owl").write_text("@prefix : <urn:x#> .\n:a a :B .\n")
    (tmp_path / "model.owl").write_text('<?xml version="1.0"?>\n<rdf:RDF/>\n')
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "rapper").write_text('#!/bin/sh\necho "<urn:s> <urn:p> <urn:o> <urn:g> ."\n')
    (bin_dir / "rapper").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    done = subprocess.run(
        ["bash"], input=" && ".join(_convert_rdfxml_steps()), text=True, cwd=tmp_path, env=env,
        capture_output=True,
    )
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "ontology.ttl").read_text().startswith("@prefix")
    assert not (tmp_path / "ontology.nq").exists() and (tmp_path / "model.nq").exists()


def test_an_empty_file_is_skipped(tmp_path):
    """UniProt publishes enzyme-hierarchy.rdf.xz empty (release 2026-09-03): an empty file has no
    statements, and the converter's "Premature end of file" stopped the Swiss-Prot build."""
    (tmp_path / "enzyme-hierarchy.rdf").write_text("")
    done = _run(_convert_rdfxml_steps(), tmp_path)
    assert done.returncode == 0, done.stderr
    assert not (tmp_path / "enzyme-hierarchy.nq").exists()
