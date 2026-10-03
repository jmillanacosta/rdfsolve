"""rdfsolve.qlever.inputs: the input files of an index and its command."""

import shlex
from pathlib import Path

from rdfsolve.qlever.datatypes import _format
from rdfsolve.qlever.inputs import index_command, qlever_format, rdf_input_files


def test_n3_files_are_turtle_inputs(tmp_path):
    (tmp_path / "rdf").mkdir()
    data = tmp_path / "rdf" / "gtp-rdf.n3"
    data.write_text('<urn:a> <urn:p> "x" .\n')
    (tmp_path / "rdf" / "gtp.ttl").write_text('<urn:d> <urn:p> "d" .\n')
    assert data in rdf_input_files(tmp_path)
    assert qlever_format(data) == "ttl"


def test_many_inputs_go_through_a_script(tmp_path):
    workdir = tmp_path / "wikipathways"
    (workdir / "rdf").mkdir(parents=True)
    mapped = [(workdir / "rdf" / f"WP{i}.ttl", "") for i in range(12543)]
    mapped.append((workdir / "rdf" / "graph.nt", "urn:graph"))
    cmd = index_command(
        Path("/data/qlever.sif"),
        tmp_path,
        workdir,
        "wikipathways",
        workdir / "wp.settings.json",
        mapped,
        parallel="false",
        buffer="10M",
        memory="16GB",
    )
    assert len(" ".join(cmd)) < 1000
    script = (workdir / "index-command.sh").read_text()
    words = shlex.split(script.splitlines()[-1])
    assert words[:2] == ["exec", "qlever-index"]
    assert words.count("-f") == 12544 and "rdf/WP0.ttl" in words and "rdf/WP12542.ttl" in words
    assert words[words.index("rdf/graph.nt") + 1 : words.index("rdf/graph.nt") + 5] == [
        "-F",
        "nt",
        "-g",
        "urn:graph",
    ]
    assert cmd[-2:] == ["bash", str(workdir / "index-command.sh")]


def test_n3_is_counted_as_turtle():
    from pyoxigraph import RdfFormat

    assert _format("n3") == RdfFormat.TURTLE
