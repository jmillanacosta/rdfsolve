"""rdfsolve.qlever.inputs: the input files of an index and its command."""

import shlex
from pathlib import Path

from rdfsolve.qlever.datatypes import _format
from rdfsolve.qlever.inputs import (
    FEED,
    expand_inputs,
    index_command,
    index_inputs,
    qlever_format,
    rdf_input_files,
)


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
    assert words[:1] == ["qlever-index"] and words[-4:] == ["2>&1", "|", "tee", "wikipathways.index-log.txt"]
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


def test_trig_is_indexed_as_n_quads(tmp_path):
    (tmp_path / "rdf").mkdir()
    trig = tmp_path / "rdf" / "proteinatlas.trig"
    trig.write_text("@prefix : <urn:> .\n:g { :a :p :o . }\n:b :p :o .\n")
    created = expand_inputs(tmp_path)
    converted = tmp_path / "rdf" / "proteinatlas.trig.nq"
    assert created == [converted]
    assert sorted(converted.read_text().splitlines()) == [
        "<urn:a> <urn:p> <urn:o> <urn:g> .",
        "<urn:b> <urn:p> <urn:o> .",
    ]
    assert rdf_input_files(tmp_path) == [converted], "The TriG file itself is not indexed"
    assert qlever_format(converted) == "nq"


def test_compressed_inputs_are_streamed_through_pipes(tmp_path):
    import gzip

    (tmp_path / "rdf").mkdir()
    with gzip.open(tmp_path / "rdf" / "a.nt.gz", "wt") as stream:
        stream.write("<urn:a> <urn:p> _:b .\n")
    (tmp_path / "rdf" / "b.ttl").write_text("<urn:b> <urn:p> <urn:o> .\n")
    with gzip.open(tmp_path / "rdf" / "b.ttl.gz", "wt") as stream:
        stream.write("<urn:b> <urn:p> <urn:o> .\n")
    inputs = index_inputs(tmp_path)
    assert [p.name for p in inputs] == ["a.nt.gz", "b.ttl"], "A plain copy is used when present"
    assert qlever_format(inputs[0]) == "nt"
    index_command(
        Path("/data/qlever.sif"),
        tmp_path,
        tmp_path,
        "t",
        tmp_path / "s.json",
        [(p, "") for p in inputs],
        parallel="false",
        buffer="10M",
        memory="1G",
    )
    script = (tmp_path / "index-command.sh").read_text()
    assert "-f .index-pipes/0.nt -F nt -f rdf/b.ttl -F ttl" in script
    assert (
        (tmp_path / FEED)
        .read_text()
        .endswith("gzip -dc rdf/a.nt.gz > .index-pipes/0.nt || echo rdf/a.nt.gz >> .index-feed.failed\n")
    ), "A failed write goes on to the next pipe, and is recorded"


def test_n_triples_that_name_graphs_are_indexed_as_n_quads(tmp_path):
    import gzip

    quads = tmp_path / "proteinatlas.0.nt.gz"
    with gzip.open(quads, "wt") as stream:
        stream.write("<urn:a> <urn:p> <urn:o> <urn:g> .\n")
    triples = tmp_path / "plain.nt"
    triples.write_text("<urn:a> <urn:p> <urn:o> .\n")
    assert (qlever_format(quads), qlever_format(triples)) == ("nq", "nt")


def test_a_feed_that_fails_and_input_qlever_left_unparsed_are_recorded(tmp_path):
    import gzip

    from rdfsolve.qlever.index_check import unparsed_input
    from rdfsolve.qlever.inputs import FEED_FAILED

    (tmp_path / "rdf").mkdir()
    hidden = tmp_path / "rdf" / "a.nt"
    hidden.write_bytes(gzip.compress(b"<urn:a> <urn:p> <urn:o> .\n"))
    index_command(
        Path("/data/qlever.sif"),
        tmp_path,
        tmp_path,
        "t",
        tmp_path / "s.json",
        [(hidden, "")],
        parallel="false",
        buffer="10M",
        memory="1G",
    )
    script = (tmp_path / "index-command.sh").read_text()
    assert "-f .index-pipes/0.nt -F nt" in script, "gzip data in a .nt file is decompressed"
    assert "2>&1 | tee t.index-log.txt" in script
    assert f"if [ -s {FEED_FAILED} ]" in script.splitlines()[-1]
    assert (tmp_path / FEED).read_text() == (
        f"gzip -dc rdf/a.nt > .index-pipes/0.nt || echo rdf/a.nt >> {FEED_FAILED}\n"
    )
    log = tmp_path / "t.index-log.txt"
    assert unparsed_input(log) == []
    log.write_text(
        "INFO: Parsing of line has Failed, but parseInput is not yet exhausted. Remaining bytes: "
        "936,951,434\nINFO: Triples parsed: 10,000,000\n"
    )
    assert unparsed_input(log) == [936951434]
