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
    assert words[:1] == ["qlever-index"] and words[-4:] == [
        "2>&1",
        "|",
        "tee",
        "wikipathways.index-log.txt",
    ]
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
        .endswith(
            "gzip -dc rdf/a.nt.gz > .index-pipes/0.nt || echo rdf/a.nt.gz >> .index-feed.failed\n"
        )
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


def test_quads_after_a_long_run_of_triples_make_an_n_quads_input(tmp_path):
    import gzip

    data = tmp_path / "proteinatlas.0.nt.gz"
    with gzip.open(data, "wt") as stream:
        stream.writelines(f"<urn:s{i}> <urn:p> <urn:o> .\n" for i in range(2501))
        stream.write("<urn:a> <urn:p> <bad iri> .\n")
        stream.write("<urn:a> <urn:p> <urn:o> <urn:g> .\n")
    assert qlever_format(data) == "nq", "Quads begin at line 2,502 (RDF Portal HPA)"


def test_a_statement_over_the_parser_buffer_is_found_and_the_buffer_grows(tmp_path):
    """qlever-index fails on a statement longer than its parser buffer; the log tells, and the buffer is asked eight times larger."""
    from rdfsolve.qlever.index_check import larger_buffer, statement_over_buffer

    log = tmp_path / "build.log"
    assert not statement_over_buffer(log)
    log.write_text(
        "2026-10-07 14:58:49.027 - ERROR: Creating the index for QLever failed with the "
        "following exception: The regex ([\\r\\n]+) which marks the end of a statement was not "
        "found in the current input batch (that was not the last one) of size 10,000,000\n"
    )
    assert statement_over_buffer(log)
    assert larger_buffer("10M") == "80M" and larger_buffer("1G") == "2048M"
    assert larger_buffer("2048M") is None and larger_buffer("lots") is None


def test_census_reads_a_statement_over_the_parser_buffer(tmp_path):
    """The datatype census reads a statement longer than pyoxigraph's buffer and
    counts its literal's datatype, instead of failing."""
    import gzip

    from rdfsolve.graph_export import OXIGRAPH_BUFFER
    from rdfsolve.qlever.datatypes import census_of_files

    path = tmp_path / "x.nt.gz"
    with gzip.open(path, "wt") as stream:
        stream.write(
            '<http://e/a> <http://e/n> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
        )
        stream.write(
            f'<http://e/a> <http://e/n> "{"9" * OXIGRAPH_BUFFER}"'
            "^^<http://www.w3.org/2001/XMLSchema#integer> .\n"
        )
    found = census_of_files([(path, "nt")])
    assert found["properties"]["http://e/n"] == {"http://www.w3.org/2001/XMLSchema#integer": 2}
    assert found["unread"]["lines"] == 0


def test_too_many_inputs_share_pipes_with_their_blank_nodes_kept_apart(tmp_path, monkeypatch):
    from rdfsolve.qlever import inputs

    monkeypatch.setattr(inputs, "ARG_BUDGET", 60)
    monkeypatch.setattr(inputs, "SHARED_PIPES", 2)
    (tmp_path / "rdf").mkdir()
    names = ["a.nt.gz", "b.nt", "c.nt.gz", "d.ttl"]
    for n in names:
        (tmp_path / "rdf" / n).write_text("")
    index_command(
        Path("/data/qlever.sif"),
        tmp_path,
        tmp_path,
        "t",
        tmp_path / "s.json",
        [(tmp_path / "rdf" / n, "") for n in names],
        parallel="false",
        buffer="10M",
        memory="1G",
    )
    script = (tmp_path / "index-command.sh").read_text()
    assert "-f .index-pipes/0.nt -F nt -f .index-pipes/1.nt -F nt -f rdf/d.ttl -F ttl" in script
    feed = (tmp_path / FEED).read_text()
    assert "relabel() {" in feed
    assert (
        "{\ngzip -dcf rdf/a.nt.gz | relabel f0_ || echo rdf/a.nt.gz >> .index-feed.failed\n"
        "gzip -dcf rdf/b.nt | relabel f1_ || echo rdf/b.nt >> .index-feed.failed\n"
        "} > .index-pipes/0.nt\n"
    ) in feed


def test_too_many_turtle_inputs_are_refused(tmp_path, monkeypatch):
    import pytest

    from rdfsolve.qlever import inputs

    monkeypatch.setattr(inputs, "ARG_BUDGET", 60)
    with pytest.raises(ValueError, match="convert them to N-Triples"):
        index_command(
            Path("/data/qlever.sif"),
            tmp_path,
            tmp_path,
            "t",
            tmp_path / "s.json",
            [(tmp_path / f"{i}.ttl", "") for i in range(3)],
            parallel="false",
            buffer="10M",
            memory="1G",
        )
