"""rdfsolve.qlever.repair: an IRI holding < > or " in an N-Triples or N-Quads input is written
percent-encoded, every other line is kept byte for byte, and each change is recorded."""

import json

from rdfsolve.qlever.repair import REPAIRS_FILE, repair_inputs, repair_line

SICI = "<https://doi.org/10.1002/1097-0142(197601)37:1<141::AID-CNCR2820370121>3.0.CO;2-Y>"


def test_an_iri_with_angle_brackets_is_encoded():
    line = f"<https://onco.cc/p/> <https://schema.org/citation> {SICI} .\n"
    assert repair_line(line) == (
        "<https://onco.cc/p/> <https://schema.org/citation> "
        "<https://doi.org/10.1002/1097-0142(197601)37:1%3C141::AID-CNCR2820370121%3E3.0.CO;2-Y> .\n"
    )


def test_a_quad_and_a_typed_literal_are_read():
    line = '<urn:a<b> <urn:p> "1"^^<urn:t> <urn:g> .\n'
    assert repair_line(line) == '<urn:a%3Cb> <urn:p> "1"^^<urn:t> <urn:g> .\n'


def test_other_lines_are_kept_as_they_are():
    for line in (
        '<urn:a>\t<urn:p>\t"x <y" .\n',
        '<urn:a> <urn:p> "a \\" <b"@en .\r\n',
        "<urn:a> <urn:p> _:b1 .\n",
        "<urn:a<b <urn:p> .\n",
    ):
        assert repair_line(line) == line


def test_the_changes_are_recorded(tmp_path):
    data = tmp_path / "onco.nt"
    kept = '<urn:a> <urn:p> "x" .\r\n'
    data.write_bytes((kept + f"<urn:a> <urn:p> {SICI} .\n").encode())
    turtle = tmp_path / "other.ttl"
    turtle.write_text(f"<urn:a> <urn:p> {SICI} .\n")
    changes = repair_inputs(tmp_path, [data, turtle])
    assert [(c["file"], c["line"]) for c in changes] == [("onco.nt", 2)]
    assert data.read_bytes().startswith(kept.encode()), "Unchanged lines keep their bytes"
    record = json.loads((tmp_path / REPAIRS_FILE).read_text())
    assert record["lines"][0]["original"] == f"<urn:a> <urn:p> {SICI} ."
    assert turtle.read_text() == f"<urn:a> <urn:p> {SICI} .\n", "Only line-based inputs"


def test_nothing_is_written_without_a_change(tmp_path):
    data = tmp_path / "ok.nt"
    data.write_text("<urn:a> <urn:p> <urn:o> .\n")
    assert repair_inputs(tmp_path, [data]) == []
    assert not (tmp_path / REPAIRS_FILE).exists() and not (tmp_path / "ok.nt.part").exists()
