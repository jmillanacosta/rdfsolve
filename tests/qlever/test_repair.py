"""rdfsolve.qlever.repair: an IRI holding < > or " and an ill-typed number or boolean in an
N-Triples or N-Quads input are written so that QLever reads them, every other line is kept byte
for byte, and each change is recorded."""

import json

from rdfsolve.qlever.repair import REPAIRS_FILE, readable_value, repair_inputs, repair_line

SICI = "<https://doi.org/10.1002/1097-0142(197601)37:1<141::AID-CNCR2820370121>3.0.CO;2-Y>"
XSD = "http://www.w3.org/2001/XMLSchema#"


def test_an_iri_with_angle_brackets_is_encoded():
    line = f"<https://onco.cc/p/> <https://schema.org/citation> {SICI} .\n"
    written = (
        "<https://onco.cc/p/> <https://schema.org/citation> "
        "<https://doi.org/10.1002/1097-0142(197601)37:1%3C141::AID-CNCR2820370121%3E3.0.CO;2-Y> .\n"
    )
    assert repair_line(line) == (written, ["iri"])


def test_a_quad_and_a_typed_literal_are_read():
    line = '<urn:a<b> <urn:p> "1"^^<urn:t> <urn:g> .\n'
    assert repair_line(line) == ('<urn:a%3Cb> <urn:p> "1"^^<urn:t> <urn:g> .\n', ["iri"])


def test_an_ill_typed_number_becomes_a_string():
    line = f'<urn:a> <urn:p> ""^^<{XSD}float> <urn:g>.\n'
    assert repair_line(line) == ('<urn:a> <urn:p> "" <urn:g> .\n', ["literal"])


def test_the_values_that_qlever_reads():
    assert readable_value("1.5e3", XSD + "double") and readable_value("-INF", XSD + "float")
    assert not readable_value("1e400", XSD + "double"), "Out of range"
    assert not readable_value(" 12", XSD + "integer") and not readable_value("yes", XSD + "boolean")
    assert readable_value("", XSD + "int") and readable_value("x", XSD + "dateTime"), (
        "QLever keeps these as text"
    )


def test_other_lines_are_kept_as_they_are():
    for line in (
        '<urn:a>\t<urn:p>\t"x <y" .\n',
        '<urn:a> <urn:p> "a \\" <b"@en .\r\n',
        "<urn:a> <urn:p> _:b1 .\n",
        "<urn:a<b <urn:p> .\n",
        f'<urn:a> <urn:p> "1.5"^^<{XSD}float> .\n',
    ):
        assert repair_line(line) == (line, [])


def test_the_changes_are_recorded(tmp_path):
    data = tmp_path / "onco.nt"
    kept = '<urn:a> <urn:p> "x" .\r\n'
    data.write_bytes((kept + f"<urn:a> <urn:p> {SICI} .\n").encode())
    turtle = tmp_path / "other.ttl"
    turtle.write_text(f"<urn:a> <urn:p> {SICI} .\n")
    changes = repair_inputs(tmp_path, [data, turtle])
    assert [(c["file"], c["line"], c["changes"]) for c in changes] == [("onco.nt", 2, ["iri"])]
    assert data.read_bytes().startswith(kept.encode()), "Unchanged lines keep their bytes"
    record = json.loads((tmp_path / REPAIRS_FILE).read_text())
    assert record["lines"][0]["original"] == f"<urn:a> <urn:p> {SICI} ."
    assert record["lines_by_change"] == {"iri": 1}
    assert turtle.read_text() == f"<urn:a> <urn:p> {SICI} .\n", "Only line-based inputs"


def test_nothing_is_written_without_a_change(tmp_path):
    data = tmp_path / "ok.nt"
    data.write_text("<urn:a> <urn:p> <urn:o> .\n")
    assert repair_inputs(tmp_path, [data]) == []
    assert not (tmp_path / REPAIRS_FILE).exists() and not (tmp_path / "ok.nt.part").exists()
