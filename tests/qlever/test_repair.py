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


def test_a_compressed_input_is_repaired_compressed(tmp_path):
    import gzip

    data = tmp_path / "onco.nt.gz"
    with gzip.open(data, "wt") as stream:
        stream.write(f"<urn:a> <urn:p> {SICI} .\n")
    assert len(repair_inputs(tmp_path, [data])) == 1
    with gzip.open(data, "rt") as stream:
        assert "%3C141" in stream.read()


def test_an_iri_holding_a_closing_bracket_is_encoded():
    line = (
        "_:b <http://www.w3.org/2000/01/rdf-schema#seeAlso> "
        "<http://ncbi.nlm.nih.gov/snp/rsc.2899A>C> .\n"
    )
    written = (
        "_:b <http://www.w3.org/2000/01/rdf-schema#seeAlso> "
        "<http://ncbi.nlm.nih.gov/snp/rsc.2899A%3EC> .\n"
    )
    assert repair_line(line) == (written, ["iri"])
    for kept in ("<urn:a><urn:p><urn:o>.\n", '<urn:a> <urn:p>"x" .\n', "<urn:a> <urn:p> <urn:o>.\n"):
        assert repair_line(kept) == (kept, []), "Terms written without whitespace are kept"


def test_a_file_without_a_change_is_not_written(tmp_path):
    data = tmp_path / "a.nt"
    data.write_text("<urn:a> <urn:p> <urn:o> .\n")
    before = data.stat().st_mtime_ns
    assert repair_inputs(tmp_path, [data], processes=1) == []
    assert data.stat().st_mtime_ns == before and not list(tmp_path.glob("*.part"))


def test_a_double_below_the_smallest_double_is_written_as_its_zero():
    double = "<http://www.w3.org/2001/XMLSchema#double>"
    for lexical, zero in (("8E-610", "0E0"), ("-8E-610", "-0E0")):
        line = f'_:b <urn:pvalue> "{lexical}"^^{double} .\n'
        assert repair_line(line) == (f'_:b <urn:pvalue> "{zero}"^^{double} .\n', ["rounded"])
    for kept in ("1E-320", "0E0", "0.0E-999", "1.5E0"):
        line = f'_:b <urn:pvalue> "{kept}"^^{double} .\n'
        assert repair_line(line) == (line, []), "QLever reads subnormals and zeros"
    assert not readable_value("8E-610", double[1:-1]) and readable_value("1E-320", double[1:-1])


def test_a_backslash_that_starts_no_escape_is_escaped_in_a_literal_and_encoded_in_an_iri():
    """BioGateway (job 115623): "Dmel\\CG6650" failed the index ("Unsupported escape sequence")
    and <.../Dmel\\CG10011> stopped qlever-index reading the file. The literal keeps its text
    with the backslash escaped; the IRI gets %5C. Valid escapes are kept."""
    gene = "<http://rdf.biogateway.eu/gene/7227/Dmel\\CG10011>"
    label = "<http://www.w3.org/2000/01/rdf-schema#label>"
    assert repair_line(f'<urn:g> {label} "Dmel\\CG6650" .\n') == (
        f'<urn:g> {label} "Dmel\\\\CG6650" .\n',
        ["escape"],
    )
    assert repair_line(f'<urn:g> {label} "a\\qb"@en <urn:graph> .\n') == (
        f'<urn:g> {label} "a\\\\qb"@en <urn:graph> .\n',
        ["escape"],
    )
    assert repair_line(f'<urn:g> <urn:p> "x\\y"^^<{XSD}string> .\n') == (
        f'<urn:g> <urn:p> "x\\\\y"^^<{XSD}string> .\n',
        ["escape"],
    )
    assert repair_line(f"{gene} <urn:p> <urn:o> .\n") == (
        "<http://rdf.biogateway.eu/gene/7227/Dmel%5CCG10011> <urn:p> <urn:o> .\n",
        ["iri"],
    )
    for line in (
        '<urn:a> <urn:p> "tab\\t quote\\" back\\\\ \\u00e9 \\U0001F600" .\n',
        "<urn:a\\u0041> <urn:p> <urn:o> .\n",
        '<urn:a> <urn:p> "a\\\\C" .\n',
    ):
        assert repair_line(line) == (line, []), line


def test_an_escape_repair_is_recorded_with_its_original_line(tmp_path):
    data = tmp_path / "biogateway.nt"
    original = '<urn:g> <urn:symbol> "Dmel\\CG6650" .'
    data.write_text(original + "\n")
    [change] = repair_inputs(tmp_path, [data])
    assert change["changes"] == ["escape"] and change["original"] == original
    assert data.read_text() == '<urn:g> <urn:symbol> "Dmel\\\\CG6650" .\n'
    record = json.loads((tmp_path / REPAIRS_FILE).read_text())
    assert record["lines_by_change"] == {"escape": 1} and "escape" in record["findings"]
