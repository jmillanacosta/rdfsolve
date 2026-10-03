"""An .n3 download is indexed as Turtle (GtoPdb 2026.3 publishes its data as gtp-rdf.n3, in
N-Triples, and its .ttl files hold only the dataset description; the .n3 file was left out of
the index, 2026-09-30). N3 that uses more than Turtle fails at parsing, not silently."""

from rdfsolve.qlever.inputs import qlever_format, rdf_input_files


def test_n3_files_are_turtle_inputs(tmp_path):
    (tmp_path / "rdf").mkdir()
    data = tmp_path / "rdf" / "gtp-rdf.n3"
    data.write_text('<urn:a> <urn:p> "x" .\n')
    (tmp_path / "rdf" / "gtp.ttl").write_text('<urn:d> <urn:p> "d" .\n')
    assert data in rdf_input_files(tmp_path)
    assert qlever_format(data) == "ttl"
