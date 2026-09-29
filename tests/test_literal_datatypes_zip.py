"""A source published as a zip archive (Bgee: rdf_easybgee.zip, 28.5 GB) is counted member by
member from the archive, without extracting it; a member is written ARCHIVE.zip!MEMBER."""

import gzip
import json
import zipfile

from rdfsolve.qlever.datatypes import count_literal_datatypes, input_format, write_census, zip_members

XSD = "http://www.w3.org/2001/XMLSchema#"
MEMBER = "<urn:a> <urn:n> 42 .\n"


def test_members_of_a_zip_archive_are_counted_without_extraction(tmp_path):
    archive = tmp_path / "rdf.zip"
    with zipfile.ZipFile(archive, "w") as out:
        out.writestr("dir/a.ttl", MEMBER)
        out.writestr("b.nt.gz", gzip.compress(f'<urn:b> <urn:f> "1.5E0"^^<{XSD}double> .\n'.encode()))
        out.writestr("README.txt", "not RDF")
    members = zip_members(archive)
    assert [str(m).rsplit("!", 1)[1] for m in members] == ["b.nt.gz", "dir/a.ttl"]
    assert [input_format(m) for m in members] == ["nt", "ttl"]
    counts = count_literal_datatypes([(m, input_format(m)) for m in members])
    assert counts == {"urn:f": {XSD + "double": 1}, "urn:n": {XSD + "integer": 1}}
    write_census(tmp_path / "census.json", counts, members)
    sources = json.loads((tmp_path / "census.json").read_text())["sources"]
    assert sources[1] == {"file": "rdf.zip!dir/a.ttl", "bytes": len(MEMBER)}
    assert sorted(p.name for p in tmp_path.iterdir()) == ["census.json", "rdf.zip"], "Not extracted"
