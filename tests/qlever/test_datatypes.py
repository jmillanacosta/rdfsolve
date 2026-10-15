"""rdfsolve.qlever.datatypes: the datatypes of literals, in files and zip archives."""

import gzip
import json
import zipfile

from rdfsolve.qlever.datatypes import (
    count_literal_datatypes,
    input_format,
    read_census,
    restore_datatypes,
    write_census,
    zip_members,
)
from rdfsolve.schema_models.pattern import SchemaPattern

XSD = "http://www.w3.org/2001/XMLSchema#"
TURTLE = """@prefix ex: <urn:> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:n 42 ; ex:m "1"^^xsd:nonNegativeInteger ; ex:d 14.1 ; ex:mixed 1 ; ex:s "x" .
"""
TRIPLES = f'<urn:b> <urn:mixed> "2"^^<{XSD}int> .\n<urn:b> <urn:f> "1.5E0"^^<{XSD}double> .\n'


def literal(prop, datatype):
    return SchemaPattern(
        subject_class="urn:A", property_uri=prop, object_class="Literal", datatype=datatype, count=1
    )


def test_the_source_datatype_is_restored_when_a_property_has_one(tmp_path):
    (tmp_path / "a.ttl").write_text(TURTLE)
    with gzip.open(tmp_path / "b.nt.gz", "wt") as out:
        out.write(TRIPLES)
    counts = count_literal_datatypes([(tmp_path / "a.ttl", "ttl"), (tmp_path / "b.nt.gz", "nt")])
    assert counts == {
        "urn:n": {XSD + "integer": 1},
        "urn:m": {XSD + "nonNegativeInteger": 1},
        "urn:d": {XSD + "decimal": 1},
        "urn:mixed": {XSD + "integer": 1, XSD + "int": 1},
        "urn:f": {XSD + "double": 1},
    }, "Numeric literals only, by the datatype of the source"
    write_census(tmp_path / "census.json", counts, [tmp_path / "a.ttl"])
    census = read_census(tmp_path / "census.json")
    patterns = [
        literal("urn:n", XSD + "int"),
        literal("urn:m", XSD + "int"),
        literal("urn:d", XSD + "double"),
        literal("urn:mixed", XSD + "int"),
        literal("urn:f", XSD + "double"),
        literal("urn:s", XSD + "string"),
    ]
    record = restore_datatypes(patterns, census)
    assert [p.datatype.removeprefix(XSD) for p in patterns] == [
        "integer",
        "nonNegativeInteger",
        "decimal",
        "int",
        "double",
        "string",
    ]
    assert record["restored"] == 3 and record["ambiguous"] == {
        "urn:mixed": {XSD + "integer": 1, XSD + "int": 1}
    }


def test_the_local_stage_restores_datatypes_and_records_the_state(tmp_path):
    from types import SimpleNamespace

    from rdfsolve.qlever.datatypes import CENSUS_FILE
    from scripts.pipeline_stages.config import PipelineConfig
    from scripts.pipeline_stages.local import LocalMiningStage

    stage = LocalMiningStage(PipelineConfig(base_dir=tmp_path, data_dir=tmp_path))
    miner = SimpleNamespace(last_report=SimpleNamespace(config={}))
    schema = SimpleNamespace(patterns=[literal("urn:n", XSD + "int")], structural_patterns=[])
    stage._restore_literal_datatypes(miner, schema, "source")
    assert miner.last_report.config["literal_datatypes"]["state"] == "not_restored"
    (tmp_path / "qlever_workdirs" / "source").mkdir(parents=True)
    write_census(
        tmp_path / "qlever_workdirs" / "source" / CENSUS_FILE, {"urn:n": {XSD + "integer": 3}}, []
    )
    stage._restore_literal_datatypes(miner, schema, "source")
    assert schema.patterns[0].datatype == XSD + "integer"
    assert miner.last_report.config["literal_datatypes"]["restored"] == 1


MEMBER = "<urn:a> <urn:n> 42 .\n"


def test_members_of_a_zip_archive_are_counted_without_extraction(tmp_path):
    archive = tmp_path / "rdf.zip"
    with zipfile.ZipFile(archive, "w") as out:
        out.writestr("dir/a.ttl", MEMBER)
        out.writestr(
            "b.nt.gz", gzip.compress(f'<urn:b> <urn:f> "1.5E0"^^<{XSD}double> .\n'.encode())
        )
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


def test_a_line_the_census_cannot_read_is_left_out_and_recorded(tmp_path):
    from rdfsolve.qlever.datatypes import census_of_files, merge_census

    bad = "<urn:x> <urn:p> <http://example.org/a>C> .\n"
    with gzip.open(tmp_path / "a.nt.gz", "wt") as out:
        out.write(TRIPLES + bad + TRIPLES)
    (tmp_path / "b.ttl").write_text("@prefix ex: <urn:> .\nex:a ex:n 1 .\nex:a ex:n <a>C> .\n")
    parts = [census_of_files([(tmp_path / "a.nt.gz", "nt")]), census_of_files([(tmp_path / "b.ttl", "ttl")])]
    census = merge_census(parts)
    assert census["properties"] == {
        "urn:mixed": {XSD + "int": 2},
        "urn:f": {XSD + "double": 2},
        "urn:n": {XSD + "integer": 1},
    }, "Every other statement is counted; Turtle up to the error"
    unread = census["unread"]
    assert unread["lines"] == 1 and unread["lines_by_file"] == {"a.nt.gz": 1}
    assert unread["sample"][0]["line"] == 3 and unread["sample"][0]["text"] == bad.strip()
    assert [f["file"] for f in unread["files_counted_up_to_an_error"]] == ["b.ttl"]
    write_census(tmp_path / "census.json", census["properties"], [], unread=unread)
    assert json.loads((tmp_path / "census.json").read_text())["unread"] == unread


def test_quads_in_an_n_triples_file_are_counted(tmp_path):
    data = tmp_path / "hpa.nt"
    data.write_text(f'<urn:a> <urn:n> "1"^^<{XSD}integer> .\n<urn:a> <urn:n> "2"^^<{XSD}integer> <urn:g> .\n')
    assert count_literal_datatypes([(data, "nt")]) == {"urn:n": {XSD + "integer": 2}}
