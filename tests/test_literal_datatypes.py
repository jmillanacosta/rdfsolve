"""QLever returns every integer type as xsd:int and a decimal as xsd:double (DATATYPE()), so the
index loses the datatype of the source: AOP-Wiki void:triples is xsd:integer, Disease Ontology
owl:qualifiedCardinality is xsd:nonNegativeInteger. The source files are read once when the
index is built, and mining restores the datatype of the source when a property has one."""

import gzip

from rdfsolve.qlever.datatypes import count_literal_datatypes, read_census, restore_datatypes, write_census
from rdfsolve.schema_models.pattern import SchemaPattern

XSD = "http://www.w3.org/2001/XMLSchema#"
TURTLE = """@prefix ex: <urn:> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:n 42 ; ex:m "1"^^xsd:nonNegativeInteger ; ex:d 14.1 ; ex:mixed 1 ; ex:s "x" .
"""
TRIPLES = f'<urn:b> <urn:mixed> "2"^^<{XSD}int> .\n<urn:b> <urn:f> "1.5E0"^^<{XSD}double> .\n'


def literal(prop, datatype):
    return SchemaPattern(subject_class="urn:A", property_uri=prop, object_class="Literal", datatype=datatype, count=1)


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
    patterns = [literal("urn:n", XSD + "int"), literal("urn:m", XSD + "int"), literal("urn:d", XSD + "double"),
                literal("urn:mixed", XSD + "int"), literal("urn:f", XSD + "double"), literal("urn:s", XSD + "string")]
    record = restore_datatypes(patterns, census)
    assert [p.datatype.removeprefix(XSD) for p in patterns] == [
        "integer", "nonNegativeInteger", "decimal", "int", "double", "string"
    ]
    assert record["restored"] == 3 and record["ambiguous"] == {"urn:mixed": {XSD + "integer": 1, XSD + "int": 1}}


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
    write_census(tmp_path / "qlever_workdirs" / "source" / CENSUS_FILE, {"urn:n": {XSD + "integer": 3}}, [])
    stage._restore_literal_datatypes(miner, schema, "source")
    assert schema.patterns[0].datatype == XSD + "integer"
    assert miner.last_report.config["literal_datatypes"]["restored"] == 1

