"""Class membership is not a schema row: rdf:type edges give the subject classes and the counts."""

from rdflib import Dataset

from rdfsolve.mining.miner import SchemaMiner

TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> .
<urn:c> a <urn:A> ; <urn:q> "x" .
"""


def test_type_rows_are_left_out_and_membership_is_kept():
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        schema = miner.mine("membership")
        report = miner.last_report
    assert not [p for p in schema.patterns if p.property_uri == TYPE], "No (C, rdf:type, X) rows"
    assert {(p.subject_class, p.property_uri) for p in schema.patterns} == {
        ("urn:A", "urn:p"),
        ("urn:A", "urn:q"),
    }
    assert schema.about.class_entity_counts == {"urn:A": 2, "urn:B": 1}, (
        "A class whose members have only rdf:type keeps its member count"
    )
    assert report.config["membership_rows_left_out"] >= 2
