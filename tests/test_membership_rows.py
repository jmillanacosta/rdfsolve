"""A (C, rdf:type, Resource) row says only that a class IRI has no type; it is not a schema row.
A row whose type value has a class, such as owl:Class, describes the source and is kept."""

from rdflib import Dataset

from rdfsolve.mining.miner import SchemaMiner

TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"
DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> .
<urn:c> a <urn:A> ; <urn:q> "x" .
<urn:B> a <http://www.w3.org/2002/07/owl#Class> .
"""


def test_only_uninformative_type_rows_are_left_out():
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        schema = miner.mine("membership")
        report = miner.last_report
    rows = {(p.subject_class, p.object_class) for p in schema.patterns if p.property_uri == TYPE}
    assert ("urn:A", "Resource") not in rows, "The class IRI urn:A has no type"
    assert ("urn:B", OWL_CLASS) in rows, "The class IRI urn:B is declared owl:Class"
    assert schema.about.class_entity_counts["urn:A"] == 2
    assert report.config["membership_rows_left_out"] >= 1
