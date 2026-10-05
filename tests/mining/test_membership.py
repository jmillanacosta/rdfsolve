"""rdfsolve.mining membership: a source whose records are placed in their classes by a property
other than rdf:type (a category), next to records typed with rdf:type, is mined with both as
class membership; the membership edges are not data rows, the records are not left untyped,
and the schema keeps the membership for the queries written from it."""

from rdflib import RDF, Graph, Literal, URIRef

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.query_builders import _type_pattern

CATEGORY = "urn:category"
DATA = [
    ("urn:g1", CATEGORY, URIRef("urn:Gene")),
    ("urn:g1", "urn:label", Literal("g1")),
    ("urn:d1", CATEGORY, URIRef("urn:Disease")),
    ("urn:d1", "urn:label", Literal("d1")),
    ("urn:a1", str(RDF.type), URIRef("urn:Association")),
    ("urn:a1", "urn:subject", URIRef("urn:g1")),
    ("urn:a1", "urn:object", URIRef("urn:d1")),
]


def mine(membership=None):
    graph = Graph()
    for s, p, o in DATA:
        graph.add((URIRef(s), URIRef(p), o))
    with SchemaMiner.from_graph(graph, delay=0, membership_properties=membership) as miner:
        return miner.mine("membership")


def test_records_placed_by_a_category_are_mined_as_class_members():
    schema = mine([str(RDF.type), CATEGORY])
    rows = {(p.subject_class, p.property_uri, p.object_class): p.count for p in schema.patterns}
    assert rows == {
        ("urn:Gene", "urn:label", "Literal"): 1,
        ("urn:Disease", "urn:label", "Literal"): 1,
        ("urn:Association", "urn:subject", "urn:Gene"): 1,
        ("urn:Association", "urn:object", "urn:Disease"): 1,
    }, "No membership rows; the associations reach typed records"
    assert not schema.structural_patterns, "No record is left untyped"
    assert schema.about.membership_property == [str(RDF.type), CATEGORY]
    path = _type_pattern("?s", "<urn:Gene>", membership=schema.about.membership_property)
    assert path == f"?s (a|<{CATEGORY}>) <urn:Gene> ."
    without = mine()
    assert "urn:Gene" not in {p.subject_class for p in without.patterns}
    assert without.structural_patterns, "With rdf:type alone, the records are untyped"
