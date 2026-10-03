"""RDF list values are constrained member by member in SHACL, and read back as lists."""

from pyshacl import validate
from rdflib import Graph

from rdfsolve.schema_models import MinedSchema

VOCABULARY = """
@prefix schema: <https://schema.org/> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
schema:Thing a rdfs:Class . schema:Person a rdfs:Class; rdfs:subClassOf schema:Thing .
schema:colleague schema:domainIncludes schema:Person; schema:rangeIncludes schema:Person, rdf:List .
"""
DATA = """
@prefix schema: <https://schema.org/> .
<urn:a> a schema:Person ; schema:colleague ( <urn:b> {second} ) .
<urn:b> a schema:Person . <urn:c> a schema:Person . <urn:x> a schema:Thing .
"""


def test_list_members_are_checked_and_the_list_survives_a_round_trip():
    schema = MinedSchema.from_vocabulary(VOCABULARY, ["https://schema.org/Person"])
    shapes = Graph().parse(
        data=schema.to_shacl(activate_observed=True, void=False), format="turtle"
    )
    for second, conforms in (("<urn:c>", True), ("<urn:x>", False)):
        data = Graph().parse(data=DATA.format(second=second), format="turtle")
        assert validate(data, shacl_graph=shapes)[0] is conforms, f"member {second}"
    restored = MinedSchema.from_shacl(shapes.serialize(format="turtle"))
    (profile,) = restored.collections
    assert (profile.subject_class, profile.property_uri, profile.member_types) == (
        "https://schema.org/Person",
        "https://schema.org/colleague",
        ["https://schema.org/Person"],
    )
