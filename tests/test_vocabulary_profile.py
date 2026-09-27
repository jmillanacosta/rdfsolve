"""Records follow what vocabularies state about their own terms, and a profile that the user owns."""

from rdfsolve.api import Client
from rdfsolve.schema_models import MinedSchema

S, FOAF = "https://schema.org/", "http://xmlns.com/foaf/0.1/"
VOCABULARY = """
@prefix schema: <https://schema.org/> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix foaf: <http://xmlns.com/foaf/0.1/> .
schema:Thing a rdfs:Class . schema:Text a schema:DataType, rdfs:Class .
schema:Person a rdfs:Class; rdfs:subClassOf schema:Thing; owl:equivalentClass foaf:Person .
schema:name schema:domainIncludes schema:Thing; schema:rangeIncludes schema:Text .
foaf:Person a rdfs:Class . foaf:Document a rdfs:Class .
foaf:familyName a owl:DatatypeProperty; rdfs:domain foaf:Person; rdfs:range rdfs:Literal .
"""


def test_an_equivalent_class_brings_its_properties_without_a_new_axiom():
    schema = MinedSchema.from_vocabulary(VOCABULARY, [S + "Person", FOAF + "Person"])
    rows = {(p.subject_class, p.property_uri) for p in schema.patterns}
    assert (S + "Person", FOAF + "familyName") in rows, "schema.org states the equivalence"
    assert (FOAF + "Person", S + "name") in rows, "Equivalence goes both ways"
    assert S + "Person" not in schema.class_hierarchy.get(FOAF + "Person", []), "Not a subclass"
    client = Client(schema)
    family = client.field_name(S + "Person", FOAF + "familyName")
    record = client.create(S + "Person", uri="urn:me", name="Me", **{family: "Me"})
    assert {str(p) for p in record.to_graph().predicates()} >= {S + "name", FOAF + "familyName"}


CODE = (
    VOCABULARY
    + """
schema:SoftwareSourceCode a rdfs:Class; rdfs:subClassOf schema:Thing .
schema:SoftwareApplication a rdfs:Class; rdfs:subClassOf schema:Thing .
schema:softwareVersion schema:domainIncludes schema:SoftwareApplication; schema:rangeIncludes schema:Text .
"""
)
PROFILE = """
@prefix schema: <https://schema.org/> . @prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix site: <urn:site:> .
site:SourceCode a sh:NodeShape ; sh:targetClass schema:SoftwareSourceCode ;
    sh:property [ sh:path schema:softwareVersion ; sh:datatype xsd:string ] .
"""


def test_a_profile_adds_rows_that_the_vocabulary_does_not_declare():
    import pytest

    classes = [S + "SoftwareSourceCode"]
    schema = MinedSchema.from_vocabulary(CODE, classes, profile=PROFILE)
    row = next(p for p in schema.patterns if p.property_uri == S + "softwareVersion")
    assert (row.subject_class, row.datatype, row.evidence_source) == (
        S + "SoftwareSourceCode",
        "http://www.w3.org/2001/XMLSchema#string",
        "shacl",
    ), "The row says that it comes from the profile, not from schema.org"
    client = Client(schema)
    record = client.create(S + "SoftwareSourceCode", uri="urn:code", softwareVersion="1.0")
    assert (None, None, None) in record.to_graph()
    with pytest.raises(ValueError, match="not requested"):
        MinedSchema.from_vocabulary(CODE, [S + "Person"], profile=PROFILE)


LISTS = """
@prefix schema: <https://schema.org/> . @prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
[] a sh:NodeShape ; sh:targetClass schema:SoftwareSourceCode ; sh:property [
    sh:path schema:contributor ; sh:nodeKind sh:BlankNode ;
    sh:property [ sh:path ( [ sh:zeroOrMorePath rdf:rest ] rdf:first ) ; sh:class schema:Person ] ] .
"""


def test_a_profile_can_declare_ordered_lists():
    from rdfsolve.api import RDFList

    schema = MinedSchema.from_vocabulary(
        CODE, [S + "SoftwareSourceCode", S + "Person"], profile=LISTS
    )
    client = Client(schema)
    people = [client.create(S + "Person", uri=f"urn:p{i}", name=f"P{i}") for i in (1, 2)]
    code = client.create(
        S + "SoftwareSourceCode", uri="urn:code", contributor=RDFList(items=people)
    )
    graph = code.to_graph()
    head = next(o for _, p, o in graph if str(p) == S + "contributor")
    assert [str(m) for m in graph.items(head)] == ["urn:p1", "urn:p2"], "In their order"
