"""The fields of a generated model are listed with their property, value types and links."""

from rdfsolve.api import Client
from rdfsolve.schema_models import MinedSchema

VOCABULARY = """
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <https://fields-test.invalid/> .
@prefix other: <https://other-test.invalid/> .
ex:Person a rdfs:Class . ex:Work a rdfs:Class . other:Place a rdfs:Class .
ex:givenName a rdf:Property ; rdfs:domain ex:Person ; rdfs:range xsd:string .
ex:knows a rdf:Property ; rdfs:label "knows" ; rdfs:domain ex:Person ; rdfs:range ex:Person .
ex:homepage a rdf:Property ; rdfs:domain ex:Person ; rdfs:range rdfs:Resource .
ex:author a rdf:Property ; rdfs:domain ex:Work ; rdfs:range ex:Person, rdf:List .
ex:editor a rdf:Property ; rdfs:domain ex:Work ; rdfs:range ex:Person .
ex:place a rdf:Property ; rdfs:domain ex:Person ; rdfs:range other:Place .
"""
CLASSES = [f"https://fields-test.invalid/{c}" for c in ("Person", "Work")] + ["https://other-test.invalid/Place"]


def test_fields_show_properties_types_links_and_lists():
    schema = MinedSchema.from_vocabulary([VOCABULARY], CLASSES)
    client = Client(schema)
    assert client.schema is schema and client.schema.patterns, "The mined schema is public"
    person = client.fields("Person").set_index("field")
    assert list(person.index) == sorted(person.index)
    assert person.loc["given_name", "property"] == "https://fields-test.invalid/givenName"
    assert person.loc["given_name", "curie"] == "ex:givenName"
    assert person.loc["given_name", "values"] == ["xsd:string"] and not person.loc["given_name", "links"]
    assert person.loc["knows", "targets"] == ["https://fields-test.invalid/Person"]
    assert person.loc["knows", "links"] and not person.loc["knows", "list"]
    assert person.loc["homepage", "links"] and person.loc["homepage", "targets"] == [], "IRIs of no class"
    author = client.fields("Work").set_index("field").loc["author"]
    assert author["list"] and author["links"]
    assert client.field_name("Person", "givenName") == client.field_name("Person", "ex:givenName") == "given_name"


def test_diagram_merges_parallel_links_and_selects_namespaces():
    client = Client(MinedSchema.from_vocabulary([VOCABULARY], CLASSES))
    raw = client.diagram(fenced=False)
    assert "ex:Person" in raw and "https://fields-test.invalid/Person" not in raw, "CURIEs by default"
    edges = [line for line in raw.splitlines() if "-->" in line]
    work_person = [e for e in edges if "author" in e.lower()]
    assert len(work_person) == 1 and "editor" in work_person[0].lower(), "One edge per pair of classes"
    assert client.links("Work").set_index("field").target["author"] == client.model("Person").__name__
    assert "other" not in client.diagram(namespaces=["ex"], fenced=False)
    assert client.diagram(namespaces=["https://other-test.invalid/"], fenced=False).count("[") == 1
    assert "https://fields-test.invalid/Person" in client.diagram(iris="full", fenced=False)
    assert "ex:" not in client.diagram(iris="none", fenced=False)
    assert len([e for e in client.diagram(merge=False, fenced=False).splitlines() if "-->" in e]) == len(edges) + 1


def test_member_classes_of_a_list_are_link_targets():
    schema = MinedSchema.from_vocabulary([VOCABULARY.replace("ex:Person, rdf:List", "rdf:List")], CLASSES)
    client = Client(schema)
    assert not client.fields("Work").set_index("field").loc["author", "links"], "Members unknown"
    schema.collections[0].member_types = ["https://fields-test.invalid/Person"]
    author = Client(schema).links("Work").set_index("field").loc["author"]
    assert (author.target, author.basis) == (client.model("Person").__name__, "list member")
