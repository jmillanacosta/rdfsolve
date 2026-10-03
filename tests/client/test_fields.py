import json
import re

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
CLASSES = [f"https://fields-test.invalid/{c}" for c in ("Person", "Work")] + [
    "https://other-test.invalid/Place"
]


def test_fields_show_properties_types_links_and_lists():
    schema = MinedSchema.from_vocabulary([VOCABULARY], CLASSES)
    client = Client(schema)
    assert client.schema is schema and client.schema.patterns, "The mined schema is public"
    person = client.fields("Person").set_index("field")
    assert list(person.index) == sorted(person.index)
    assert person.loc["given_name", "property"] == "https://fields-test.invalid/givenName"
    assert person.loc["given_name", "curie"] == "ex:givenName"
    assert (
        person.loc["given_name", "values"] == ["xsd:string"]
        and not person.loc["given_name", "links"]
    )
    assert person.loc["knows", "targets"] == ["https://fields-test.invalid/Person"]
    assert person.loc["knows", "links"] and not person.loc["knows", "list"]
    assert person.loc["homepage", "links"] and person.loc["homepage", "targets"] == [], (
        "IRIs of no class"
    )
    author = client.fields("Work").set_index("field").loc["author"]
    assert author["list"] and author["links"]
    assert (
        client.field_name("Person", "givenName")
        == client.field_name("Person", "ex:givenName")
        == "given_name"
    )


def test_diagram_merges_parallel_links_and_selects_namespaces():
    client = Client(MinedSchema.from_vocabulary([VOCABULARY], CLASSES))
    raw = client.diagram(fenced=False)
    assert "ex:Person" in raw and "https://fields-test.invalid/Person" not in raw, (
        "CURIEs by default"
    )
    edges = re.findall(r'\w+ -->\|"`(.*?)`"\| \w+', raw, re.DOTALL)
    work_person = [e for e in edges if "author" in e.lower()]
    assert len(work_person) == 1 and "editor" in work_person[0].lower(), (
        "One edge per pair of classes"
    )
    assert (
        client.links("Work").set_index("field").target["author"] == client.model("Person").__name__
    )
    assert "other" not in client.diagram(namespaces=["ex"], fenced=False)
    assert (
        client.diagram(namespaces=["https://other-test.invalid/"], fenced=False).count('("`') == 1
    )
    assert "https://fields-test.invalid/Person" in client.diagram(iris="full", fenced=False)
    assert "ex:" not in client.diagram(iris="none", fenced=False)
    assert (
        len([e for e in client.diagram(merge=False, fenced=False).splitlines() if "-->" in e])
        == len(edges) + 1
    )


def test_member_classes_of_a_list_are_link_targets():
    schema = MinedSchema.from_vocabulary(
        [VOCABULARY.replace("ex:Person, rdf:List", "rdf:List")], CLASSES
    )
    client = Client(schema)
    assert not client.fields("Work").set_index("field").loc["author", "links"], "Members unknown"
    schema.collections[0].member_types = ["https://fields-test.invalid/Person"]
    author = Client(schema).links("Work").set_index("field").loc["author"]
    assert (author.target, author.basis) == (client.model("Person").__name__, "list member")


def test_properties_with_code_names_are_named_by_their_labels():
    codes = """
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix wdt: <http://www.wikidata.org/prop/direct/> .
    @prefix ex: <https://fields-test.invalid/> .
    ex:Event a rdfs:Class .
    wdt:P580 a rdf:Property ; rdfs:label "start time"@en, "Startzeit"@de ; rdfs:domain ex:Event ; rdfs:range xsd:dateTime .
    wdt:P9 a rdf:Property ; rdfs:domain ex:Event ; rdfs:range xsd:string .
    wdt:P2037 a rdf:Property ; rdfs:label "GitHub account" ; rdfs:domain ex:Event ; rdfs:range xsd:string .
    wdt:P496 a rdf:Property ; rdfs:label "ORCID iD" ; rdfs:domain ex:Event ; rdfs:range xsd:string .
    ex:givenName a rdf:Property ; rdfs:label "first name" ; rdfs:domain ex:Event ; rdfs:range xsd:string .
    """
    client = Client(MinedSchema.from_vocabulary([codes], ["https://fields-test.invalid/Event"]))
    fields = set(client.model("Event").model_fields)
    assert {"start_time", "p9", "given_name"} <= fields, (
        "A label names a code; a word keeps its name"
    )
    assert {"github_account", "orcid_id"} <= fields, "The words of a label are not split further"
    assert {client.field_name("Event", n) for n in ("P580", "wdt:P580", "start time")} == {
        "start_time"
    }


def test_the_links_of_one_record_type_are_drawn_with_what_they_reach():
    """A link diagram draws one record type, the named links and what each reaches; a link
    named with ^ points to the record type; classes of another namespace are left out."""
    client = Client(MinedSchema.from_vocabulary([VOCABULARY], CLASSES))
    drawn = client.diagram("Work", links=["author", "editor"])
    assert drawn.startswith("```mermaid") and "**Work**" in drawn and "**Person**" in drawn
    assert re.search(r'C0 -->\|"`author \(\d[\d,]*\)`"\| C1', drawn), drawn
    assert "Place" not in client.diagram("Person", links=["place"]), "another namespace"
    into = client.diagram("Person", links=["^knows"])
    assert re.search(r'C1 -->\|"`knows', into) is None and "C0 -->|" in into, (
        "knows: Person to Person"
    )


def test_a_record_type_can_be_named_by_its_curie():
    client = Client(MinedSchema.from_vocabulary([VOCABULARY], CLASSES))
    assert client.model("ex:Person") is client.model("Person")


def test_a_shared_name_is_decided_by_the_links_used():
    """Two classes named Place: the one whose statements use the link is chosen."""
    vocabulary = (
        VOCABULARY
        + """
    ex:Place a rdfs:Class . ex:near a rdf:Property ; rdfs:domain ex:Place ; rdfs:range ex:Person ."""
    )
    client = Client(
        MinedSchema.from_vocabulary([vocabulary], [*CLASSES, "https://fields-test.invalid/Place"])
    )
    from rdfsolve.client.hydration import class_iri

    assert class_iri(client.model("Place", links=["near"])) == "https://fields-test.invalid/Place"


def test_the_release_is_read_from_the_metadata_saved_beside_the_schema(tmp_path, monkeypatch):
    """A source whose description comes with its download: the release is read from
    <name>_metadata.ttl, from the dataset whose subjects are the schema's classes."""
    schema = MinedSchema.from_vocabulary([VOCABULARY], CLASSES)
    path = tmp_path / "people_local_schema.json"
    path.write_text(json.dumps(schema.to_dict()))
    (tmp_path / "people_local_metadata.ttl").write_text("""
    @prefix void: <http://rdfs.org/ns/void#> . @prefix dcterms: <http://purl.org/dc/terms/> .
    <urn:release-7> a void:Dataset ; dcterms:issued "2026-08-10" ; dcterms:subject <https://fields-test.invalid/Person> .
    <urn:other-ontology> a void:Dataset ; dcterms:issued "2020-01-01" .""")
    client = Client.open(path)
    assert str(client.schema.about.source_issued).startswith("2026-08-10")
    dotted = tmp_path / "people.schema.json"
    dotted.write_text(path.read_text())
    (tmp_path / "people.metadata.ttl").write_text(
        (tmp_path / "people_local_metadata.ttl").read_text()
    )
    assert str(Client.open(dotted).schema.about.source_issued).startswith("2026-08-10")
    monkeypatch.chdir(tmp_path)
    assert str(Client.open("people.schema.json").schema.about.source_issued).startswith(
        "2026-08-10"
    ), "relative path"


def test_a_construct_runs_on_local_rdf(tmp_path):
    data = tmp_path / "people.ttl"
    data.write_text(
        "<urn:ada> a <https://fields-test.invalid/Person> ; <https://fields-test.invalid/givenName> 'Ada' ."
    )
    schema = tmp_path / "people_local_schema.json"
    schema.write_text(json.dumps(MinedSchema.from_vocabulary([VOCABULARY], CLASSES).to_dict()))
    client = Client.open(schema, data_file=data)
    out = client.construct(
        "CONSTRUCT { ?p <urn:named> ?n } WHERE { ?p <https://fields-test.invalid/givenName> ?n }"
    )
    assert [q.object.value for q in out] == ["Ada"]
    assert "CONSTRUCT" in client.queries[-1]
