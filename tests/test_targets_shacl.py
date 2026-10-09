"""A SHACL shapes graph read as a target model, and the target file dispatch."""

import json

import pytest

from rdfsolve.client.diagram import flowchart
from rdfsolve.plan import NotFoundError, PlanError, Target
from rdfsolve.target_model import Model, read_model
from rdfsolve.targets.shacl import Shacl

SHAPES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/> .

ex:Agent a rdfs:Class .
ex:Person rdfs:subClassOf ex:Agent ; rdfs:comment "A human being." .
ex:AgentShape a sh:NodeShape ; sh:targetClass ex:Agent .
ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;
    sh:property [ sh:path rdfs:label ; sh:datatype xsd:string ] ;
    sh:property [ sh:path ex:memberOf ; sh:class ex:Group ] ;
    sh:property [ sh:path ex:knows ; sh:or ( [ sh:class ex:Person ] [ sh:node ex:AgentShape ] ) ] ;
    sh:property [ sh:path ex:status ; sh:in ( ex:active ex:retired ) ] ;
    sh:property [ sh:path [ sh:inversePath ex:memberOf ] ] .
ex:GroupShape a sh:NodeShape ; sh:targetClass ex:Group ; rdfs:label "WorkingGroup" ;
    sh:property [ sh:path ex:memberOf ; sh:class ex:Group ] .
"""


@pytest.fixture
def shapes(tmp_path):
    path = tmp_path / "people_shapes.ttl"
    path.write_text(SHAPES)
    return path


def test_shapes_give_kinds_relations_and_values(shapes):
    model = Shacl.read(shapes)
    kinds = {k.name: k for k in model.kinds()}
    assert set(kinds) == {"Agent", "Person", "Working group"}, (
        "a label written as an identifier is split"
    )
    assert kinds["Person"].parents == ("Agent",) and kinds["Person"].definition == "A human being."
    rels = {r.name: r for r in model.relations()}
    assert rels["member of"].pairs == (
        ("Person", "Working group"),
        ("Working group", "Working group"),
    )
    assert rels["member of"].domain is None and rels["member of"].range == "Working group"
    assert rels["knows"].pairs == (("Person", "Agent"), ("Person", "Person"))
    assert rels["label"].range == "literal"
    assert [v.name for v in model.values()] == ["active", "retired"]
    assert model.skipped == 1, "a path other than one IRI is counted, not read"
    assert model.naming_relation() == "label"
    assert model.kind_ancestors("Person") == ["Agent"]
    assert {"hierarchy", "definitions"} <= model.capabilities


def test_a_target_from_shapes_finds_terms_by_their_words(shapes):
    target = Target(shapes)
    assert target.kinds["working group"].iri == "http://example.org/Group"
    assert target.kinds.WorkingGroup is target.kinds["Working group"]
    member = target.relations.member_of
    assert member.to_dict()["pairs"] == [
        ["Person", "Working group"],
        ["Working group", "Working group"],
    ]
    assert "between: any → Working group" in repr(member)
    with pytest.raises(NotFoundError) as found:
        target.relations["member"]
    assert found.value.code == "unknown_name" and "member_of" in found.value.choices


def test_read_model_dispatches_by_file(shapes, tmp_path):
    from rdflib import Graph

    assert isinstance(read_model(shapes), Shacl)
    assert isinstance(read_model(Graph().parse(data=SHAPES, format="turtle")), Shacl)
    yaml = tmp_path / "m.yaml"
    yaml.write_text("id: https://example.org/m\nname: m\nclasses:\n  thing: {}\n")
    assert isinstance(read_model(yaml), Model)
    other = tmp_path / "x.json"
    other.write_text(json.dumps({"a": 1}))
    with pytest.raises(PlanError) as wrong:
        read_model(other)
    assert wrong.value.to_dict()["error"]["code"] == wrong.value.code
    with pytest.raises(PlanError) as missing:
        read_model(tmp_path / "none.yaml")
    assert missing.value.code == "no_such_file" and missing.value.hint


def test_plan_errors_are_data():
    error = NotFoundError(
        "No kind named Bindng", code="not_found", hint="plan.Binding", choices=["Binding"]
    )
    assert error.to_dict() == {
        "error": {
            "code": "not_found",
            "message": "No kind named Bindng",
            "hint": "plan.Binding",
            "choices": ["Binding"],
        }
    }
    assert isinstance(error, (AttributeError, KeyError, ValueError))
    assert "try: plan.Binding" in str(error)


def test_flowchart_draws_boxes_and_labelled_arrows():
    text = str(
        flowchart(
            [("a", "Person", "3 records", "focus"), ("b", "Group", "", "")],
            [("a", "member of", "b", ""), ("a", "knows", "a", "dashed")],
        )
    )
    assert 'a("`**Person**' in text and ":::focus" in text
    assert 'a -->|"`member of`"| b' in text and 'a -.->|"`knows`"| a' in text
