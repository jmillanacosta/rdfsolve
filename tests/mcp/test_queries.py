"""Expected bindings and counterexamples define query validity."""

from conftest import prepare

import pytest
from conftest import E, declare, event_goals, field, insert, values
from rdflib import RDF, XSD, Literal, URIRef

from rdfsolve.retrieval import QueryValidationError


def test_event_answers_preserve_owner_scope_and_optional_data(session):
    prepared = prepare(session, event_goals(session))
    rows = values(session, prepared)
    actual = {
        (r["event"]["value"], r.get("species", {}).get("value"), r.get("method", {}).get("value"))
        for r in rows
    }
    assert actual == {
        (str(E.ke1), str(E.Mouse), "Assay A"),
        (str(E.ke1), str(E.Mouse), "Assay B"),
        (str(E.ke3), None, None),
    }
    assert {r["method"]["xml:lang"] for r in rows if "method" in r} == {"en"}
    assert (
        str(E.mouseAOP)
        not in session.export(session.finish(prepared["query_ref"])["result_ref"])["query"]
    )


@pytest.mark.parametrize("change", ["projection", "anchor", "optional", "empty"])
def test_final_query_must_preserve_declared_goal_witnesses(session, change):
    goals = event_goals(session)
    ref = prepare(session, goals)["query_ref"]
    query = session.prepared[ref].template
    if change == "projection":
        query = query.replace("SELECT DISTINCT ?event", "SELECT DISTINCT ?aop")
    elif change == "anchor":
        import re

        query = re.sub(r"VALUES \?tax \{[^}]+\}\s*}", "", query)
    elif change == "optional":
        query = query.replace("OPTIONAL", "")
    else:
        query = "SELECT ?event WHERE { FILTER(false) }"
    with pytest.raises(ValueError):
        prepare(session, goals, sparql=query)


def test_missing_goal_never_becomes_ready(session):
    goals = event_goals(session)
    del goals["g2"]
    with pytest.raises(QueryValidationError, match="every active"):
        prepare(session, goals)


def test_optional_parent_cannot_bind_an_unrelated_chemical(session):
    declare(session, "Pathways and available chemical names")
    parent = field(session, E.AOP, E.chemical, "a", "c")
    name = field(session, E.Chemical, "http://www.w3.org/2000/01/rdf-schema#label", "c", "name")
    root = insert(session.catalogue.type_refs[str(E.AOP)], "a")
    bad = root + " OPTIONAL { " + parent + " } OPTIONAL { " + name + " }"
    with pytest.raises(QueryValidationError, match="OPTIONAL"):
        prepare(session, {"g1": {"pattern": bad, "project": ["a", "c", "name"]}})
    good = root + " OPTIONAL { " + parent + " OPTIONAL { " + name + " } }"
    rows = values(
        session, prepare(session, {"g1": {"pattern": good, "project": ["a", "c", "name"]}})
    )
    assert [r for r in rows if r["a"]["value"] == str(E.noChemicalAOP)] == [
        {"a": {"type": "uri", "value": str(E.noChemicalAOP)}}
    ]
    assert {r["name"]["xml:lang"] for r in rows if "name" in r} == {"en", "nl"}


@pytest.mark.parametrize(
    "pattern,code",
    [
        (f"?a a <{E.AOP}> . ?a <{E.method}> ?m .", "field_owner"),
        (f'?a a <{E.AOP}> . ?a <{E.taxon}> "Human" .', "field_term_kind"),
        (f"?a a <{E.AOP}> . ?c a <{E.Chemical}> .", "disconnected_roles"),
        (f"?a a <{E.AOP}> . BIND(STR(?a) AS ?value)", "retrieval_operator"),
        (f"?a a <{E.AOP}> . ?a ?predicate ?value", "variable_predicate"),
    ],
)
def test_field_and_retrieval_constraints(session, pattern, code):
    declare(session, "Return a")
    with pytest.raises(QueryValidationError) as error:
        prepare(session, {"g1": {"pattern": pattern, "project": ["a"]}})
    assert error.value.code == code


@pytest.mark.parametrize(
    "pattern",
    [
        "SERVICE SILENT <https://remote.invalid/sparql> { ?a ?p ?o }",
        "{ SELECT ?a WHERE { SERVICE ?endpoint { ?a ?p ?o } } }",
        "GRAPH <urn:graph> { ?a ?p ?o }",
    ],
)
def test_source_scope_cannot_be_overridden(session, pattern):
    declare(session, "Return a")
    with pytest.raises(ValueError):
        prepare(session, {"g1": {"pattern": pattern, "project": ["a"]}})
    assert not session.client.queries


def test_same_record_constraint_and_repeated_class_roles():
    from rdflib import Graph

    from rdfsolve.client_api import Client
    from rdfsolve.mcp.session import Session
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern

    graph = Graph().parse(
        data="""@prefix e: <urn:jobs:> .
        e:alice a e:Person; e:job e:j1, e:j2 . e:bob a e:Person; e:job e:j3 .
        e:j1 a e:Job; e:employer e:X; e:year 2010 .
        e:j2 a e:Job; e:employer e:Y; e:year 2020 .
        e:j3 a e:Job; e:employer e:X; e:year 2020 .
        e:X a e:Company . e:Y a e:Company .""",
        format="turtle",
    )
    patterns = [
        ("Person", "job", "Job"),
        ("Job", "employer", "Company"),
        ("Job", "year", "Literal"),
    ]
    schema = MinedSchema(
        about={"dataset_name": "jobs"},
        patterns=[
            SchemaPattern(
                subject_class="urn:jobs:" + s,
                property_uri="urn:jobs:" + p,
                object_class="Literal" if o == "Literal" else "urn:jobs:" + o,
            )
            for s, p, o in patterns
        ],
    )
    s = Session(Client(schema, graph, graph_uris=[]))
    declare(s, "People whose same job is at X during 2020")
    pattern = "?person a <urn:jobs:Person>; <urn:jobs:job> ?job . ?job a <urn:jobs:Job>; <urn:jobs:employer> ?company; <urn:jobs:year> 2020 . ?company a <urn:jobs:Company> ."
    # Register the actual company through typed package retrieval.
    record = s.client.get(s.client.model("urn:jobs:Company"), "urn:jobs:X", fields=[])
    from rdfsolve.query_fragments import Fragment
    from rdfsolve.schema_models.enrichment import RdfTerm

    ref = s.catalogue._put(
        Fragment("term", "X", term=RdfTerm(kind="uri", value=str(record.uri))), str(record.uri)
    )
    rows = values(
        s,
        prepare(
            s,
            {
                "g1": {
                    "pattern": pattern + " VALUES ?company { " + insert(ref) + " }",
                    "project": ["person"],
                }
            },
        ),
    )
    assert rows == [{"person": {"type": "uri", "value": "urn:jobs:bob"}}]
    roles = s.paths("urn:jobs:Person", "urn:jobs:Person", max_hops=4)
    assert roles["items"]


def test_graph_scope_does_not_join_different_named_graphs(session):
    from conftest import event_goals
    from rdflib import Dataset

    from rdfsolve.client_api import Client
    from rdfsolve.mcp.session import Session

    graphs = Dataset()
    for triple in session.client.source:
        graph = graphs.graph(URIRef("urn:first" if triple[1] != E.event else "urn:second"))
        graph.add(triple)
    scoped = Session(Client(session.client._schema, graphs, graph_uris=["urn:first", "urn:second"]))
    rows = values(scoped, prepare(scoped, event_goals(scoped)))
    assert rows == []
    for triple in session.client.source:
        graphs.graph(URIRef("urn:complete")).add(triple)
    complete = Session(Client(session.client._schema, graphs, graph_uris=["urn:complete"]))
    assert len(values(complete, prepare(complete, event_goals(complete)))) == 3


def test_declared_outputs_and_entity_restrictions_need_actual_witnesses(session):
    c = session.catalogue
    goals = [
        dict(clause="Return Key Events", kind="output", concept="Key Event"),
        dict(
            clause="AOPs apply to Human",
            kind="entity_filter",
            concept="Taxon",
            owner="Adverse Outcome Pathway",
            value="Human",
        ),
    ]
    session.schema(question="Return Key Events of AOPs applicable to Human", goals=goals)
    human = session.find("Human", str(E.Taxon))["items"][0]["ref"]
    event = c.type_refs[str(E.Event)]
    tax = c.field_refs[
        (str(E.AOP), session.client.field_name(session.client.model(str(E.AOP)), str(E.taxon)))
    ]
    grounding = {
        "g1": dict(
            pattern=field(session, E.AOP, E.event, "a", "event") + " " + insert(event, "event"),
            project=["event"],
            evidence=[event],
        ),
        "g2": dict(
            pattern=field(session, E.AOP, E.taxon, "a", "tax")
            + " VALUES ?tax { "
            + insert(human)
            + " }",
            evidence=[tax],
            entity=human,
        ),
    }
    assert len(values(session, prepare(session, grounding))) == 2
    from copy import deepcopy

    bad = deepcopy(grounding)
    bad["g1"]["project"] = ["a"]
    with pytest.raises(QueryValidationError, match="project"):
        prepare(session, bad)
    bad = deepcopy(grounding)
    bad["g2"]["pattern"] = (
        field(session, E.AOP, E.context, "a", "context")
        + ' FILTER(CONTAINS(STR(?context),"Human"))'
    )
    with pytest.raises(QueryValidationError):
        prepare(session, bad)
    bad = deepcopy(grounding)
    bad["g2"]["pattern"] = field(session, E.AOP, E.taxon, "a", "tax")
    with pytest.raises(QueryValidationError, match="exact entity"):
        prepare(session, bad)
    bad = deepcopy(grounding)
    bad["g1"] = dict(
        pattern=insert(c.type_refs[str(E.AOP)], "a"),
        project=["a"],
        evidence=[c.type_refs[str(E.AOP)]],
    )
    with pytest.raises(QueryValidationError, match="Key Event"):
        prepare(session, bad)


def test_source_scope_is_configuration_and_term_kinds_include_values(session):
    c = session.catalogue
    session.schema(
        question="List AOPs from rdf",
        goals=[
            dict(clause="Return AOPs", kind="output", concept="Adverse Outcome Pathway"),
            dict(clause="from rdf", kind="entity_filter", concept="database", value="rdf"),
        ],
    )
    root = c.type_refs[str(E.AOP)]
    good = session.prepare(
        "SELECT ?a WHERE { " + insert(root, "a") + " }",
        {"g1": {"evidence": [root], "project": ["a"]}},
    )
    assert len(values(session, good)) == 3
    human = session.find("Human", str(E.Taxon))["items"][0]["ref"]
    wrong = (
        "SELECT ?a WHERE { "
        + field(session, E.AOP, E.context, "a", "text")
        + " VALUES ?text { "
        + insert(human)
        + " } }"
    )
    with pytest.raises(QueryValidationError, match="literal-valued"):
        session.prepare(wrong, {"g1": {"evidence": [root], "project": ["a"]}})


def test_initial_grounding_repairs_preserve_clauses_and_stop_after_preparation(session):
    request = "Return pathways"
    wrong = [
        dict(clause=request, kind="entity_filter", concept="Adverse Outcome Pathway", value="Human")
    ]
    session.schema(question=request, goals=wrong)
    corrected = [dict(clause=request, kind="output", concept="Adverse Outcome Pathway")]
    session.schema(question=request, goals=corrected)
    ref = session.catalogue.type_refs[str(E.AOP)]
    ready = session.prepare(
        "SELECT ?a WHERE { " + insert(ref, "a") + " }", {"g1": {"evidence": [ref]}}
    )
    assert ready["warnings"] and len(values(session, ready)) == 3
    with pytest.raises(ValueError, match="retained across probes"):
        session.schema(question=request, goals=wrong)
