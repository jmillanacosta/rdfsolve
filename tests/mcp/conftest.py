"""Small RDF oracles for the public investigation workflow."""

from collections import Counter

import pytest
from rdflib import RDF, RDFS, XSD, BNode, Dataset, Graph, Literal, Namespace

from rdfsolve.client_api import Client
from rdfsolve.mcp.session import Session
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import RdfTerm, TermAnnotation
from rdfsolve.schema_models.pattern import SchemaPattern

E = Namespace("https://workspace-test.invalid/")


def insert(ref, *variables):
    return "{{" + ref + (" " + " ".join("?" + v for v in variables) if variables else "") + "}}"


def make_session(graph=None, scope=None):
    g = graph if graph is not None else Graph()
    triples = [
        (E.humanAOP, RDF.type, E.AOP),
        (E.mouseAOP, RDF.type, E.AOP),
        (E.noChemicalAOP, RDF.type, E.AOP),
        (E.Human, RDF.type, E.Taxon),
        (E.Mouse, RDF.type, E.Taxon),
        (E.Human, RDFS.label, Literal("Human")),
        (E.Mouse, RDFS.label, Literal("Mouse")),
        (E.humanAOP, E.taxon, E.Human),
        (E.mouseAOP, E.taxon, E.Mouse),
        (E.noChemicalAOP, E.taxon, E.Human),
        (E.humanAOP, E.context, Literal("Thyroid response in this species")),
        (E.mouseAOP, E.context, Literal("Thyroid in mice; relevance to human health unknown")),
        (E.noChemicalAOP, E.context, Literal("Thyroid response without chemical metadata")),
        (E.ke1, RDF.type, E.Event),
        (E.ke2, RDF.type, E.Event),
        (E.ke3, RDF.type, E.Event),
        (E.humanAOP, E.event, E.ke1),
        (E.humanAOP, E.event, E.ke3),
        (E.mouseAOP, E.event, E.ke2),
        (E.ke1, E.taxon, E.Mouse),
        (E.ke2, E.taxon, E.Human),
        (E.ke1, E.method, Literal("Assay A", lang="en")),
        (E.ke1, E.method, Literal("Assay B", lang="en")),
        (E.ke2, E.method, Literal("Unrelated assay")),
        (E.chemical1, RDF.type, E.Chemical),
        (E.humanAOP, E.chemical, E.chemical1),
        (E.chemical1, RDFS.label, Literal("Name one", lang="en")),
        (E.chemical1, RDFS.label, Literal("Naam twee", lang="nl")),
        (E.chemical1, E.identifier, Literal("001", datatype=XSD.string)),
        (E.chemical1, E.identifier, Literal("123", datatype=XSD.string)),
    ]
    for t in triples:
        g.add(t)
    patterns = [
        (E.AOP, E.taxon, E.Taxon),
        (E.AOP, E.context, "Literal"),
        (E.AOP, E.event, E.Event),
        (E.AOP, E.chemical, E.Chemical),
        (E.Event, E.taxon, E.Taxon),
        (E.Event, E.method, "Literal"),
        (E.Taxon, RDFS.label, "Literal"),
        (E.Chemical, RDFS.label, "Literal"),
        (E.Chemical, E.identifier, "Literal"),
    ]
    schema = MinedSchema(
        about={"dataset_name": "test"},
        patterns=[
            SchemaPattern(subject_class=str(a), property_uri=str(p), object_class=str(b))
            for a, p, b in patterns
        ],
    )
    labels = [
        (E.AOP, "Adverse Outcome Pathway"),
        (E.Event, "Key Event"),
        (E.Taxon, "Taxon"),
        (E.Chemical, "Chemical"),
        (E.event, "has Key Event"),
        (E.taxon, "applicable taxon"),
        (E.method, "measurement method"),
    ]
    for iri, label in labels:
        schema.enrichment.labels.append(
            TermAnnotation(
                term_iri=str(iri),
                predicate=str(RDFS.label),
                text=RdfTerm(kind="literal", value=label),
            )
        )
    c = Client(schema, g, graph_uris=scope or [])
    return Session(c)


@pytest.fixture
def session():
    s = make_session()
    yield s
    s.client.close()


def field(s, owner, predicate, left, right):
    c = s.catalogue
    name = s.client.field_name(s.client.model(str(owner)), str(predicate))
    return insert(c.field_refs[(str(owner), name)], left, right)


def declare(s, *clauses):
    s.schema(question="; ".join(clauses), goals=list(clauses))


def event_goals(s):
    s.schema(
        question="Return Key Events and available metadata for Human AOPs",
        goals=[
            {"clause": "Return Key Events", "kind": "output", "concept": "Key Event"},
            {
                "clause": "AOP applicability is Human",
                "kind": "entity_filter",
                "concept": "Taxon",
                "owner": "Adverse Outcome Pathway",
                "value": "Human",
            },
            {
                "clause": "Return available species and methods",
                "kind": "output",
                "concept": "species and methods",
                "required": False,
            },
        ],
    )
    human = s.find("Human", str(E.Taxon))["items"][0]["ref"]
    return {
        "g1": {
            "pattern": field(s, E.AOP, E.event, "aop", "event")
            + " "
            + insert(s.catalogue.type_refs[str(E.Event)], "event"),
            "evidence": [s.catalogue.type_refs[str(E.Event)]],
            "project": ["event"],
        },
        "g2": {
            "evidence": [
                s.catalogue.field_refs[
                    (str(E.AOP), s.client.field_name(s.client.model(str(E.AOP)), str(E.taxon)))
                ]
            ],
            "entity": human,
            "pattern": field(s, E.AOP, E.taxon, "aop", "tax")
            + " VALUES ?tax { "
            + insert(human)
            + " }",
        },
        "g3": {
            "evidence": [
                s.catalogue.field_refs[
                    (str(E.Event), s.client.field_name(s.client.model(str(E.Event)), str(p)))
                ]
                for p in (E.taxon, E.method)
            ],
            "pattern": "OPTIONAL { " + field(s, E.Event, E.taxon, "event", "species") + " } "
            "OPTIONAL { " + field(s, E.Event, E.method, "event", "method") + " }",
            "project": ["species", "method"],
        },
    }


def values(s, prepared):
    done = s.finish(prepared["query_ref"])
    assert done["state"] == "complete", done
    return s.export(done["result_ref"])["bindings"]


def prepare(session, clauses, *, sparql=None):
    """Build fixture SELECTs explicitly while using the public query boundary."""
    projection = list(
        dict.fromkeys(v.lstrip("?$") for g in clauses.values() for v in g.get("project", []))
    )
    patterns = [g["pattern"].rstrip() for g in clauses.values()]
    query = (
        "SELECT DISTINCT "
        + " ".join("?" + v for v in projection)
        + " WHERE {\n"
        + "\n".join(patterns)
        + "\n}"
    )
    evidence = {key: {k: v for k, v in g.items() if k != "pattern"} for key, g in clauses.items()}
    return session.prepare(sparql or query, evidence)
