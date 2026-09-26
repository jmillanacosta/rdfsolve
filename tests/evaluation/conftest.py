"""A small graph where resources have names, identifiers and pages."""

import json

import pytest
from rdflib import DC, DCTERMS, FOAF, RDF, RDFS, Graph, Literal, Namespace

E = Namespace("https://evaluation-test.invalid/")


@pytest.fixture
def graph():
    g = Graph()
    for n in (1, 2):
        event = E[f"ke{n}"]
        g.add((event, RDF.type, E.Event))
        g.add((event, RDFS.label, Literal(f"KE {n}")))
        g.add((event, DC.title, Literal(f"Event title {n}")))
        g.add((event, DC.identifier, event))
        g.add((event, FOAF.page, E[f"page{n}"]))
        g.add((event, DCTERMS.description, Literal("Shared text")))
    g.add((E.gene, RDFS.label, Literal("TPO")))
    g.add((E.gene, E.hgnc, Literal("11040")))
    return g


@pytest.fixture
def select(graph):
    """Run a SELECT on the graph and give SPARQL JSON bindings."""
    queries = []

    def run(query):
        queries.append(query)
        return json.loads(graph.query(query).serialize(format="json"))["results"]["bindings"]

    run.queries = queries
    return run


def uri(value):
    return {"type": "uri", "value": str(value)}


def text(value, **extra):
    return {"type": "literal", "value": value, **extra}
