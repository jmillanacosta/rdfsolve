"""rdfsolve.sparql_terms: a term that is not an RDF IRI (a space in it) is written into a query
with IRI("..."), in a triple, a filter, a VALUES block and a projection, and the rewritten query
finds the data; a query without such a term is unchanged."""

import logging

import pytest
from rdflib import Dataset, Literal, URIRef

from rdfsolve.mining.local_graph import LocalGraphHelper
from rdfsolve.sparql_terms import writable_query

BAD = "urn:bad name"


@pytest.fixture
def helper():
    logging.getLogger("rdflib.term").setLevel(logging.ERROR)
    data = Dataset()
    data.add((URIRef("urn:s"), URIRef(BAD), Literal("x")))
    data.add((URIRef("urn:s"), URIRef("urn:p"), URIRef(BAD)))
    return LocalGraphHelper("urn:local", data, backend="rdflib")


def rows(helper, query):
    answer = helper.select(query)["results"]["bindings"]
    return sorted(tuple(sorted((k, v["value"]) for k, v in row.items())) for row in answer)


def test_a_query_without_such_a_term_is_unchanged():
    query = 'SELECT ?s WHERE { ?s <urn:p> "<urn:a b>" . FILTER(?n < 5) } # <urn:c d>'
    assert writable_query(query) == query
    several = f"SELECT * WHERE {{ VALUES (?a ?b) {{ (<urn:p> <{BAD}>) }} }}"
    assert writable_query(several) == several, "Left to the engine, which refuses it"


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (f"SELECT ?s ?o WHERE {{ ?s <{BAD}> ?o }}", [(("o", "x"), ("s", "urn:s"))]),
        (f"SELECT ?s WHERE {{ ?s <urn:p> <{BAD}> }}", [(("s", "urn:s"),)]),
        (f"SELECT ?s ?o WHERE {{ ?s ?p ?o FILTER(?p = <{BAD}>) }}", [(("o", "x"), ("s", "urn:s"))]),
        (
            f"SELECT ?p WHERE {{ VALUES ?p {{ <urn:p> <{BAD}> }} ?s ?p ?o }}",
            [(("p", BAD),), (("p", "urn:p"),)],
        ),
        (
            f"SELECT ?p WHERE {{ VALUES (?p) {{ (<urn:p>) (<{BAD}>) }} ?s ?p ?o }}",
            [(("p", BAD),), (("p", "urn:p"),)],
        ),
        (
            f"SELECT ?s WHERE {{ ?s ?q ?v FILTER NOT EXISTS {{ ?s <{BAD}> ?o }} }}",
            [],
        ),
        (
            (
                f"SELECT ?c ?n WHERE {{ {{ SELECT (<{BAD}> AS ?c) (COUNT(*) AS ?n) "
                f"WHERE {{ ?s <{BAD}> ?o }} }} }}"
            ),
            [(("c", BAD), ("n", "1"))],
        ),
    ],
)
def test_the_rewritten_query_finds_the_term(helper, query, expected):
    rewritten = writable_query(query)
    assert f"<{BAD}>" not in rewritten and f'IRI("{BAD}")' in rewritten
    assert rows(helper, rewritten) == expected
