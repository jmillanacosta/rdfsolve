"""Exhaustive SELECT pages keep grouped results complete and order them by the group keys."""

import json

import pytest
from rdflib import Graph, Literal, URIRef

from rdfsolve.sparql_helper import QueryError, SparqlHelper


@pytest.fixture
def paged(monkeypatch):
    """A helper whose requests run on a local graph, two rows per page."""
    graph = Graph()
    for i in range(5):
        for j in range(i + 1):
            graph.add((URIRef(f"urn:s{j}"), URIRef("urn:p"), Literal(f"k{i}")))
    helper = SparqlHelper("http://example.invalid/sparql", inter_request_delay=0)
    helper.select_page_size = 2
    sent = []

    def select(query, **_):
        sent.append(query)
        return json.loads(graph.query(query).serialize(format="json"))

    monkeypatch.setattr(helper, "select", select)
    return helper, sent


def test_grouped_pages_are_ordered_by_group_keys_not_by_aggregates(paged):
    helper, sent = paged
    query = (
        "SELECT ?k (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s <urn:p> ?k } "
        "GROUP BY ?k HAVING (COUNT(DISTINCT ?s) > 1)"
    )
    rows = helper.select_with_fallback(query, exhaustive=True)["results"]["bindings"]
    assert {r["k"]["value"]: r["n"]["value"] for r in rows} == {"k1": "2", "k2": "3", "k3": "4", "k4": "5"}
    assert len(sent) == 3, "Two full pages and one empty page"
    assert all("?n" not in q.split("ORDER BY")[1] for q in sent), "Virtuoso refuses aggregate aliases"


def test_one_group_needs_no_order_and_unnamed_group_keys_cannot_be_paged(paged):
    helper, sent = paged
    rows = helper.select_with_fallback(
        "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?k }", exhaustive=True
    )["results"]["bindings"]
    assert [r["c"]["value"] for r in rows] == ["15"]
    assert "ORDER BY" not in sent[0]
    named = "SELECT ?g (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?k } GROUP BY (STR(?k) AS ?g)"
    assert len(helper.select_with_fallback(named, exhaustive=True)["results"]["bindings"]) == 5
    with pytest.raises(QueryError, match="GROUP BY"):
        helper.select_with_fallback(
            "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?k } GROUP BY STR(?k)", exhaustive=True
        )
