"""Executed queries are recorded at once and become portable examples only when read."""

from unittest.mock import Mock

from rdfsolve.query_collection import QueryCollection
from rdfsolve.sparql_helper import SparqlHelper


def test_executions_become_examples_when_the_collection_is_read(monkeypatch):
    added = []
    add = QueryCollection.add
    monkeypatch.setattr(
        QueryCollection, "add", lambda self, *a, **k: added.append(a) or add(self, *a, **k)
    )
    with SparqlHelper("https://example.org/sparql") as helper:
        response = {"head": {"vars": ["s"]}, "results": {"bindings": []}}
        monkeypatch.setattr(helper, "_execute_request", Mock(return_value=response))
        helper.enable_query_collection()
        for limit in (1, 2, 2):
            helper.select(f"SELECT ?s WHERE {{ ?s ?p ?o }} LIMIT {limit}")
        assert len(helper.get_collected_queries()) == 3, "Each execution is recorded at once"
        assert not added, "No query is parsed while it runs"
        assert len(helper.queries.shacl.queries) == 2, "Reading the collection adds the examples"
        assert len(added) == 2, "A repeated text is one example"
