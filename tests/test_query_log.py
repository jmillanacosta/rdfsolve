"""Check response retention and safe rendering without endpoint requests."""

from unittest.mock import Mock

import pytest

from rdfsolve.query_log import QueryLog
from rdfsolve.sparql_helper import EndpointError, SparqlHelper


def test_responses_are_opt_in_isolated_and_distinct_from_failures(monkeypatch):
    with SparqlHelper("https://example.org/sparql") as helper:
        response = {"head": {"vars": ["value"]}, "results": {"bindings": []}}
        execute = Mock(return_value=response)
        monkeypatch.setattr(helper, "_execute_request", execute)
        helper.enable_query_collection()
        helper.select("SELECT ?value WHERE {}")
        assert not helper.get_collected_queries()[-1].result_retained
        helper.enable_query_collection(clear=False, include_results=True)
        helper.select("SELECT ?value WHERE {}")
        response["results"]["bindings"].append({"value": {"type": "literal", "value": "<script>"}})
        helper.select("SELECT ?value WHERE {}")
        execute.return_value = {"boolean": False}
        assert helper.ask("ASK {}") is False
        execute.side_effect = EndpointError("offline")
        with pytest.raises(EndpointError):
            helper.ask("ASK {}")
        from dataclasses import asdict
        queries = [{"id": i, **asdict(record)} for i, record in enumerate(helper.get_collected_queries(), 1)]
        html = QueryLog({"queries": queries, "steps": [{"name": "<unsafe>", "query_ids": [3]}]})._repr_html_()
        assert queries[1]["result"]["results"]["bindings"] == []
        assert "Response not retained" in html and "0 rows" in html
        assert "False" in html and "Failed" in html and "EndpointError" in html
        assert "<script>" not in html and "<unsafe>" not in html
        assert "&lt;script&gt;" in html and "&lt;unsafe&gt;" in html
