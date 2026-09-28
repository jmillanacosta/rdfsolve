"""Virtuoso answers a query that reaches its ANYTIME limit with incomplete results: HTTP 206 and
X-SQL-State S1TAT, with a SPARQL JSON body. Such results are not complete and must not be used
as complete (AOP-Wiki: a discovery filter then listed covered edges as uncovered)."""

from unittest.mock import Mock

import pytest
import requests
from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper

BODY = b'{"head": {"vars": ["s"]}, "results": {"bindings": [{"s": {"type": "uri", "value": "urn:a"}}]}}'


def answer(status: int, state: str | None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.headers["Content-Type"] = "application/sparql-results+json"
    if state:
        response.headers["X-SQL-State"] = state
        response.headers["X-SQL-Message"] = (
            "RC...: Returning incomplete results, query interrupted by result timeout."
        )
    response._content = BODY
    response._content_consumed = True
    return response


def test_incomplete_results_are_a_cost_limit(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    with SparqlHelper("https://example.org/sparql", max_retries=3) as helper:
        helper.initial_backoff = 0
        for status, state in ((206, "S1TAT"), (200, "S1TAT"), (206, None)):
            request = Mock(return_value=answer(status, state))
            monkeypatch.setattr(helper._session, "request", request)
            with pytest.raises(EndpointTimeoutError, match="incomplete results"):
                helper.select("SELECT ?s WHERE { ?s ?p ?o }")
            assert request.call_count == 1, "An incomplete answer is made smaller, not repeated"
        request = Mock(return_value=answer(200, None))
        monkeypatch.setattr(helper._session, "request", request)
        assert helper.select("SELECT ?s WHERE { ?s ?p ?o }")["results"]["bindings"]
