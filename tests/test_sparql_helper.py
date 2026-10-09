"""rdfsolve.sparql_helper: queries through the helper, recorded with their purpose and results, logged,
with failures reported and incomplete results detected."""

from unittest.mock import Mock

import pytest
import requests

from rdfsolve.client.query_log import QueryLog
from rdfsolve.query_collection import QueryCollection
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointRateLimitError,
    EndpointTimeoutError,
    PaginationTruncatedError,
    QueryCut,
    QueryCuts,
    SparqlHelper,
)


def test_http_errors_preserve_query_limits_and_host_limits(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    defer = Mock()
    monkeypatch.setattr("rdfsolve._http_policy.defer_host", defer)
    response = requests.Response()
    response.status_code = 500
    response.headers["Content-Type"] = "application/json"
    response._content = b"SPARQL compiler: syntax error"
    response._content_consumed = True
    with SparqlHelper("https://example.org/sparql", max_retries=1) as helper:
        helper.enable_query_collection()
        request = Mock(return_value=response)
        monkeypatch.setattr(helper._session, "request", request)
        with pytest.raises(EndpointError, match="query rejected"):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        response._content = (
            b'{"exception":"Tried to allocate 250 MB, but only 92 MB were available"}'
        )
        helper.max_retries = 3
        helper.initial_backoff = 0
        with pytest.raises(EndpointTimeoutError, match="Tried to allocate"):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert request.call_count == 2, "A query memory limit must reach the caller without retries"
        response._content = b"Virtuoso S1TAT Error Query did not complete due to ANYTIME timeout."
        with pytest.raises(EndpointTimeoutError, match="ANYTIME"):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert request.call_count == 3, "A Virtuoso time limit is a cost limit for the caller"
        helper.max_retries = 1
        response.status_code = 429
        response._content = b'{"exception":"Operation timed out: deadline exceeded"}'
        with pytest.raises(EndpointTimeoutError, match="Operation timed out"):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        defer.assert_not_called()
        record = helper.get_collected_queries()[-1]
        assert not record.success and record.status_code == 429
        assert record.attempts == 1 and record.error == "EndpointTimeoutError"
        response._content = b"Too many requests"
        response.headers["Retry-After"] = "30"
        with pytest.raises(EndpointRateLimitError):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert {c.args for c in defer.call_args_list} == {("example.org", 30)}
        assert request.call_count == 4 + 1 + SparqlHelper.OVERLOAD_RETRIES, (
            "A busy server's Retry-After is waited out, OVERLOAD_RETRIES times, whatever the "
            "retries of the step; rejected queries are not repeated"
        )
    sent = request.call_args.kwargs["headers"]["User-Agent"]
    assert sent.startswith("rdfsolve/") and "@" not in sent, "Identify the software, never a person"
    monkeypatch.setenv("RDFSOLVE_USER_AGENT", "my-project/1 (https://example.org/contact)")
    assert (
        SparqlHelper("https://example.org/sparql").user_agent
        == "my-project/1 (https://example.org/contact)"
    )
    assert SparqlHelper("https://example.org/sparql", user_agent="tool/2").user_agent == "tool/2"


def test_a_connection_closed_after_a_long_wait_is_a_cost_limit(monkeypatch, tmp_path):
    """A proxy or server that closes the connection after a long query gave up on the query.

    The caller must make the query smaller, not repeat it (Rhea through a proxy: 16 minutes
    per attempt). A connection closed at once is a network error and is retried.
    """
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    dropped = requests.exceptions.ProxyError(
        "Unable to connect to proxy', RemoteDisconnected('Remote end closed connection without response')"
    )
    with SparqlHelper("https://example.org/sparql", max_retries=3) as helper:
        helper.initial_backoff = 0
        request = Mock(side_effect=dropped)
        monkeypatch.setattr(helper._session, "request", request)
        helper.DROPPED_AFTER_SECONDS = 0.0
        with pytest.raises(EndpointTimeoutError, match="closed"):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert request.call_count == 1, "A query dropped after a long wait is not repeated"
        helper.DROPPED_AFTER_SECONDS = 1e9
        with pytest.raises(EndpointError):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert request.call_count == 4, "A connection closed at once is retried"


def test_json_error_reports_its_reason(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    response = requests.Response()
    response.status_code = 400
    response.headers["Content-Type"] = "application/json"
    response._content = b'{"exception":"Invalid SPARQL query: Built-in function sameterm"}'
    response._content_consumed = True
    with SparqlHelper("https://example.org/sparql", max_retries=1) as helper:
        monkeypatch.setattr(helper._session, "request", Mock(return_value=response))
        with pytest.raises(EndpointError, match="Built-in function sameterm"):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")


def test_a_budget_gives_one_try_and_no_pages(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    response = requests.Response()
    response.status_code = 500
    response.headers["Content-Type"] = "application/json"
    response._content = b'{"exception":"Tried to allocate 250 MB, but only 92 MB were available"}'
    response._content_consumed = True
    with SparqlHelper("https://example.org/sparql", max_retries=3, timeout=600) as helper:
        helper.initial_backoff = 0
        request = Mock(return_value=response)
        monkeypatch.setattr(helper._session, "request", request)
        with helper.budget(5):
            assert (helper.timeout, helper.max_retries, helper.page_recovery) == (5, 1, False)
            with pytest.raises(EndpointTimeoutError):
                helper.select_with_fallback("SELECT ?s WHERE { ?s ?p ?o }")
        assert request.call_count == 1, "One request: no retry, no pages"
        assert (helper.timeout, helper.max_retries, helper.page_recovery) == (600, 3, True)


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

        queries = [
            {"id": i, **asdict(record)}
            for i, record in enumerate(helper.get_collected_queries(), 1)
        ]
        html = QueryLog(
            {"queries": queries, "steps": [{"name": "<unsafe>", "query_ids": [3]}]}
        )._repr_html_()
        assert queries[1]["result"]["results"]["bindings"] == []
        assert "Response not retained" in html and "0 rows" in html
        assert "False" in html and "Failed" in html and ("EndpointError" in html)
        assert "<script>" not in html and "<unsafe>" not in html
        assert "&lt;script&gt;" in html and "&lt;unsafe&gt;" in html


def test_paging_recovers_timeout_and_short_server_pages(monkeypatch):
    import json
    import re

    from rdflib import Graph

    from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper
    from tests.client.test_api import DATA

    graph = Graph().parse(DATA, format="turtle")
    query = "SELECT ?s ?title WHERE { ?s <http://purl.org/dc/elements/1.1/title> ?title } ORDER BY ?s ?title"
    expected = json.loads(graph.query(query).serialize(format="json"))["results"]["bindings"]
    calls = []

    def select(query, **kwargs):
        offset = int(re.search("OFFSET (\\d+)", query)[1])
        size = int(re.search("LIMIT (\\d+)", query)[1])
        calls.append((offset, size))
        if len(calls) == 1:
            raise EndpointTimeoutError("The first page timed out")
        return {"results": {"bindings": expected[offset : offset + min(size, 2)]}}

    monkeypatch.setattr("rdfsolve.sparql_helper.time.sleep", lambda _: None)
    with SparqlHelper("https://example.org/sparql") as helper:
        monkeypatch.setattr(helper, "select", select)
        actual = [
            row
            for page in helper.select_chunked(
                helper.prepare_paginated_query(query),
                chunk_size=8,
                max_pages=None,
                until_empty=True,
                stable_terms=True,
            )
            for row in page
        ]
        assert actual == expected and calls[:2] == [(0, 8), (0, 4)]
        assert calls[-1][0] == len(expected)
        monkeypatch.setattr(
            helper, "select", lambda *a, **k: {"results": {"bindings": expected[:2]}}
        )
        with pytest.raises(PaginationTruncatedError, match="repeated a page"):
            list(
                helper.select_chunked(
                    helper.prepare_paginated_query(query),
                    chunk_size=8,
                    max_pages=None,
                    until_empty=True,
                )
            )


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


def _cut(seconds: float = 128.0, kind: str = "HTTP 524") -> EndpointError:
    error = EndpointError(f"{kind}: A timeout occurred")
    error.cut = QueryCut(kind, seconds)
    return error


def test_a_gateway_timeout_or_a_late_close_is_a_cut_and_the_own_timeout_is_not(
    monkeypatch, tmp_path
):
    """The helper says how a query was cut: a gateway-timeout status, or a server error or a
    connection closed after a long wait. A quick refusal and the client's own read timeout
    (the limit the caller set) are not cuts."""
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    response = requests.Response()
    response.status_code = 524
    response.headers["Content-Type"] = "application/json"
    response._content = b'{"title":"Error 524: A timeout occurred","status":524}'
    response._content_consumed = True
    dropped = requests.exceptions.ProxyError(
        "RemoteDisconnected('Remote end closed connection without response')"
    )
    with SparqlHelper("https://example.org/sparql", max_retries=1) as helper:
        request = Mock(return_value=response)
        monkeypatch.setattr(helper._session, "request", request)
        with pytest.raises(EndpointError, match="HTTP 524") as raised:
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert raised.value.cut is not None and raised.value.cut.kind == "HTTP 524"
        response.status_code = 500
        response._content = b"Internal error"
        with pytest.raises(EndpointError) as raised:
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert raised.value.cut is None, "A server error at once is a refusal, not a cut"
        helper.DROPPED_AFTER_SECONDS = 0.0
        with pytest.raises(EndpointError) as raised:
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert raised.value.cut.kind == "HTTP 500", "A server error after a long wait is a cut"
        request.side_effect = dropped
        with pytest.raises(EndpointTimeoutError, match="closed") as raised:
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert raised.value.cut.kind == "connection closed without a response"
        request.side_effect = requests.exceptions.ReadTimeout("Read timed out")
        with pytest.raises(EndpointTimeoutError) as raised:
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert raised.value.cut is None, "The client's own limit is not a cut of the endpoint"


def test_a_step_stops_after_three_cuts_at_the_same_limit():
    cuts = QueryCuts()
    assert cuts.after == 3
    for _ in range(2):
        assert cuts.failed("nav", _cut()) is None
    assert not cuts.skip("nav"), "Two cuts can be two slow queries"
    assert cuts.failed("nav", _cut(128.4)) == "endpoint cuts queries at 128 s"
    assert cuts.skip("nav") and cuts.skip("nav")
    assert not cuts.skip("other"), "Another purpose of the step goes on"
    assert cuts.skip("other", whole_step=True)
    assert cuts.record() == {
        "nav": {
            "stopped": "endpoint cuts queries at 128 s",
            "cut_by": "HTTP 524",
            "queries_cut": 3,
            "queries_not_sent": 2,
        },
        "other": {
            "stopped": "endpoint cuts queries at 128 s",
            "cut_by": None,
            "queries_cut": 0,
            "queries_not_sent": 1,
        },
    }


def test_an_answer_or_another_limit_starts_the_count_again():
    cuts = QueryCuts()
    cuts.failed("nav", _cut())
    cuts.failed("nav", _cut())
    cuts.answered("nav")
    cuts.failed("nav", _cut())
    cuts.failed("nav", _cut())
    assert cuts.stopped("nav") is None, "An answer resets the count"
    cuts.failed("nav", _cut(900.0, "connection closed without a response"))
    cuts.failed("nav", _cut(60.0))
    cuts.failed("nav", _cut(128.0))
    assert cuts.stopped("nav") is None, "Cuts of another kind or time are another limit"
    cuts.failed("nav", EndpointError("HTTP 400: syntax error"))
    cuts.failed("nav", _cut(128.0))
    assert cuts.stopped("nav") is None, "A refused query neither counts nor resets"
    cuts.failed("nav", _cut(128.0))
    assert cuts.stopped("nav") == "endpoint cuts queries at 128 s"
    assert cuts.record()["nav"]["queries_cut"] == 9
