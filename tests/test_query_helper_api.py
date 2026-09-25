from unittest.mock import Mock

import pytest
import requests
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointRateLimitError,
    EndpointTimeoutError,
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
        response._content = b'{"exception":"Tried to allocate 250 MB, but only 92 MB were available"}'
        helper.max_retries = 3
        helper.initial_backoff = 0
        with pytest.raises(EndpointTimeoutError, match="Tried to allocate"):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert request.call_count == 2, "A query memory limit must reach the caller without retries"
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
        defer.assert_called_once_with("example.org", 30)
        assert request.call_count == 4, "Rejected queries must not repeat unchanged"
    sent = request.call_args.kwargs["headers"]["User-Agent"]
    assert sent.startswith("rdfsolve/") and "@" not in sent, "Identify the software, never a person"
    monkeypatch.setenv("RDFSOLVE_USER_AGENT", "my-project/1 (https://example.org/contact)")
    assert SparqlHelper("https://example.org/sparql").user_agent == "my-project/1 (https://example.org/contact)"
    assert SparqlHelper("https://example.org/sparql", user_agent="tool/2").user_agent == "tool/2"
