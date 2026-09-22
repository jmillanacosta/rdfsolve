from unittest.mock import Mock

from rdfsolve.endpoint_health import check_endpoint_health
from rdfsolve.sparql_helper import EndpointTimeoutError


def test_health_distinguishes_empty_endpoint_from_timeout(monkeypatch):
    helper = Mock()
    helper.ask.side_effect = [False, EndpointTimeoutError("request timed out")]
    monkeypatch.setattr("rdfsolve.endpoint_health.SparqlHelper", Mock(return_value=helper))
    empty = check_endpoint_health("https://example.org/sparql")
    failed = check_endpoint_health("https://example.org/sparql")
    assert (empty.status, empty.error_message) == ("up", ""), "Empty but responsive endpoint"
    assert (failed.status, failed.error_message) == ("timeout", "request timed out"), (
        "Failed request"
    )
    assert helper.close.call_count == 2, "Both requests release the helper"
