"""rdfsolve.endpoint_health: an endpoint is up when it answers a one-row SELECT, which every SPARQL
engine supports (QLever refuses ASK), also when it holds no triples; a request that fails is
told apart from an empty endpoint."""

from unittest.mock import Mock

from rdfsolve.endpoint_health import check_endpoint_health
from rdfsolve.sparql_helper import EndpointTimeoutError


def test_health_asks_a_one_row_select_and_tells_an_empty_endpoint_from_a_timeout(monkeypatch):
    helper = Mock()
    helper.ask.side_effect = AssertionError("ASK is refused by QLever endpoints")
    helper.select.side_effect = [
        {"results": {"bindings": []}},
        EndpointTimeoutError("request timed out"),
    ]
    monkeypatch.setattr("rdfsolve.endpoint_health.SparqlHelper", Mock(return_value=helper))
    empty = check_endpoint_health("https://example.org/sparql")
    failed = check_endpoint_health("https://example.org/sparql")
    assert (empty.status, empty.error_message) == ("up", ""), "Empty but responsive endpoint"
    assert (failed.status, failed.error_message) == ("timeout", "request timed out")
    assert "LIMIT 1" in helper.select.call_args_list[0].args[0]
    assert helper.close.call_count == 2, "Both requests release the helper"
