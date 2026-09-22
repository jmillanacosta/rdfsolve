from unittest.mock import Mock

from rdfsolve.mining.query_builders import _build_batched_literal_query
from rdfsolve.mining.query_fallbacks import query_with_bisect
from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError


def response(rows):
    return {"results": {"bindings": rows}}


def binding(value="urn:A", **terms):
    return {"class": {"type": "uri", "value": value}, **terms}


def run_query(helper, classes=None, collect=None):
    return query_with_bisect(
        classes or ["urn:A"],
        ["urn:graph"],
        _build_batched_literal_query,
        "test/literal",
        helper,
        collect or Mock(),
        100,
    )


def test_bisection_preserves_success_and_failure():
    rows = [binding()]
    helper = Mock(
        select=Mock(
            side_effect=[EndpointTimeoutError("split"), response(rows), EndpointError("offline")]
        )
    )
    result = run_query(helper, ["urn:A", "urn:B"])
    assert result.rows == rows
    assert result.state == "partial"
    assert result.failures[0].classes == ["urn:B"]
