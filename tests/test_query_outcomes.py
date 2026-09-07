"""Check empty, failed, and partially recovered query groups."""

from unittest.mock import Mock

import pytest

from rdfsolve.mining.query_builders import _build_batched_literal_query
from rdfsolve.mining.query_fallbacks import query_with_bisect, typed_object_by_property
from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError, PaginationTruncatedError


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


def test_empty_success_is_complete():
    result = run_query(Mock(select=Mock(return_value=response([]))))
    assert result.rows == []
    assert result.state == "complete"
    assert result.failures == []


def test_endpoint_failure_is_not_empty_success():
    collect = Mock()
    result = run_query(Mock(select=Mock(side_effect=EndpointError("offline"))), collect=collect)
    assert result.rows == []
    assert result.state == "failed"
    assert result.failures[0].category == "endpoint"
    assert result.failures[0].classes == ["urn:A"]
    assert result.failures[0].graph_uris == ["urn:graph"]
    collect.assert_not_called()


@pytest.mark.parametrize("rows", [[], [binding()]])
def test_bisection_preserves_success_and_failure(rows):
    helper = Mock(
        select=Mock(
            side_effect=[
                EndpointTimeoutError("split"),
                response(rows),
                EndpointError("offline"),
            ]
        )
    )
    result = run_query(helper, ["urn:A", "urn:B"])
    assert result.rows == rows
    assert result.state == "partial"
    assert result.failures[0].classes == ["urn:B"]


def test_bisection_can_recover_completely():
    helper = Mock(
        select=Mock(side_effect=[EndpointTimeoutError("split"), response([]), response([])])
    )
    result = run_query(helper, ["urn:A", "urn:B"])
    assert result.state == "complete"
    assert not result.failures


def test_bisection_can_fail_completely():
    helper = Mock(
        select=Mock(
            side_effect=[
                EndpointTimeoutError("split"),
                EndpointError("left"),
                EndpointError("right"),
            ]
        )
    )
    result = run_query(helper, ["urn:A", "urn:B"])
    assert result.state == "failed"
    assert len(result.failures) == 2


def test_pagination_preserves_rdf_term_identity():
    integer = binding(term={"type": "literal", "value": "1", "datatype": "urn:integer"})
    text = binding(term={"type": "literal", "value": "1", "datatype": "urn:string"})
    helper = Mock(select=Mock(side_effect=EndpointTimeoutError("page")))
    result = run_query(helper, collect=Mock(return_value=[integer, integer, text]))
    assert result.state == "complete"
    assert result.rows == [integer, text]


def test_exhausted_pagination_records_failure():
    helper = Mock(select=Mock(side_effect=EndpointTimeoutError("page")))
    result = run_query(
        helper, collect=Mock(side_effect=PaginationTruncatedError("cut", offset=100))
    )
    assert result.state == "failed"
    assert result.failures[0].category == "truncated"


def test_partial_pagination_keeps_rows():
    error = PaginationTruncatedError("cut", offset=100)
    error.partial_rows = [binding()]
    result = run_query(
        Mock(select=Mock(side_effect=EndpointTimeoutError("page"))),
        collect=Mock(side_effect=error),
    )
    assert result.state == "partial"
    assert result.rows == [binding()]


@pytest.mark.parametrize("raw", [{}, {"results": {}}, {"results": {"bindings": "invalid"}}])
def test_invalid_response_is_not_empty_success(raw):
    result = run_query(Mock(select=Mock(return_value=raw)))
    assert result.state == "failed"
    assert result.failures[0].category == "invalid_response"


def test_programming_errors_are_not_retried():
    helper = Mock(select=Mock(side_effect=RuntimeError("bug")))
    with pytest.raises(RuntimeError, match="bug"):
        run_query(helper)


def test_cancellation_propagates():
    helper = Mock(select=Mock(side_effect=KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        run_query(helper)


def test_empty_class_batch_does_not_query():
    helper = Mock()
    result = query_with_bisect([], None, _build_batched_literal_query, "empty", helper, Mock(), 100)
    assert result.state == "complete"
    helper.select.assert_not_called()


def test_property_failure_preserves_other_properties():
    helper = Mock(
        select=Mock(
            side_effect=[
                response([{"p": {"value": "urn:p"}}, {"p": {"value": "urn:q"}}]),
                response([{"oc": {"value": "urn:B"}}]),
                EndpointError("offline"),
            ]
        )
    )
    result = typed_object_by_property("urn:A", ["urn:graph"], "test/typed", helper, Mock(), 100)
    assert result.state == "partial"
    assert len(result.rows) == 1
    assert result.rows[0]["p"]["value"] == "urn:p"
    assert "urn:q" in result.failures[0].purpose
