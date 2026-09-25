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


    from rdfsolve.mining.query_builders import _build_batched_typed_object_query

    typed = {"oc": {"type": "uri", "value": "urn:B"}}
    helper = Mock(select=Mock(side_effect=[
        EndpointTimeoutError("Class join exceeds the query budget"),
        response([{"p": {"type": "uri", "value": "urn:p"}}]),
        response([typed]),
    ]))
    collect = Mock(side_effect=AssertionError("Split the timed-out join before repeating pages"))
    result = query_with_bisect(
        ["urn:A"], ["urn:graph"], _build_batched_typed_object_query,
        "test/typed", helper, collect, 100,
    )
    assert result.state == "complete", result.failures
    assert result.rows == [binding(p={"type": "uri", "value": "urn:p"}, **typed)]
    assert not collect.called

    from rdflib import Graph

    from rdfsolve.mining.local_graph import LocalGraphHelper

    graph = Graph().parse(data='<urn:a> a <urn:A>; <urn:p> "x" . <urn:b> a <urn:A>; <urn:q> 1 .', format="turtle")
    local = LocalGraphHelper("local", graph)
    select = local.select

    def whole_class_times_out(query, **kwargs):
        if "SELECT ?s WHERE" not in query:  # only member windows fit the budget
            raise EndpointTimeoutError("Operation timed out")
        return select(query, **kwargs)

    local.select = whole_class_times_out
    pages = Mock(side_effect=EndpointTimeoutError("Operation timed out"))
    result = query_with_bisect(["urn:A"], None, _build_batched_literal_query, "test/literal", local, pages, 100)
    assert {r["p"]["value"] for r in result.rows} == {"urn:p", "urn:q"}, "Windows still observe rows"
    assert result.state == "partial" and result.failures[-1].category == "sampled", "Never complete"
    assert "windows" in result.failures[-1].message
