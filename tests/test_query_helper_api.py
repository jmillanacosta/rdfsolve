"""Exercise public query helpers without endpoint requests."""

from pathlib import Path
from unittest.mock import Mock

import pytest
from rdflib import Graph
from rdflib.compare import isomorphic

from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError, SparqlHelper


def test_rdf_response_is_parsed_or_raises(monkeypatch):
    text = (Path(__file__).parent / "test_data/aopwikirdf_metadata_excerpt.ttl").read_text()
    with SparqlHelper("https://example.org/sparql") as helper:
        construct = Mock(return_value=text)
        monkeypatch.setattr(helper, "construct", construct)
        assert isomorphic(helper.construct_graph("CONSTRUCT {} WHERE {}"),
                          Graph().parse(data=text, format="turtle"))
        construct.return_value = "<html>gateway error</html>"
        with pytest.raises(EndpointError, match="invalid Turtle"):
            helper.construct_graph("CONSTRUCT {} WHERE {}")


def test_ask_does_not_turn_bad_responses_into_false(monkeypatch):
    with SparqlHelper("https://example.org/sparql") as helper:
        execute = Mock(return_value={"boolean": False})
        monkeypatch.setattr(helper, "_execute", execute)
        assert helper.ask("ASK {}") is False
        for invalid in ({}, {"boolean": "unknown"}, {"boolean": 1}):
            execute.return_value = invalid
            with pytest.raises(EndpointError, match="boolean"):
                helper.ask("ASK {}")


def test_registry_helper_uses_request_budgets():
    with SparqlHelper.from_source_entry({
        "endpoint": "https://example.org/sparql", "timeout": 12,
        "delay": 2, "max_response_bytes": 2048, "sparql_strategy": "post",
    }) as helper:
        assert (helper.timeout, helper.inter_request_delay, helper.max_response_bytes) == (12, 2, 2048)
        assert helper._requires_post
    with pytest.raises(ValueError, match="endpoint"):
        SparqlHelper.from_source_entry({"name": "no-endpoint"})


@pytest.mark.parametrize("method", ["find_classes_for_iris", "find_classes_for_iris_by_graph"])
def test_class_lookup_does_not_hide_failed_batches(method, monkeypatch):
    with SparqlHelper("https://example.org/sparql") as helper:
        select = Mock(side_effect=EndpointError("unavailable"))
        monkeypatch.setattr(helper, "select", select)
        lookup = getattr(helper, method)
        with pytest.raises(ValueError):
            lookup(["urn:s"], values_batch_size=0)
        with pytest.raises(ValueError):
            lookup(["urn:s> } UNION { ?s ?p ?o"])
        select.assert_not_called()
        with pytest.raises(EndpointError, match="unavailable"):
            lookup(["urn:s"])


def test_fallback_log_reports_transport_change(monkeypatch, caplog):
    import logging

    caplog.set_level(logging.INFO, logger="rdfsolve.sparql_helper")
    with SparqlHelper("https://example.org/sparql", max_retries=1) as helper:
        helper.enable_query_collection()
        monkeypatch.setattr(helper, "_get_query", Mock(return_value="<html>error</html>"))
        monkeypatch.setattr(helper, "_post_query", Mock(return_value='{"boolean": true}'))
        assert helper.ask("ASK {}")
        assert "HTTP fallback used" in caplog.text
        assert helper.get_collected_queries()[-1].fallback_used
        assert helper.get_collected_queries()[-1].attempts == 2
        caplog.clear()
        assert helper.ask("ASK {}")
        assert "no HTTP fallback" in caplog.text
        assert not helper.get_collected_queries()[-1].fallback_used
        assert helper.get_collected_queries()[-1].attempts == 1
        assert "ASK {}" not in caplog.text


@pytest.mark.parametrize("body,category,calls", [
    ("Virtuoso 37000 Error SP030: SPARQL compiler: syntax error", EndpointError, 1),
    ("Query timed out", EndpointTimeoutError, 1),
    ("Internal server error", EndpointError, 2),
])
def test_http_500_recovery_depends_on_error_body(monkeypatch, body, category, calls):
    import requests

    with SparqlHelper("https://example.org/sparql", max_retries=2, initial_backoff=0) as helper:
        helper.enable_query_collection()
        def fail(*args, **kwargs):
            helper._last_error_body = body
            response = requests.Response()
            response.status_code = 500
            raise requests.HTTPError("HTTP 500", response=response)
        request = Mock(side_effect=fail)
        monkeypatch.setattr(helper, "_get_query", request)
        with pytest.raises(category):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert request.call_count == calls
        record = helper.get_collected_queries()[-1]
        assert not record.success and record.status_code == 500
        assert record.response_excerpt == body and record.error_message


def test_long_queries_use_post_without_first_sending_a_long_url(monkeypatch):
    with SparqlHelper("https://example.org/sparql") as helper:
        get = Mock(side_effect=AssertionError("Do not send a long URL"))
        post = Mock(return_value='{"boolean": true}')
        monkeypatch.setattr(helper, "_get_query", get)
        monkeypatch.setattr(helper, "_post_query", post)
        assert helper.ask("ASK { VALUES ?s { " + "<https://identifiers.org/aop/162> " * 100 + "} }")
        assert post.call_count == 1
