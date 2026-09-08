"""Exercise public query helpers without endpoint requests."""

from pathlib import Path
from unittest.mock import Mock

import pytest
from rdflib import Graph
from rdflib.compare import isomorphic

from rdfsolve.sparql_helper import EndpointError, SparqlHelper


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
