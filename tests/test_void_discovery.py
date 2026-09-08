"""Check scoped VoID discovery against a saved AOPWiki RDF export."""

import json
from unittest.mock import Mock, patch

import pytest
from rdflib import Dataset

from rdfsolve.api import discover_void_graphs, discover_void_source
from rdfsolve.sparql_helper import EndpointTimeoutError
from tests.test_navigation import aop_schema


def test_discovery_honors_scope_and_request_budget():
    dataset = Dataset()
    graph_uri = "http://rdfsolve.org/graph/aopwikirdf"
    graph = dataset.graph(graph_uri)
    graph += aop_schema().to_void_graph()
    helper = Mock()
    helper.select.side_effect = lambda query, **kwargs: json.loads(
        dataset.query(query).serialize(format="json")
    )
    with patch("rdfsolve.sparql_helper.SparqlHelper", return_value=helper) as factory:
        result = discover_void_graphs("https://example.org/sparql", graph_uris=[graph_uri],
                                      timeout=7, max_retries=1)
        assert result["partitions"]
        assert result["found_graphs"] == [graph_uri]
        factory.assert_called_with("https://example.org/sparql", timeout=7, max_retries=1)
        assert not discover_void_graphs(
            "https://example.org/sparql", graph_uris=["urn:absent"]
        )["partitions"]
        with pytest.raises(ValueError, match="select at least one graph"):
            discover_void_graphs("https://example.org/sparql", graph_uris=[])


def test_discovery_failure_is_not_an_empty_export(tmp_path):
    with patch("rdfsolve.sparql_helper.SparqlHelper") as factory:
        factory.return_value.select.side_effect = EndpointTimeoutError("request timed out")
        with pytest.raises(EndpointTimeoutError, match="timed out"):
            discover_void_source("https://example.org/sparql", "source", tmp_path / "output",
                                 timeout=5)
    assert not (tmp_path / "output").exists()
