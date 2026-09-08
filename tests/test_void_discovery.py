"""Run discovery queries against the saved AOPWiki schema."""

import json
from unittest.mock import patch

import pytest
from rdflib import Dataset
from rdflib.compare import isomorphic

from rdfsolve.api import discover_void_graphs, discover_void_source
from rdfsolve.sparql_helper import EndpointUnhealthyError, PaginationTruncatedError, SparqlHelper
from tests.test_navigation import aop_schema


@pytest.fixture
def catalog():
    dataset = Dataset()
    graph_uri = "http://rdfsolve.org/graph/aopwikirdf"
    graph = dataset.graph(graph_uri)
    graph.parse(data=aop_schema().to_void_graph().serialize(format="turtle"),
                format="turtle", publicID="https://example.org/sparql")

    def select(self, query, **kwargs):
        return json.loads(dataset.query(query).serialize(format="json"))

    def construct(self, query):
        return dataset.query(query).serialize(format="turtle").decode()

    with patch.object(SparqlHelper, "select", select), patch.object(
        SparqlHelper, "construct", construct
    ):
        yield dataset, graph_uri, graph


def test_discovery_preserves_published_rdf(catalog, tmp_path):
    _, graph_uri, graph = catalog
    result = discover_void_graphs(
        "https://example.org/sparql", graph_uris=[graph_uri], batch_size=1
    )
    assert isomorphic(result["graph"], graph)
    expected = aop_schema()
    assert len(result["schema"].patterns) == len(expected.patterns)
    assert result["found_graphs"] == [graph_uri]
    assert not discover_void_graphs(
        "https://example.org/sparql", graph_uris=["urn:absent"]
    )["has_void_descriptions"]
    with pytest.raises(ValueError, match="select at least one graph"):
        discover_void_graphs("https://example.org/sparql", graph_uris=[])
    exported = discover_void_source(
        "https://example.org/sparql", "aopwiki", tmp_path,
        graph_uris=[graph_uri], fmt="void",
    )
    from rdfsolve.schema_models.core import MinedSchema

    stored = MinedSchema.from_dict(json.loads(
        (tmp_path / "aopwiki_discovered_remote_schema.json").read_text()
    ))
    assert len(stored.patterns) == len(expected.patterns)
    assert "schema_json" in exported.files


def test_discovery_uses_graph_names_and_stops_at_page_limit(catalog):
    _, graph_uri, _ = catalog
    assert discover_void_graphs("https://example.org/sparql")["found_graphs"] == [graph_uri]
    with pytest.raises(PaginationTruncatedError):
        discover_void_graphs("https://example.org/sparql", batch_size=1, max_pages=1)


def test_discovery_failure_is_not_an_empty_export(tmp_path):
    with patch.object(SparqlHelper, "select", side_effect=EndpointUnhealthyError("unavailable")):
        with pytest.raises(PaginationTruncatedError, match="unavailable"):
            discover_void_source("https://example.org/sparql", "source", tmp_path / "output")
    assert not (tmp_path / "output").exists()


def test_discovery_keeps_all_graphs_after_a_void_named_match(catalog):
    dataset, graph_uri, source = catalog
    # Put the same real export in differently named contexts to check coverage.
    dataset.graph("urn:well-known:void").__iadd__(source)
    dataset.default_context.__iadd__(source)
    document = discover_void_source("https://example.org/sparql", "aopwiki")
    assert set(document.graph_uris) == {graph_uri, "urn:well-known:void"}
    assert document.default_graph
    for uri in document.graph_uris:
        view = document.for_graph(uri)
        assert isomorphic(view.graph, source)
        assert view.has_void and view.has_patterns
        assert view.to_mined_schema().about.graph_uris is None
        assert view.to_mined_schema().about.metadata_graph_uris == [uri]
    assert isomorphic(document.for_graph(None).graph, source)
    restored = Dataset().parse(data=document.to_trig(), format="trig")
    assert isomorphic(restored.graph(graph_uri), source)
    assert isomorphic(document.get_metadata().for_graph(graph_uri).graph, source)
