"""rdfsolve.void_retrieval: VoID descriptions are discovered at an endpoint, with the identity of a
service's graphs."""

from __future__ import annotations

import json
from unittest.mock import Mock, patch

import pytest
from rdflib import RDF, Dataset, Namespace, URIRef
from rdflib.compare import isomorphic

from rdfsolve.sparql_helper import SparqlHelper
from rdfsolve.void_retrieval import retrieve_description
from rdfsolve.void_source import discover_void_graphs, discover_void_source
from tests.mining.test_navigation import aop_schema


@pytest.fixture
def catalog():
    dataset = Dataset()
    graph_uri = "http://rdfsolve.org/graph/aopwikirdf"
    graph = dataset.graph(graph_uri)
    graph.parse(
        data=aop_schema().to_void_graph().serialize(format="turtle"),
        format="turtle",
        publicID="https://example.org/sparql",
    )

    def select(self, query, **kwargs):
        return json.loads(dataset.query(query).serialize(format="json"))

    def construct(self, query):
        return dataset.query(query).serialize(format="turtle").decode()

    with (
        patch.object(SparqlHelper, "select", select),
        patch.object(SparqlHelper, "construct", construct),
    ):
        yield (dataset, graph_uri, graph)


def test_discovery_preserves_published_rdf(catalog, tmp_path):
    _, graph_uri, graph = catalog
    result = discover_void_graphs(
        "https://example.org/sparql", graph_uris=[graph_uri], batch_size=1
    )
    assert isomorphic(result["graph"], graph)
    expected = aop_schema()
    assert len(result["schema"].patterns) == len(expected.patterns)
    assert result["found_graphs"] == [graph_uri]
    assert not discover_void_graphs("https://example.org/sparql", graph_uris=["urn:absent"])[
        "has_void_descriptions"
    ]
    with pytest.raises(ValueError, match="select at least one graph"):
        discover_void_graphs("https://example.org/sparql", graph_uris=[])
    exported = discover_void_source(
        "https://example.org/sparql", "aopwiki", tmp_path, graph_uris=[graph_uri], fmt="void"
    )
    from rdfsolve.schema_models.core import MinedSchema

    stored = MinedSchema.from_dict(
        json.loads((tmp_path / "aopwiki_discovered_remote_schema.json").read_text())
    )
    assert len(stored.patterns) == len(expected.patterns)
    assert "schema_json" in exported.files


VOID = Namespace("http://rdfs.org/ns/void#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")


def test_retrieve_description_keeps_sd_name_and_graph_description():
    data = Dataset()
    meta = data.graph(URIRef("urn:g:meta"))
    dataset = URIRef("urn:dataset")
    named = URIRef("urn:named-description")
    graph_desc = URIRef("urn:graph-description")
    data_graph = URIRef("urn:g:data")
    meta.add((dataset, RDF.type, VOID.Dataset))
    meta.add((dataset, SD.namedGraph, named))
    meta.add((named, RDF.type, SD.NamedGraph))
    meta.add((named, SD.name, data_graph))
    meta.add((named, SD.graph, graph_desc))
    meta.add((graph_desc, RDF.type, SD.Graph))
    meta.add((graph_desc, VOID.triples, URIRef("urn:count-placeholder")))
    helper = Mock(endpoint_url="https://example.org/sparql")
    helper.construct.side_effect = lambda query: (
        lambda value: value.decode() if isinstance(value, bytes) else str(value)
    )(data.query(query).serialize(format="turtle"))
    graph = retrieve_description(helper, ["urn:g:meta"])
    assert (named, SD.name, data_graph) in graph
    assert (named, SD.graph, graph_desc) in graph
    assert (graph_desc, RDF.type, SD.Graph) in graph
