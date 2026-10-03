import json
from unittest.mock import patch

import pytest
from rdflib import Dataset
from rdflib.compare import isomorphic
from rdfsolve.sparql_helper import SparqlHelper
from rdfsolve.void_source import discover_void_graphs, discover_void_source

from tests.test_navigation import aop_schema


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
