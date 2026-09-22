import io
import json
from pathlib import Path
from unittest.mock import patch

import requests
from rdflib import Graph
from rdflib.compare import isomorphic

from rdfsolve import MinedSchema, VoidSchema, discover_void_source

DATA = Path(__file__).parent / "test_data" / "aopwikirdf_generated_void.ttl"
STORE = "https://example.org/store"
GRAPH = "http://aopwiki.org/"


def response(content_type="text/turtle", status=200):
    result = requests.Response()
    result.status_code = status
    result.headers["Content-Type"] = content_type
    result.raw = io.BytesIO(DATA.read_bytes())
    return result


def count_query(query, **kwargs):
    assert kwargs.get("purpose") == "graph-store/verify-count"
    graph = Graph().parse(DATA, format="turtle")
    return json.loads(
        graph.query("SELECT (COUNT(*) AS ?count) WHERE {?s ?p ?o}").serialize(format="json")
    )


def test_void_returns_object_and_keeps_metadata(tmp_path):
    options = dict(
        graph_uris=[GRAPH],
        get_graphs_from_store=True,
        graph_store_url=STORE,
        graph_store_dir=tmp_path,
    )
    with (
        patch("requests.Session.get", side_effect=lambda *a, **k: response()),
        patch("rdfsolve.sparql_helper.SparqlHelper.select", side_effect=count_query),
    ):
        document = discover_void_source(STORE, "aopwiki", **options)
        schema = MinedSchema.from_void_source(STORE, "aopwiki", **options)
    assert isinstance(document, VoidSchema)
    assert isomorphic(document.graph, Graph().parse(DATA, format="turtle"))
    assert not document.datasets
    assert document.class_partitions
    assert schema.about.class_entity_counts
    assert not schema.patterns
    assert not document.files
