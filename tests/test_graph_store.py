"""Check Graph Store boundaries with the saved AOPWiki RDF description."""

import io
import json
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
from rdflib import Graph
from rdflib.compare import isomorphic

from rdfsolve import MinedSchema, SchemaMiner, VoidSchema, discover_void_source
from rdfsolve.graph_store import download_graphs

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
    return json.loads(graph.query("SELECT (COUNT(*) AS ?count) WHERE {?s ?p ?o}").serialize(format="json"))


def test_void_returns_object_and_keeps_metadata(tmp_path):
    options = dict(graph_uris=[GRAPH], get_graphs_from_store=True,
                   graph_store_url=STORE, graph_store_dir=tmp_path)
    with patch("requests.Session.get", side_effect=lambda *a, **k: response()), patch(
        "rdfsolve.sparql_helper.SparqlHelper.select", side_effect=count_query
    ):
        document = discover_void_source(STORE, "aopwiki", **options)
        schema = MinedSchema.from_void_source(STORE, "aopwiki", **options)
    assert isinstance(document, VoidSchema)
    assert isomorphic(document.graph, Graph().parse(DATA, format="turtle"))
    assert not document.datasets  # The source has standalone class partitions.
    assert document.class_partitions
    assert schema.about.class_entity_counts
    assert not schema.patterns  # This saved description has class counts only.
    assert not document.files


@pytest.mark.parametrize("kind", ["html", "auth", "oversize"])
def test_invalid_download_is_not_complete(tmp_path, kind):
    reply = response("text/html" if kind == "html" else "text/turtle",
                     403 if kind == "auth" else 200)
    with patch("requests.Session.get", return_value=reply):
        with pytest.raises((ValueError, requests.HTTPError)):
            download_graphs(STORE, [GRAPH], tmp_path,
                            max_bytes=1 if kind == "oversize" else 1024 * 1024)
    assert not list(tmp_path.rglob("download.json"))


def test_local_mining_does_not_query_remote(tmp_path):
    # Mine actual triples in the saved RDF, not its published partition table.
    with patch("requests.Session.get", side_effect=lambda *a, **k: response()), patch(
        "rdfsolve.sparql_helper.SparqlHelper.select",
        side_effect=count_query,
    ):
        miner = SchemaMiner(
            STORE, graph_uris=[GRAPH], get_graphs_from_store=True,
            graph_store_url=STORE, graph_store_dir=tmp_path,
            delay=0, strategy="one-shot",
        )
        schema = miner.mine("aopwiki-description")
    assert not schema.patterns  # The saved RDF has no explicitly typed instances.
    assert miner.last_report.config["graph_store"]["engine"] == "rdflib"
