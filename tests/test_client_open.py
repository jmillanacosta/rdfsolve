"""Open saved AOPWiki schemas without mining or hidden endpoint requests."""

import json

import pytest

from rdfsolve.client_api import Client
from rdfsolve.schema_models.core import MinedSchema
from tests.test_client_api import CHEMICAL, DATA, client


def test_open_saved_formats_and_mined_schema(tmp_path, monkeypatch):
    monkeypatch.setattr("rdfsolve.miner.SchemaMiner.mine", lambda *a, **k: pytest.fail("Do not mine"))
    with client() as original:
        schema = original._schema
        with schema.client(original.source) as data:
            assert isinstance(data, Client)
            assert not data.queries
        exports = [
            ("schema.json", None, json.dumps(schema.to_dict())),
            ("shapes.ttl", "shacl", schema.to_shacl()),
            ("void.ttl", "void", schema.to_void_graph().serialize(format="turtle")),
        ]
        for name, format, content in exports:
            path = tmp_path / name
            path.write_text(content)
            with Client.open(path, format=format, data_file=DATA) as data:
                assert not data.queries and not data.graph_uris
                records = data.find("Phenobarbital", kind=CHEMICAL)
                assert records[0].uri == "https://identifiers.org/cas/50-06-6"
        with Client.open(schema, original.source) as data:
            assert not data.queries
        with pytest.raises(ValueError, match="Turtle"):
            Client.open(tmp_path / "shapes.ttl")
        with pytest.raises(ValueError, match="not both"):
            Client.open(schema, original.source, data_file=DATA)
        with pytest.raises(ValueError, match="no classes"):
            Client.open(MinedSchema(about={}), original.source)


def test_open_keeps_endpoint_scope_without_requests(tmp_path, monkeypatch):
    monkeypatch.setattr("rdfsolve.sparql_helper.SparqlHelper.select", lambda *a, **k: pytest.fail("Do not query"))
    with client() as original:
        schema = original._schema.model_copy(deep=True)
        schema.about.endpoint = "https://example.org/sparql"
        schema.about.graph_uris = ["urn:source"]
        path = tmp_path / "schema.json"
        path.write_text(json.dumps(schema.to_dict()))
        with Client.open(path) as data:
            assert data.source.endpoint_url == schema.about.endpoint
            assert data.graph_uris == ["urn:source"]
        with Client.open(path, "https://example.org/other", graph_uris=[]) as data:
            assert data.source.endpoint_url.endswith("/other") and data.graph_uris == []
