import json

import pytest
from rdfsolve.client.api import Client
from rdfsolve.schema_models.core import MinedSchema

from tests.test_client_api import CHEMICAL, DATA, client


def test_open_saved_formats_and_mined_schema(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "rdfsolve.mining.miner.SchemaMiner.mine", lambda *a, **k: pytest.fail("Do not mine")
    )
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
                assert not data.queries and (not data.graph_uris)
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
