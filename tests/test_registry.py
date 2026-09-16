"""Exercise discovery and route execution against retained AOPWiki statements."""

import json

import pytest
from rdflib import RDF, Dataset, URIRef

from rdfsolve.client.api import Client
from rdfsolve.client.hydration import HydrationLimitError
from rdfsolve.client.registry import Registry
from tests.test_client_api import AOP, CHEMICAL, DATA, client


def test_registry_roundtrip_and_rejected_documents(tmp_path):
    with client() as data:
        registry = data.registry(source_id="aopwikirdf")
        assert not data.queries
        path = tmp_path / "registry.json"
        registry.write(path)
        assert Registry.read(path) == registry
        raw = json.loads(path.read_text())
        for bad in (
            {**raw, "format_version": 2},
            {**raw, "source_id": "changed"},
            {**raw, "load_code": "untrusted.py"},
        ):
            path.write_text(json.dumps(bad))
            with pytest.raises(ValueError):
                Registry.read(path)
        data.graph_uris = ["http://aopwiki.org/"]
        assert data.registry(source_id="aopwikirdf").revision != registry.revision
