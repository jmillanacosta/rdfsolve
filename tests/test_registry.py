"""Test registry boundaries with retained AOPWiki data."""

import json

import pytest

from rdfsolve.registry import Registry
from tests.test_client_api import AOP, client


def test_registry_roundtrip_and_rejected_documents(tmp_path):
    with client() as data:
        registry = data.registry(source_id="aopwikirdf")
        assert not data.queries
        assert registry.describe("records.get")["arguments"]["additionalProperties"] is False
        assert registry.describe(AOP)["fields"]
        output = tmp_path / "registry.json"
        registry.write(output)
        assert Registry.read(output) == registry
        raw = json.loads(output.read_text())
        for bad in (
            {**raw, "format_version": 99},
            {**raw, "source_id": "changed"},
            {**raw, "operations": raw["operations"] * 2},
            {**raw, "load_code": "untrusted.py"},
        ):
            output.write_text(json.dumps(bad))
            with pytest.raises(ValueError):
                Registry.read(output)


def test_registry_revision_tracks_scope_not_labels_as_identity():
    with client() as data:
        first = data.registry(source_id="aopwikirdf")
        second = data.registry(source_id="aopwikirdf")
        assert first.revision == second.revision
        data.graph_uris = ["http://aopwiki.org/"]
        scoped = data.registry(source_id="aopwikirdf")
        assert first.revision != scoped.revision
        assert [t.id for t in first.types] == [t.id for t in scoped.types]
        assert first.find("identifier", limit=1)
        assert first.find("not-a-capability") == []
        with pytest.raises(ValueError):
            first.describe("not-an-operation")
