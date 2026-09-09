"""Test registry boundaries with retained AOPWiki data."""

import json

import pytest

from rdfsolve.registry import Registry
from rdfsolve.hydration import HydrationLimitError
from tests.test_client_api import AOP, CHEMICAL, STRESSOR, client


def test_registry_roundtrip_and_rejected_documents(tmp_path):
    with client() as data:
        registry = data.registry(source_id="aopwikirdf")
        assert not data.queries
        assert registry.describe("records.get")["arguments"]["additionalProperties"] is False
        assert registry.describe(AOP)["fields"]
        stressor_field = next(field for field in registry.describe(AOP)["fields"] if field["name"] == "c54571")
        assert stressor_field["targets"] == [STRESSOR]
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


def test_session_pages_before_reading_fields_and_keeps_evidence(tmp_path):
    with client() as data:
        session = data.session(source_id="aopwikirdf", preview_rows=1)
        chemicals = session.call("records.find", {"text": "Phenobarbital", "kind": CHEMICAL})
        stressors = session.call("records.related", {
            "reference": chemicals["reference"], "kind": STRESSOR, "incoming": True,
        })
        pathways = session.call("records.related", {
            "reference": stressors["reference"], "kind": AOP, "incoming": True,
        })
        assert pathways["retained_records"] == 2 and pathways["next_offset"] == 1
        before = len(data.queries)
        page = session.call("records.select", {
            "reference": pathways["reference"], "fields": ["C54571"], "limit": 1,
        })
        retained = session.results[pathways["reference"]].records
        assert "c54571" in retained[0].rdf_loaded_fields
        assert "c54571" not in retained[1].rdf_loaded_fields
        assert all(term["kind"] == "uri" for term in page["rows"][0]["fields"]["c54571"])
        assert len(data.queries) > before
        before = len(data.queries)
        assert session.call("records.select", {
            "reference": pathways["reference"], "fields": ["C54571"], "limit": 1,
        })["rows"] == page["rows"]
        assert len(data.queries) == before
        before = len(data.queries)
        session.preview_rows = 2
        widened = session.call("records.select", {
            "reference": pathways["reference"], "fields": ["C54571"], "limit": 2,
        })
        assert len(widened["rows"]) == 2 and len(data.queries) == before + 1
        assert all(record.rdf_loaded_fields for record in session.result(pathways["reference"]))
        fresh = session.call("records.get", {"iri": "https://identifiers.org/aop/107", "kind": AOP})
        before = len(data.queries)
        session.call("records.select", {
            "reference": fresh["reference"], "fields": ["C54571", "has_key_event"],
        })
        assert len(data.queries) == before + 1
        output = tmp_path / "session.json"
        data.save_session(output)
        saved = json.loads(output.read_text())
        execution = next(item for item in saved["operations"] if item["id"] == widened["execution"])
        assert execution["query_ids"] and execution["status"] == "complete"
        assert execution["registry_revision"] in saved["registries"]
        assert all(q in {query["id"] for query in saved["queries"]} for q in execution["query_ids"])


def test_session_rejects_invalid_calls_before_queries_and_records_failures():
    with client() as data:
        session = data.session(source_id="aopwikirdf", max_results=1)
        for operation, arguments in (
            ("delete", {}),
            ("records.get", {"iri": "urn:x", "kind": AOP, "timeout": 100}),
            ("records.select", {"reference": "missing", "limit": "20"}),
            ("records.select", {"reference": "missing"}),
        ):
            with pytest.raises(ValueError):
                session.call(operation, arguments)
        assert not data.queries
        assert all(item["status"] == "failed" and not item["query_ids"]
                   for item in data.session_metadata()["operations"])
        found = session.call("records.find", {"text": "Phenobarbital", "kind": CHEMICAL})
        before = len(data.queries)
        with pytest.raises(HydrationLimitError):
            session.call("records.find", {"text": "thyroid"})
        with pytest.raises(ValueError):
            session.call("records.select", {"reference": found["reference"], "fields": ["missing"]})
        assert len(data.queries) == before
        session.release(found["reference"])
        with pytest.raises(ValueError):
            session.call("records.select", {"reference": found["reference"]})
        with pytest.raises(LookupError):
            session.call("records.get", {"iri": "urn:missing", "kind": AOP})
        last = data.session_metadata()["operations"][-1]
        assert last["status"] == "failed" and last["query_ids"]


def test_session_does_not_certify_requested_types_or_keep_oversized_results():
    with client() as data:
        session = data.session(source_id="aopwikirdf", max_records=1)
        with pytest.raises(HydrationLimitError):
            session.call("records.find", {"text": "carcinomas", "kind": AOP})
        assert not session.results
        result = session.call("records.get", {
            "iri": "https://identifiers.org/aop/107", "kind": CHEMICAL,
        })
        row = result["rows"][0]
        assert row["type"] == CHEMICAL and row["observed_types"] == [AOP]
        assert row["type_evidence"] == "not established"
        data.graph_uris = ["urn:different"]
        before = len(data.queries)
        with pytest.raises(ValueError, match="scope"):
            session.call("records.select", {"reference": result["reference"]})
        assert len(data.queries) == before
