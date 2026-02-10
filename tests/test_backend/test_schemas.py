"""Tests for schema routes and SchemaService."""

from __future__ import annotations


def test_health(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"


def test_list_schemas_empty(client):
    resp = client.get("/api/schemas/")
    assert resp.status_code == 200
    assert resp.get_json() == []


def test_upload_and_get_schema(client):
    schema = {
        "@context": {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        },
        "@graph": [{"@id": "ex:Thing", "@type": "owl:Class"}],
        "@about": {
            "dataset_name": "test",
            "endpoint": "http://example.org/sparql",
            "pattern_count": 1,
        },
    }

    # Upload
    resp = client.post(
        "/api/schemas/upload",
        json=schema,
    )
    assert resp.status_code == 201
    data = resp.get_json()
    assert "id" in data

    # List — should contain one entry
    resp = client.get("/api/schemas/")
    assert resp.status_code == 200
    items = resp.get_json()
    assert len(items) == 1
    assert items[0]["name"] == "test"

    # Get by ID
    schema_id = data["id"]
    resp = client.get(f"/api/schemas/{schema_id}")
    assert resp.status_code == 200
    fetched = resp.get_json()
    assert fetched["@about"]["dataset_name"] == "test"


def test_get_schema_404(client):
    resp = client.get("/api/schemas/nonexistent")
    assert resp.status_code == 404


def test_delete_schema(client):
    schema = {
        "@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
        "@graph": [],
        "@about": {"dataset_name": "deleteme"},
    }
    resp = client.post("/api/schemas/upload", json=schema)
    assert resp.status_code == 201
    schema_id = resp.get_json()["id"]

    resp = client.delete(f"/api/schemas/{schema_id}")
    assert resp.status_code == 200

    resp = client.get(f"/api/schemas/{schema_id}")
    assert resp.status_code == 404


def test_upload_invalid_schema(client):
    resp = client.post(
        "/api/schemas/upload",
        json={"foo": "bar"},
    )
    assert resp.status_code == 400
