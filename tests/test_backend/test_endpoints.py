"""Tests for endpoint management routes."""

from __future__ import annotations


def test_list_endpoints_empty(client):
    resp = client.get("/api/endpoints/")
    assert resp.status_code == 200
    assert resp.get_json() == []


def test_add_and_list_endpoint(client):
    resp = client.post(
        "/api/endpoints/",
        json={
            "name": "WikiPathways",
            "endpoint": "https://sparql.wikipathways.org/sparql/",
        },
    )
    assert resp.status_code == 201

    resp = client.get("/api/endpoints/")
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["name"] == "WikiPathways"
    assert data[0]["endpoint"] == "https://sparql.wikipathways.org/sparql/"


def test_add_endpoint_missing_url(client):
    resp = client.post("/api/endpoints/", json={"name": "test"})
    assert resp.status_code == 400


def test_sources_jsonld(client):
    client.post(
        "/api/endpoints/",
        json={
            "name": "Test",
            "endpoint": "http://example.org/sparql",
        },
    )
    resp = client.get("/api/endpoints/sources")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "@context" in data
    assert len(data["@graph"]) == 1
    assert data["@graph"][0]["dcterms:title"] == "Test"


def test_endpoints_discovered_from_schema(client):
    """Endpoints in schema @about should be auto-discovered."""
    schema = {
        "@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
        "@graph": [],
        "@about": {
            "dataset_name": "wiki",
            "endpoint": "https://sparql.wikipathways.org/sparql/",
        },
    }
    client.post("/api/schemas/upload", json=schema)

    resp = client.get("/api/endpoints/")
    data = resp.get_json()
    urls = [ep["endpoint"] for ep in data]
    assert "https://sparql.wikipathways.org/sparql/" in urls
