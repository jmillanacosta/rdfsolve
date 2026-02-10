"""Tests for export routes."""

from __future__ import annotations


def test_export_query_jsonld(client):
    resp = client.post(
        "/api/export/query",
        json={
            "query": "SELECT ?s WHERE { ?s a ?t }",
            "query_type": "select",
            "prefixes": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
            "endpoint": "http://example.org/sparql",
            "description": "Test query",
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "sh:SPARQLExecutable" in data["@type"]
    assert data["sh:select"] == "SELECT ?s WHERE { ?s a ?t }"
    assert data["schema:description"] == "Test query"
    assert data["schema:target"]["sd:endpoint"] == "http://example.org/sparql"


def test_export_results_jsonld(client):
    resp = client.post(
        "/api/export/results",
        json={
            "endpoint": "http://example.org/sparql",
            "query": "SELECT ?s WHERE { ?s a ?t }",
            "variables": ["s"],
            "row_count": 1,
            "rows": [{"s": {"value": "http://example.org/1", "type": "uri"}}],
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["@type"] == "schema:Dataset"
    assert data["schema:size"] == 1


def test_export_query_without_endpoint(client):
    resp = client.post(
        "/api/export/query",
        json={
            "query": "ASK {}",
            "query_type": "ask",
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "schema:target" not in data
    assert "sh:SPARQLAskExecutable" in data["@type"]
