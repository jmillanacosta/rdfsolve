"""Tests for SPARQL proxy route."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_proxy_query_missing_fields(client):
    resp = client.post("/api/sparql/query", json={})
    assert resp.status_code == 400


@patch("rdfsolve.sparql_helper.requests.Session")
def test_proxy_query_success(mock_session_cls, client):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    mock_resp.text = '{"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"http://example.org/1"}}]}}'
    mock_resp.json.return_value = {
        "head": {"vars": ["s"]},
        "results": {
            "bindings": [
                {"s": {"type": "uri", "value": "http://example.org/1"}},
            ],
        },
    }
    mock_resp.headers = {"content-type": "application/sparql-results+json"}
    mock_resp.raise_for_status = MagicMock()
    mock_session.get.return_value = mock_resp

    resp = client.post(
        "/api/sparql/query",
        json={
            "query": "SELECT ?s WHERE { ?s a ?t } LIMIT 1",
            "endpoint": "http://example.org/sparql",
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["row_count"] == 1
    assert data["variables"] == ["s"]
