"""Tests for query composition route."""

from __future__ import annotations


def test_compose_from_paths(client):
    resp = client.post(
        "/api/compose/from-paths",
        json={
            "paths": [
                {
                    "edges": [
                        {
                            "source": "http://example.org/Person",
                            "target": "http://example.org/Organization",
                            "predicate": "http://example.org/worksFor",
                            "is_forward": True,
                        },
                    ],
                },
            ],
            "prefixes": {"ex": "http://example.org/"},
            "options": {
                "include_types": True,
                "include_labels": True,
                "limit": 50,
            },
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    query = data["query"]

    # Variable names derived from local names
    assert "?person" in query
    assert "?organization" in query

    # Triple pattern present
    assert "ex:worksFor" in query

    # Type assertions
    assert "?person a ex:Person" in query

    # OPTIONAL labels
    assert "OPTIONAL" in query
    assert "rdfs:label" in query

    # LIMIT
    assert "LIMIT 50" in query

    # Variable map traces back to schema URIs
    vmap = data["variable_map"]
    assert vmap["person"] == "http://example.org/Person"
    assert vmap["organization"] == "http://example.org/Organization"
