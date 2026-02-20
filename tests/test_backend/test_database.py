"""Tests for the Database layer."""

from __future__ import annotations

from rdfsolve.backend.database import Database


def test_database_schema_crud():
    """Basic CRUD on the schemas table."""
    db = Database(":memory:")

    # Initially empty
    assert db.list_schemas() == []

    # Insert
    db.save_schema(
        schema_id="test_schema",
        name="Test",
        data={"@context": {}, "@graph": []},
        endpoint="http://example.org/sparql",
        pattern_count=42,
    )

    # List
    schemas = db.list_schemas()
    assert len(schemas) == 1
    assert schemas[0]["id"] == "test_schema"
    assert schemas[0]["pattern_count"] == 42

    # Get
    data = db.get_schema("test_schema")
    assert data is not None
    assert data["@graph"] == []

    # Delete
    assert db.delete_schema("test_schema") is True
    assert db.get_schema("test_schema") is None
    assert db.delete_schema("test_schema") is False

    db.close()


def test_database_endpoint_crud():
    """Basic CRUD on the endpoints table."""
    db = Database(":memory:")

    assert db.list_endpoints() == []

    db.add_endpoint(
        name="WikiPathways",
        endpoint="https://sparql.wikipathways.org/sparql/",
    )

    eps = db.list_endpoints()
    assert len(eps) == 1
    assert eps[0]["name"] == "WikiPathways"

    assert db.delete_endpoint("https://sparql.wikipathways.org/sparql/")
    assert db.list_endpoints() == []

    db.close()


def test_schema_endpoints_discovery():
    """Endpoints in schema @about should be discovered."""
    db = Database(":memory:")
    db.save_schema(
        schema_id="wp",
        name="WikiPathways",
        data={
            "@context": {},
            "@graph": [],
            "@about": {
                "dataset_name": "WikiPathways",
                "endpoint": "https://sparql.wikipathways.org/sparql/",
            },
        },
    )

    eps = db.get_schema_endpoints()
    assert len(eps) == 1
    assert eps[0]["endpoint"] == "https://sparql.wikipathways.org/sparql/"

    db.close()
