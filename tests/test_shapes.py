"""Tests for rdfsolve.shapes - subset and SHACL conversion.

subset_jsonld is used by the UI shapes panel. jsonld_to_shacl
is the full pipeline: JSON-LD -> VoidParser -> LinkML -> ShaclGenerator.
"""
# ruff: noqa: D101, D102

from __future__ import annotations

import json
from pathlib import Path

from rdfsolve.shapes import jsonld_to_shacl, subset_jsonld

DATA = Path(__file__).parent / "test_data"
SCHEMA = DATA / "minimal_schema.jsonld"


def _load():
    with open(SCHEMA) as f:
        return json.load(f)


class TestSubsetJsonLD:

    def test_empty_keep_returns_empty_graph(self):
        result = subset_jsonld(_load(), keep_edges=[])
        assert result["@graph"] == []
        assert "@context" in result
        # @about is preserved even on empty subset
        assert result.get("@about", {}).get(
            "dataset_name",
        ) == "test_aopwiki"

    def test_one_edge_filters_correctly(self):
        doc = _load()
        node = doc["@graph"][0]
        nid = node["@id"]
        prop = next(
            k for k in node
            if not k.startswith("@") and k != "_counts"
        )
        result = subset_jsonld(
            doc,
            keep_edges=[
                {
                    "subject": nid,
                    "predicate": prop,
                    "object": "",
                },
            ],
        )
        assert len(result["@graph"]) == 1
        assert result["@graph"][0]["@id"] == nid
        assert prop in result["@graph"][0]
        # Other nodes excluded
        ids = {n["@id"] for n in result["@graph"]}
        assert len(ids) == 1


class TestJsonLDToShacl:
    """Full pipeline: JSON-LD -> SHACL Turtle."""

    def test_produces_valid_turtle(self):
        doc = _load()
        shacl = jsonld_to_shacl(
            doc, schema_name="test_aop",
        )
        assert isinstance(shacl, str)
        assert len(shacl) > 0
        # Must contain SHACL namespace
        assert "sh:" in shacl or "shacl" in shacl.lower()

    def test_subset_then_shacl(self):
        """The real UI flow: user selects edges -> subset -> SHACL."""
        doc = _load()
        node = doc["@graph"][0]
        nid = node["@id"]
        prop = next(
            k for k in node
            if not k.startswith("@") and k != "_counts"
        )
        sub = subset_jsonld(
            doc,
            keep_edges=[
                {
                    "subject": nid,
                    "predicate": prop,
                    "object": "",
                },
            ],
        )
        shacl = jsonld_to_shacl(
            sub, schema_name="subset_test",
        )
        assert isinstance(shacl, str)
        assert len(shacl) > 0
