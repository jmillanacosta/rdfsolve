"""Tests for pipeline integration - inference, merging, query models.

These cover the functions that glue the pipeline together:
  1. inference._load_edges_from_jsonld - loads mapping fixtures
  2. api._merge_instance_mapping_jsonld - merges probe results
  3. query.QueryResult / ResultCell - SPARQL result models
"""
# ruff: noqa: D102

from __future__ import annotations

import copy
from pathlib import Path

DATA = Path(__file__).parent / "test_data"
SSSOM_MAPPING = DATA / "sssom_mapping.jsonld"
INSTANCE_MAPPING = DATA / "instance_mapping.jsonld"


# ── inference._load_edges_from_jsonld ─────────────────────────────


class TestLoadEdgesFromJsonLD:
    """_load_edges_from_jsonld is used by infer_mappings."""

    def test_loads_sssom_edges(self):
        from rdfsolve.inference import _load_edges_from_jsonld

        edges = _load_edges_from_jsonld(SSSOM_MAPPING)
        assert len(edges) == 2
        # Each edge has expanded URIs
        for e in edges:
            assert e.source_class.startswith("http")
            assert e.target_class.startswith("http")
            assert e.predicate.startswith("http")

    def test_empty_mapping_returns_empty(self):
        from rdfsolve.inference import _load_edges_from_jsonld

        edges = _load_edges_from_jsonld(INSTANCE_MAPPING)
        assert edges == []


# ── api._merge_instance_mapping_jsonld ────────────────────────────


class TestMergeInstanceMappingJsonLD:
    """_merge_instance_mapping_jsonld merges probe results on disk."""

    def _make_mapping(self, nodes, uri_formats=None):
        return {
            "@context": {
                "skos": "http://www.w3.org/2004/02/skos/core#",
                "void": "http://rdfs.org/ns/void#",
            },
            "@graph": nodes,
            "@about": {
                "dataset_name": "test",
                "uri_formats_queried": uri_formats or [],
                "pattern_count": len(nodes),
                "generated_at": "2025-01-01T00:00:00+00:00",
            },
        }

    def test_merge_new_node(self):
        from rdfsolve.api import _merge_instance_mapping_jsonld

        existing = self._make_mapping(
            [{"@id": "http://a.org/A", "skos:narrowMatch": {
                "@id": "http://b.org/B",
            }}],
            uri_formats=["http://a.org/"],
        )
        new = self._make_mapping(
            [{"@id": "http://c.org/C", "skos:narrowMatch": {
                "@id": "http://d.org/D",
            }}],
            uri_formats=["http://c.org/"],
        )
        merged = _merge_instance_mapping_jsonld(
            copy.deepcopy(existing), new,
        )
        ids = {n["@id"] for n in merged["@graph"]}
        assert "http://a.org/A" in ids
        assert "http://c.org/C" in ids

    def test_merge_same_node_adds_targets(self):
        from rdfsolve.api import _merge_instance_mapping_jsonld

        existing = self._make_mapping(
            [{"@id": "http://a.org/A", "skos:narrowMatch": {
                "@id": "http://b.org/B1",
            }}],
        )
        new = self._make_mapping(
            [{"@id": "http://a.org/A", "skos:narrowMatch": {
                "@id": "http://b.org/B2",
            }}],
        )
        merged = _merge_instance_mapping_jsonld(
            copy.deepcopy(existing), new,
        )
        assert len(merged["@graph"]) == 1
        targets = merged["@graph"][0]["skos:narrowMatch"]
        # After merge, both B1 and B2 should be present
        if isinstance(targets, list):
            ids = {t["@id"] for t in targets}
        else:
            ids = {targets["@id"]}
        assert "http://b.org/B1" in ids
        assert "http://b.org/B2" in ids

    def test_uri_formats_unioned(self):
        from rdfsolve.api import _merge_instance_mapping_jsonld

        existing = self._make_mapping(
            [], uri_formats=["http://a.org/"],
        )
        new = self._make_mapping(
            [], uri_formats=["http://b.org/"],
        )
        merged = _merge_instance_mapping_jsonld(
            copy.deepcopy(existing), new,
        )
        fmts = merged["@about"]["uri_formats_queried"]
        assert "http://a.org/" in fmts
        assert "http://b.org/" in fmts


# ── query.QueryResult / ResultCell ────────────────────────────────


class TestQueryResultModels:
    """QueryResult is the structured return from execute_sparql."""

    def test_result_cell_construction(self):
        from rdfsolve.query import ResultCell

        cell = ResultCell(
            value="http://example.org/x",
            type="uri",
        )
        assert cell.value == "http://example.org/x"
        assert cell.type == "uri"
        assert cell.lang is None

    def test_query_result_serialization(self):
        from rdfsolve.query import QueryResult, ResultCell

        qr = QueryResult(
            query="SELECT ?s WHERE { ?s ?p ?o }",
            endpoint="http://example.org/sparql",
            variables=["s"],
            rows=[
                {"s": ResultCell(
                    value="http://example.org/1",
                    type="uri",
                )},
            ],
            row_count=1,
            duration_ms=42,
        )
        d = qr.model_dump()
        assert d["row_count"] == 1
        assert d["rows"][0]["s"]["type"] == "uri"
        assert d["error"] is None
