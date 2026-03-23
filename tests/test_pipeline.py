"""Tests for pipeline integration - inference, merging, query models.

These cover the functions that glue the pipeline together:
  1. inference._load_edges_from_jsonld - loads mapping fixtures
  2. api._merge_instance_mapping_jsonld - merges probe results
  3. query.QueryResult / ResultCell - SPARQL result models
  4. class derivation + enrichment integration - with mock ClassIndex
"""
# ruff: noqa: D102

from __future__ import annotations

import copy
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

DATA = Path(__file__).parent / "test_data"
SSSOM_MAPPING = DATA / "sssom_mapping.jsonld"
INSTANCE_MAPPING = DATA / "instance_mapping.jsonld"
SYNTHETIC_INSTANCE = DATA / "instance_mappings" / "synthetic_instance.jsonld"


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


# ── class derivation + enrichment integration ─────────────────────


_GRAPH_A = "https://example.org/graphs/a"
_GENE_IRI = "https://identifiers.org/ncbigene/672"
_CLASS_GENE = "http://purl.obolibrary.org/obo/SO_0000704"
_PROTEIN_IRI = "https://identifiers.org/uniprot/P38398"
_CLASS_PROTEIN = "http://purl.obolibrary.org/obo/PR_000000001"


def _make_class_index():
    from rdfsolve.class_index import ClassIndex, EntityClassInfo

    idx = ClassIndex(endpoint_url="https://example.org/sparql")
    idx.entities[_GENE_IRI] = EntityClassInfo(
        entity_iri=_GENE_IRI,
        graph_classes={_GRAPH_A: [_CLASS_GENE]},
    )
    idx.entities[_PROTEIN_IRI] = EntityClassInfo(
        entity_iri=_PROTEIN_IRI,
        graph_classes={_GRAPH_A: [_CLASS_PROTEIN]},
    )
    return idx


class TestDeriveClassMappingsPipeline:
    """Full derivation pipeline: instance edges + ClassIndex -> output."""

    def _make_instance_edges(self):
        from rdfsolve.mapping_models.core import MappingEdge

        return [
            MappingEdge(
                source_class=_GENE_IRI,
                target_class=_PROTEIN_IRI,
                predicate="skos:exactMatch",
                source_dataset="ncbigene",
                target_dataset="uniprot",
            )
        ]

    def test_derive_produces_class_edge(self):
        """A single instance edge with known class index yields >= 1 pair."""
        from rdfsolve.class_derivation import derive_class_mappings

        pairs, stats = derive_class_mappings(
            self._make_instance_edges(),
            _make_class_index(),
        )
        assert len(pairs) >= 1
        assert stats["input_edges"] == 1
        assert stats["output_edges"] >= 1

    def test_derive_to_class_derived_mapping_jsonld(self):
        """Derived pairs must serialise to valid ClassDerivedMapping JSON-LD."""
        from rdfsolve.class_derivation import derive_class_mappings
        from rdfsolve.mapping_models.class_derived import ClassDerivedMapping
        from rdfsolve.schema_models.core import AboutMetadata

        pairs, stats = derive_class_mappings(
            self._make_instance_edges(),
            _make_class_index(),
        )
        about = AboutMetadata.build(
            dataset_name="test_derived",
            pattern_count=len(pairs),
            strategy="class_derived",
        )
        mapping = ClassDerivedMapping(
            edges=[p.to_mapping_edge() for p in pairs],
            about=about,
            source_mapping_type="sssom_import",
            derivation_stats=stats,
        )
        doc = mapping.to_jsonld()
        assert doc["@about"]["strategy"] == "class_derived"
        assert len(doc["@graph"]) >= 1

    def test_derive_no_class_info_yields_empty(self):
        """Edges whose entities are absent from the index are skipped."""
        from rdfsolve.class_derivation import derive_class_mappings
        from rdfsolve.class_index import ClassIndex
        from rdfsolve.mapping_models.core import MappingEdge

        empty_idx = ClassIndex(endpoint_url="https://example.org/sparql")
        edges = [
            MappingEdge(
                source_class="https://unknown.org/A",
                target_class="https://unknown.org/B",
                predicate="skos:exactMatch",
                source_dataset="x",
                target_dataset="y",
            )
        ]
        pairs, stats = derive_class_mappings(edges, empty_idx)
        assert pairs == []
        assert stats["output_edges"] == 0


class TestEnrichInstanceJsonldWritesFile:
    """enrich_instance_jsonld must write an enriched file to disk."""

    def test_enriched_file_written_and_has_type(self):
        from rdfsolve.api import enrich_instance_jsonld

        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "mapping.jsonld"
            src.write_text(
                json.dumps({
                    "@context": {},
                    "@graph": [{"@id": _GENE_IRI}],
                    "@about": {},
                })
            )
            enrich_instance_jsonld(
                str(src),
                _make_class_index(),
            )
            enriched = src.with_suffix(".enriched.jsonld")
            assert enriched.exists()
            doc = json.loads(enriched.read_text())
            gene_node = next(
                n for n in doc["@graph"]
                if n.get("@id") == _GENE_IRI
            )
            assert "@type" in gene_node
            assert _CLASS_GENE in gene_node["@type"]

    def test_enrichment_stats_keys_present(self):
        from rdfsolve.api import enrich_instance_jsonld

        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "mapping.jsonld"
            src.write_text(
                json.dumps({
                    "@context": {},
                    "@graph": [{"@id": _GENE_IRI}],
                    "@about": {},
                })
            )
            stats = enrich_instance_jsonld(
                str(src),
                _make_class_index(),
            )
        assert "entities_total" in stats
        assert "entities_enriched" in stats
        assert stats["entities_enriched"] >= 1
