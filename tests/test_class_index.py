"""Tests for rdfsolve.class_index.

Focus on behaviour that would break if the implementation changed in a
meaningful way: IRI lookup semantics, cache integrity, enrichment
decisions, and batch-query wiring.
"""
# ruff: noqa: D101, D102

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from rdfsolve.class_index import (
    ClassIndex,
    EntityClassInfo,
    build_class_index,
    enrich_jsonld_with_classes,
    expand_iri_alternatives,
    load_class_index,
    save_class_index,
)

_GRAPH_A = "https://example.org/graphs/a"
_GRAPH_B = "https://example.org/graphs/b"
_GENE_IRI = "https://identifiers.org/ncbigene/672"
_PROTEIN_IRI = "https://identifiers.org/uniprot/P38398"
_CLASS_GENE = "http://purl.obolibrary.org/obo/SO_0000704"
_CLASS_PROTEIN = "http://purl.obolibrary.org/obo/PR_000000001"


def _make_index() -> ClassIndex:
    idx = ClassIndex(endpoint_url="https://example.org/sparql")
    idx.entities[_GENE_IRI] = EntityClassInfo(
        entity_iri=_GENE_IRI,
        alternative_iris=[_GENE_IRI],
        graph_classes={_GRAPH_A: [_CLASS_GENE]},
    )
    idx.entities[_PROTEIN_IRI] = EntityClassInfo(
        entity_iri=_PROTEIN_IRI,
        alternative_iris=[_PROTEIN_IRI],
        graph_classes={_GRAPH_B: [_CLASS_PROTEIN]},
    )
    return idx


# ---------------------------------------------------------------------------
# ClassIndex lookup semantics
# ---------------------------------------------------------------------------


class TestClassIndexLookup:
    def test_known_entity_returns_its_classes(self):
        idx = _make_index()
        gc = idx.classes_for_entity(_GENE_IRI)
        assert _CLASS_GENE in gc.get(_GRAPH_A, [])

    def test_unknown_entity_returns_empty_dict_not_error(self):
        """Consumers rely on a falsy return, not an exception."""
        idx = _make_index()
        assert idx.classes_for_entity("https://never-seen.org/x") == {}

    def test_entity_found_reflects_index_membership(self):
        idx = _make_index()
        assert idx.entity_found(_GENE_IRI) is True
        assert idx.entity_found("https://never-seen.org/x") is False

    def test_entity_with_multiple_graphs_all_returned(self):
        idx = ClassIndex(endpoint_url="https://example.org/sparql")
        idx.entities[_GENE_IRI] = EntityClassInfo(
            entity_iri=_GENE_IRI,
            graph_classes={
                _GRAPH_A: [_CLASS_GENE],
                _GRAPH_B: ["http://example.org/AltClass"],
            },
        )
        gc = idx.classes_for_entity(_GENE_IRI)
        assert _GRAPH_A in gc and _GRAPH_B in gc

    def test_all_classes_flattens_across_graphs(self):
        info = EntityClassInfo(
            entity_iri=_GENE_IRI,
            graph_classes={
                _GRAPH_A: [_CLASS_GENE],
                _GRAPH_B: ["http://example.org/AltClass"],
            },
        )
        flat = info.all_classes()
        assert _CLASS_GENE in flat
        assert "http://example.org/AltClass" in flat


# ---------------------------------------------------------------------------
# Persistence — only test that reload is equivalent to original
# ---------------------------------------------------------------------------


class TestClassIndexPersistence:
    def test_reloaded_index_has_same_entities_and_classes(self):
        idx = _make_index()
        with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        ) as f:
            path = f.name
        try:
            save_class_index(idx, path)
            idx2 = load_class_index(path)
            assert set(idx2.entities) == set(idx.entities)
            assert idx2.endpoint_url == idx.endpoint_url
            gc = idx2.entities[_GENE_IRI].graph_classes
            assert _CLASS_GENE in gc.get(_GRAPH_A, [])
        finally:
            Path(path).unlink(missing_ok=True)

    def test_load_missing_file_raises(self):
        with pytest.raises(Exception):
            load_class_index("/tmp/does_not_exist_rdfsolve_test.json")


# ---------------------------------------------------------------------------
# IRI expansion
# ---------------------------------------------------------------------------


class TestExpandIriAlternatives:
    def test_always_includes_original(self):
        iri = "https://totally.unknown/custom/123"
        assert iri in expand_iri_alternatives(iri)

    def test_no_duplicates_in_result(self):
        iri = "https://identifiers.org/chebi/CHEBI:15422"
        alts = expand_iri_alternatives(iri)
        assert len(alts) == len(set(alts))


# ---------------------------------------------------------------------------
# build_class_index — SPARQL wiring and stat accounting
# ---------------------------------------------------------------------------


class TestBuildClassIndex:
    def test_empty_input_never_calls_sparql(self):
        with patch("rdfsolve.sparql_helper.SparqlHelper") as MockHelper:
            idx, stats = build_class_index(
                [], endpoint_url="https://example.org/sparql"
            )
        helper = MockHelper.return_value
        helper.find_classes_for_iris_by_graph.assert_not_called()
        assert len(idx.entities) == 0
        assert stats["iris_total"] == 0
        assert stats["sparql_queries_sent"] == 0

    def test_found_iri_is_indexed(self):
        mock_result = {_GENE_IRI: {_GRAPH_A: [_CLASS_GENE]}}
        with patch("rdfsolve.sparql_helper.SparqlHelper") as MockHelper:
            helper = MockHelper.return_value
            helper.find_classes_for_iris_by_graph.return_value = mock_result
            idx, stats = build_class_index(
                [_GENE_IRI],
                endpoint_url="https://example.org/sparql",
            )
        assert idx.entity_found(_GENE_IRI)
        assert stats["iris_total"] == 1

    def test_missing_iri_increments_not_found(self):
        """An IRI that SPARQL returns nothing for must still be in entities
        but entity_found() must return False."""
        with patch("rdfsolve.sparql_helper.SparqlHelper") as MockHelper:
            helper = MockHelper.return_value
            helper.find_classes_for_iris_by_graph.return_value = {}
            idx, stats = build_class_index(
                [_GENE_IRI],
                endpoint_url="https://example.org/sparql",
            )
        assert not idx.entity_found(_GENE_IRI)
        assert stats["iris_total"] == 1

    def test_batch_size_controls_call_count(self):
        """With 5 IRIs and batch_size=2 we expect ceil(5/2)=3 helper calls."""
        iris = [f"https://example.org/e/{i}" for i in range(5)]
        with patch("rdfsolve.sparql_helper.SparqlHelper") as MockHelper:
            helper = MockHelper.return_value
            helper.find_classes_for_iris_by_graph.return_value = {}
            build_class_index(
                iris,
                endpoint_url="https://example.org/sparql",
                batch_size=2,
            )
        call_count = (
            helper.find_classes_for_iris_by_graph.call_count
        )
        assert call_count == 3


# ---------------------------------------------------------------------------
# enrich_jsonld_with_classes
# ---------------------------------------------------------------------------


class TestEnrichJsonldWithClasses:
    def _make_doc(self) -> dict:
        # Each @graph node has an @id that the enricher walks directly
        return {
            "@context": {},
            "@graph": [
                {
                    "@id": _GENE_IRI,
                },
                {
                    "@id": _PROTEIN_IRI,
                },
                {
                    "@id": "https://unknown.org/x",
                },
            ],
            "@about": {},
        }

    def test_known_entities_get_class_annotations(self):
        doc, _ = enrich_jsonld_with_classes(self._make_doc(), _make_index())
        gene_node = next(n for n in doc["@graph"] if n.get("@id") == _GENE_IRI)
        assert "@type" in gene_node
        assert _CLASS_GENE in gene_node["@type"]

    def test_unknown_entities_receive_no_type(self):
        doc, _ = enrich_jsonld_with_classes(self._make_doc(), _make_index())
        unknown_node = next(
            n for n in doc["@graph"] if n.get("@id") == "https://unknown.org/x"
        )
        assert "@type" not in unknown_node

    def test_stats_account_for_all_entities(self):
        _, stats = enrich_jsonld_with_classes(self._make_doc(), _make_index())
        assert stats["entities_total"] == 3
        assert stats["entities_enriched"] >= 1
        assert stats["entities_enriched"] + stats["entities_not_found"] == (
            stats["entities_total"]
        )

    def test_empty_graph_produces_zero_stats(self):
        doc = {"@context": {}, "@graph": [], "@about": {}}
        _, stats = enrich_jsonld_with_classes(doc, _make_index())
        assert stats["entities_total"] == 0
        assert stats["entities_enriched"] == 0

    def test_missing_at_graph_key_is_handled_gracefully(self):
        """A doc without @graph must not crash; yields zero stats."""
        doc, stats = enrich_jsonld_with_classes({"@about": {}}, _make_index())
        assert stats["entities_total"] == 0
