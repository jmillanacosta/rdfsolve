"""Tests for rdfsolve.class_derivation.

Focus on invariants that would break if the algorithm changed in a
meaningful way: aggregation correctness, filter semantics, confidence
monotonicity, and graph provenance tracking.
"""
# ruff: noqa: D101, D102

from __future__ import annotations

import math

import pytest

from rdfsolve.class_derivation import (
    ClassPairEvidence,
    compute_confidence,
    derive_class_mappings,
)
from rdfsolve.class_index import ClassIndex, EntityClassInfo
from rdfsolve.mapping_models.core import MappingEdge

_GRAPH_A = "https://example.org/graphs/a"
_GRAPH_B = "https://example.org/graphs/b"
_CLASS_PERSON = "https://schema.org/Person"
_CLASS_DRUG = "https://schema.org/Drug"
_CLASS_GENE = "http://purl.obolibrary.org/obo/SO_0000704"
_CLASS_PROTEIN = "http://purl.obolibrary.org/obo/PR_000000001"
_PERSON_IRI = "https://example.org/persons/alice"
_DRUG_IRI = "https://example.org/drugs/aspirin"
_GENE_IRI = "https://example.org/genes/brca1"
_PROTEIN_IRI = "https://example.org/proteins/p12345"


def _make_class_index() -> ClassIndex:
    idx = ClassIndex(endpoint_url="https://example.org/sparql")
    idx.entities[_PERSON_IRI] = EntityClassInfo(
        entity_iri=_PERSON_IRI,
        graph_classes={_GRAPH_A: [_CLASS_PERSON]},
    )
    idx.entities[_DRUG_IRI] = EntityClassInfo(
        entity_iri=_DRUG_IRI,
        graph_classes={_GRAPH_B: [_CLASS_DRUG]},
    )
    idx.entities[_GENE_IRI] = EntityClassInfo(
        entity_iri=_GENE_IRI,
        graph_classes={_GRAPH_A: [_CLASS_GENE]},
    )
    idx.entities[_PROTEIN_IRI] = EntityClassInfo(
        entity_iri=_PROTEIN_IRI,
        graph_classes={_GRAPH_B: [_CLASS_PROTEIN]},
    )
    return idx


def _edge(src: str, tgt: str, pred: str = "skos:exactMatch") -> MappingEdge:
    return MappingEdge(
        source_class=src,
        target_class=tgt,
        predicate=pred,
        source_dataset="ds_a",
        target_dataset="ds_b",
    )


# ---------------------------------------------------------------------------
# compute_confidence
# ---------------------------------------------------------------------------


class TestComputeConfidence:
    def test_formula_at_one_instance(self):
        """Exact formula check: min(1, log2(2)/10)."""
        ev = ClassPairEvidence(
            source_class="A", target_class="B",
            predicate="p", instance_count=1,
        )
        assert compute_confidence(ev) == pytest.approx(math.log2(2) / 10)

    def test_zero_instances_gives_zero(self):
        ev = ClassPairEvidence(
            source_class="A", target_class="B",
            predicate="p", instance_count=0,
        )
        assert compute_confidence(ev) == 0.0

    def test_capped_at_one(self):
        """Large counts must never exceed 1.0."""
        ev = ClassPairEvidence(
            source_class="A", target_class="B",
            predicate="p", instance_count=10_000,
        )
        assert compute_confidence(ev) == pytest.approx(1.0)

    def test_strictly_monotone_with_count(self):
        counts = [1, 10, 100, 1000]
        scores = [
            compute_confidence(
                ClassPairEvidence(
                    source_class="A", target_class="B",
                    predicate="p", instance_count=n,
                )
            )
            for n in counts
        ]
        assert scores == sorted(scores)
        assert len(set(scores)) == len(scores)  # no ties


# ---------------------------------------------------------------------------
# derive_class_mappings — aggregation and filter semantics
# ---------------------------------------------------------------------------


class TestDeriveClassMappings:
    def _run(self, edges, idx=None, **kw):
        return derive_class_mappings(edges, idx or _make_class_index(), **kw)

    def test_empty_input_produces_empty_output(self):
        pairs, stats = self._run([])
        assert pairs == []
        assert stats["input_edges"] == 0 and stats["output_edges"] == 0

    def test_single_matching_edge_yields_one_class_pair(self):
        pairs, _ = self._run([_edge(_PERSON_IRI, _DRUG_IRI)])
        assert len(pairs) == 1
        assert pairs[0].source_class == _CLASS_PERSON
        assert pairs[0].target_class == _CLASS_DRUG

    def test_repeated_edges_accumulate_into_one_pair(self):
        """N identical instance edges must yield 1 pair with count=N."""
        n = 7
        pairs, _ = self._run([_edge(_PERSON_IRI, _DRUG_IRI)] * n)
        assert len(pairs) == 1
        assert pairs[0].instance_count == n

    def test_entity_absent_from_index_causes_edge_to_be_dropped(self):
        pairs, stats = self._run([_edge("https://unknown.org/x", _DRUG_IRI)])
        assert pairs == []
        assert stats["input_edges"] == 1

    def test_min_instance_count_filter_drops_below_threshold(self):
        pairs, _ = self._run(
            [_edge(_PERSON_IRI, _DRUG_IRI)], min_instance_count=5
        )
        assert pairs == []

    def test_min_instance_count_filter_keeps_at_threshold(self):
        pairs, _ = self._run(
            [_edge(_PERSON_IRI, _DRUG_IRI)] * 5, min_instance_count=5
        )
        assert len(pairs) == 1

    def test_min_confidence_filter_drops_low_confidence(self):
        pairs, _ = self._run(
            [_edge(_PERSON_IRI, _DRUG_IRI)], min_confidence=0.99
        )
        assert pairs == []

    def test_output_sorted_descending_by_confidence(self):
        edges = (
            [_edge(_PERSON_IRI, _DRUG_IRI)] * 100
            + [_edge(_GENE_IRI, _PROTEIN_IRI)] * 2
        )
        pairs, _ = self._run(edges)
        assert pairs[0].confidence >= pairs[-1].confidence

    def test_entity_with_two_classes_expands_to_two_pairs(self):
        """One entity typed as two classes must produce an edge per class."""
        idx = ClassIndex(endpoint_url="https://example.org/sparql")
        idx.entities[_PERSON_IRI] = EntityClassInfo(
            entity_iri=_PERSON_IRI,
            graph_classes={_GRAPH_A: [_CLASS_PERSON, _CLASS_GENE]},
        )
        idx.entities[_DRUG_IRI] = EntityClassInfo(
            entity_iri=_DRUG_IRI,
            graph_classes={_GRAPH_B: [_CLASS_DRUG]},
        )
        pairs, _ = derive_class_mappings([_edge(_PERSON_IRI, _DRUG_IRI)], idx)
        class_pairs = {(p.source_class, p.target_class) for p in pairs}
        assert (_CLASS_PERSON, _CLASS_DRUG) in class_pairs
        assert (_CLASS_GENE, _CLASS_DRUG) in class_pairs

    def test_source_and_target_graphs_recorded(self):
        pairs, _ = self._run([_edge(_PERSON_IRI, _DRUG_IRI)])
        assert _GRAPH_A in pairs[0].source_graphs
        assert _GRAPH_B in pairs[0].target_graphs

    def test_predicate_preserved_on_majority_vote(self):
        """The dominant predicate must appear on the output pair."""
        edges = (
            [_edge(_PERSON_IRI, _DRUG_IRI, pred="skos:exactMatch")] * 3
            + [_edge(_PERSON_IRI, _DRUG_IRI, pred="skos:closeMatch")]
        )
        pairs, _ = self._run(edges)
        assert pairs[0].predicate == "skos:exactMatch"

    def test_to_mapping_edge_preserves_class_and_confidence(self):
        pairs, _ = self._run([_edge(_PERSON_IRI, _DRUG_IRI)] * 5)
        me = pairs[0].to_mapping_edge()
        assert me.source_class == _CLASS_PERSON
        assert me.target_class == _CLASS_DRUG
        assert me.confidence == pytest.approx(pairs[0].confidence)

    def test_stats_include_distribution_keys(self):
        _, stats = self._run([_edge(_PERSON_IRI, _DRUG_IRI)])
        assert "predicates_distribution" in stats
        assert "top_class_pairs" in stats
        assert "confidence_mean" in stats
