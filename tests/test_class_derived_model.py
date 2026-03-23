"""Tests for ClassDerivedMapping.

Focus on contract boundaries and invariants.
"""
# ruff: noqa: D101, D102

from __future__ import annotations

import pytest

from rdfsolve.mapping_models.class_derived import ClassDerivedMapping
from rdfsolve.mapping_models.core import MappingEdge
from rdfsolve.schema_models.core import AboutMetadata


def _make_mapping(**kwargs) -> ClassDerivedMapping:
    about = AboutMetadata.build(
        dataset_name="test", pattern_count=1, strategy="class_derived"
    )
    defaults = dict(
        edges=[
            MappingEdge(
                source_class="http://example.org/A",
                target_class="http://example.org/B",
                predicate="skos:exactMatch",
                source_dataset="g_a",
                target_dataset="g_b",
            )
        ],
        about=about,
        source_mapping_type="sssom_import",
        source_mapping_files=["x.jsonld"],
        derivation_stats={"input_edges": 100},
        enrichment_stats={"enriched_edges": 80},
        class_index_endpoint="https://sparql.example.org",
    )
    defaults.update(kwargs)
    return ClassDerivedMapping(**defaults)


class TestRequiredFields:
    def test_missing_source_mapping_type_raises(self):
        """source_mapping_type is required — must fail at construction."""
        with pytest.raises(Exception):
            _make_mapping(source_mapping_type=None)  # type: ignore[arg-type]

    def test_mapping_type_cannot_be_overridden_to_wrong_value(self):
        """mapping_type default is class_derived; passing a different string
        is accepted by Pydantic (it's just a str field), but the default
        must be class_derived when not specified."""
        m = _make_mapping()
        assert m.mapping_type == "class_derived"

    def test_empty_source_files_list_is_valid(self):
        """source_mapping_files defaults to []; empty list must be accepted."""
        m = _make_mapping(source_mapping_files=[])
        assert m.source_mapping_files == []


class TestJsonldContract:
    """The @about block must expose provenance fields consumers depend on."""

    def test_strategy_is_class_derived(self):
        about = _make_mapping().to_jsonld()["@about"]
        assert about["strategy"] == "class_derived"

    def test_source_mapping_type_present(self):
        about = _make_mapping().to_jsonld()["@about"]
        assert about["source_mapping_type"] == "sssom_import"

    def test_non_empty_stats_appear_in_about(self):
        about = _make_mapping(
            derivation_stats={"input_edges": 5},
            enrichment_stats={"enriched_edges": 3},
        ).to_jsonld()["@about"]
        assert "derivation_stats" in about
        assert "enrichment_stats" in about

    def test_empty_stats_are_omitted(self):
        """Empty dicts must NOT pollute the @about block."""
        about = _make_mapping(
            derivation_stats={}, enrichment_stats={}
        ).to_jsonld()["@about"]
        assert "derivation_stats" not in about
        assert "enrichment_stats" not in about

    def test_no_endpoint_absent_from_about(self):
        """None endpoint must not appear as a key at all."""
        about = _make_mapping(class_index_endpoint=None).to_jsonld()["@about"]
        assert "class_index_endpoint" not in about

    def test_edge_count_matches_graph_length(self):
        """@graph entry count equals number of distinct source subjects.
        Use OBO URIs so bioregistry can produce distinct CURIEs."""
        # SO_0000001..4 are all distinct subjects → 4 @graph nodes
        edges = [
            MappingEdge(
                source_class=f"http://purl.obolibrary.org/obo/SO_000000{i}",
                target_class=f"http://purl.obolibrary.org/obo/GO_000000{i}",
                predicate="skos:exactMatch",
                source_dataset="g",
                target_dataset="g",
            )
            for i in range(1, 5)
        ]
        doc = _make_mapping(edges=edges).to_jsonld()
        assert len(doc["@graph"]) == 4
