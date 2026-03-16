"""Tests for rdfsolve.models - schema and mapping lifecycle.

Covers the real data flows:
  1. MinedSchema: from_jsonld -> to_jsonld round-trip (preserves
     patterns, labels, counts, @about)
  2. MinedSchema.to_void_graph -> VoidParser reparse
  3. Mapping: from_jsonld -> to_jsonld round-trip (preserves edges,
     confidence, datasets)
  4. Mapping subclass to_jsonld provenance metadata
  5. Mapping.dataset_graph integration
  6. load_parser_from_jsonld (the full jsonld -> VoidParser pipeline)
"""
# ruff: noqa: D101, D102

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

DATA = Path(__file__).parent / "test_data"
SCHEMA_WITH_ABOUT = DATA / "minimal_schema.jsonld"
SSSOM_MAPPING = DATA / "sssom_mapping.jsonld"
INSTANCE_MAPPING = DATA / "instance_mapping.jsonld"


# ── MinedSchema full round-trip ───────────────────────────────────


class TestMinedSchemaRoundTrip:
    """Load jsonld -> MinedSchema -> to_jsonld -> reload -> compare."""

    def test_pattern_count_survives(self):
        from rdfsolve.models import MinedSchema

        ms = MinedSchema.from_jsonld(SCHEMA_WITH_ABOUT)
        doc = ms.to_jsonld()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False,
        ) as f:
            json.dump(doc, f)
            f.flush()
            ms2 = MinedSchema.from_jsonld(f.name)
        assert len(ms2.patterns) == len(ms.patterns)

    def test_about_survives(self):
        from rdfsolve.models import MinedSchema

        ms = MinedSchema.from_jsonld(SCHEMA_WITH_ABOUT)
        doc = ms.to_jsonld()
        assert doc["@about"]["dataset_name"] == "test_aopwiki"
        assert "@context" in doc
        assert "@graph" in doc

    def test_labels_survive(self):
        from rdfsolve.models import MinedSchema

        ms = MinedSchema.from_jsonld(SCHEMA_WITH_ABOUT)
        doc = ms.to_jsonld()
        assert len(doc.get("_labels", {})) > 0
        # labels actually attached to patterns
        labelled = [
            p for p in ms.patterns if p.subject_label
        ]
        assert len(labelled) > 0

    def test_counts_survive(self):
        from rdfsolve.models import MinedSchema

        doc = MinedSchema.from_jsonld(
            SCHEMA_WITH_ABOUT,
        ).to_jsonld()
        nodes_with_counts = [
            n for n in doc["@graph"] if "_counts" in n
        ]
        assert len(nodes_with_counts) > 0

    def test_get_classes_excludes_sentinels(self):
        from rdfsolve.models import MinedSchema

        classes = MinedSchema.from_jsonld(
            SCHEMA_WITH_ABOUT,
        ).get_classes()
        assert len(classes) >= 4
        assert "Literal" not in classes
        assert "Resource" not in classes


# ── MinedSchema -> VoID graph -> VoidParser ─────────────────────────


class TestMinedSchemaToVoidGraph:
    """to_void_graph is the bridge to all export formats."""

    def test_void_graph_has_triples(self):
        from rdfsolve.models import MinedSchema

        g = MinedSchema.from_jsonld(
            SCHEMA_WITH_ABOUT,
        ).to_void_graph()
        assert len(g) > 0

    def test_void_graph_parseable_by_voidparser(self):
        """The critical pipeline: jsonld -> VoID graph -> VoidParser."""
        from rdfsolve.models import MinedSchema
        from rdfsolve.parser import VoidParser

        g = MinedSchema.from_jsonld(
            SCHEMA_WITH_ABOUT,
        ).to_void_graph()
        vp = VoidParser(void_source=g)
        schema_df = vp.to_schema(
            filter_void_admin_nodes=True,
        )
        assert len(schema_df) > 0


# ── load_parser_from_jsonld (api.py) ──────────────────────────────


class TestLoadParserFromJsonLD:
    """The end-to-end pipeline: file -> MinedSchema -> VoID -> parser."""

    def test_returns_usable_parser(self):
        from rdfsolve.api import load_parser_from_jsonld

        vp = load_parser_from_jsonld(str(SCHEMA_WITH_ABOUT))
        schema_df = vp.to_schema(
            filter_void_admin_nodes=True,
        )
        assert len(schema_df) > 0


# ── Mapping round-trip ────────────────────────────────────────────


class TestMappingRoundTrip:
    """Load mapping fixtures, round-trip, check nothing is lost."""

    def test_sssom_edges_preserved(self):
        from rdfsolve.models import Mapping

        m = Mapping.from_jsonld(SSSOM_MAPPING)
        assert len(m.edges) == 2
        assert m.mapping_type == "sssom_import"
        # datasets populated from void:inDataset
        assert m.edges[0].source_dataset != ""

    def test_constructed_round_trip(self):
        from rdfsolve.models import (
            AboutMetadata,
            Mapping,
            MappingEdge,
        )

        skos = "http://www.w3.org/2004/02/skos/core#"
        original = Mapping(
            edges=[
                MappingEdge(
                    source_class="http://purl.obolibrary.org/obo/CHEBI_1",
                    target_class="http://identifiers.org/drugbank/DB1",
                    predicate=f"{skos}narrowMatch",
                    source_dataset="chebi",
                    target_dataset="drugbank",
                    confidence=0.95,
                ),
            ],
            about=AboutMetadata.build(
                strategy="instance_matcher",
            ),
            mapping_type="instance_matcher",
        )
        doc = original.to_jsonld()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False,
        ) as f:
            json.dump(doc, f)
            f.flush()
            reloaded = Mapping.from_jsonld(f.name)
        assert len(reloaded.edges) == 1
        assert reloaded.edges[0].confidence is not None
        assert abs(reloaded.edges[0].confidence - 0.95) < 0.01

    def test_to_networkx(self):
        pytest.importorskip("networkx")
        from rdfsolve.models import Mapping

        m = Mapping.from_jsonld(SSSOM_MAPPING)
        g = m.to_networkx()
        assert g.number_of_nodes() > 0
        assert g.number_of_edges() == 2


# ── Mapping subclasses - provenance in to_jsonld ──────────────────


class TestMappingSubclassProvenance:
    """Each subclass adds specific provenance keys to @about."""

    def _about(self, name, strategy):
        from rdfsolve.models import AboutMetadata

        return AboutMetadata(
            generated_by="test",
            generated_at="2025-01-01T00:00:00+00:00",
            dataset_name=name,
            pattern_count=0,
            strategy=strategy,
        )

    def test_instance_mapping_has_resource(self):
        from rdfsolve.models import InstanceMapping

        doc = InstanceMapping(
            edges=[],
            about=self._about("im", "instance_matcher"),
            resource_prefix="ensembl",
            uri_formats=["http://identifiers.org/ensembl/"],
        ).to_jsonld()
        assert doc["@about"]["resource"] == "ensembl"

    def test_sssom_mapping_merges_curie_map(self):
        from rdfsolve.models import SsomMapping

        chebi = "http://purl.obolibrary.org/obo/CHEBI_"
        doc = SsomMapping(
            edges=[],
            about=self._about("ss", "sssom_import"),
            source_name="ols_mappings",
            sssom_file="test.sssom.tsv",
            curie_map={"CHEBI": chebi},
        ).to_jsonld()
        assert doc["@about"]["sssom_source"] == "ols_mappings"
        # curie_map entries merged into @context
        assert doc["@context"]["CHEBI"] == chebi

    def test_inferenced_mapping_has_stats(self):
        from rdfsolve.models import InferencedMapping

        doc = InferencedMapping(
            edges=[],
            about=self._about("inf", "inferenced"),
            inference_types=["inversion", "transitivity"],
            source_mapping_files=["a.jsonld"],
            stats={"input_edges": 10, "output_edges": 15},
        ).to_jsonld()
        assert doc["@about"]["stats"]["output_edges"] == 15
        assert "inversion" in doc["@about"]["inference_types"]


# ── Mapping.dataset_graph ─────────────────────────────────────────


class TestDatasetGraph:

    def test_empty_paths(self):
        pytest.importorskip("networkx")
        from rdfsolve.models import Mapping

        g = Mapping.dataset_graph(
            paths=[], class_to_datasets={},
        )
        assert g.number_of_edges() == 0

    def test_with_schema_and_mapping(self):
        pytest.importorskip("networkx")
        from collections import defaultdict

        from rdfsolve.models import Mapping, MinedSchema

        ms = MinedSchema.from_jsonld(SCHEMA_WITH_ABOUT)
        c2d: dict[str, set[str]] = defaultdict(set)
        ds = ms.about.dataset_name or "test"
        for pat in ms.patterns:
            if pat.subject_class not in (
                "Literal", "Resource",
            ):
                c2d[pat.subject_class].add(ds)
        g = Mapping.dataset_graph(
            paths=[SSSOM_MAPPING],
            class_to_datasets=c2d,
        )
        assert g is not None
