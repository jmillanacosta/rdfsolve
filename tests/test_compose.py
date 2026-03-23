"""Tests for rdfsolve.compose - SPARQL query composition.

Covers: variable naming, forward/reverse edges, multi-edge chains,
variable reuse across paths (fan pattern), and the JSON-LD export.
"""
# ruff: noqa: D101, D102

from __future__ import annotations

from rdfsolve.compose import compose_query_from_paths

PREFIXES = {
    "wp": "http://vocabularies.wikipathways.org/wp#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
}


def _edge(src, pred, tgt, *, forward=True):
    return {
        "source": src,
        "target": tgt,
        "predicate": pred,
        "is_forward": forward,
    }


class TestForwardEdge:

    def test_query_has_triple_pattern(self):
        r = compose_query_from_paths(
            [{"edges": [_edge(
                "http://ex.org/Gene",
                "http://ex.org/encodes",
                "http://ex.org/Protein",
            )]}],
            PREFIXES,
        )
        q = r["query"]
        assert "SELECT DISTINCT" in q
        assert "encodes" in q
        assert len(r["variable_map"]) == 2


class TestReverseEdge:

    def test_reverse_swaps_subject_object(self):
        r = compose_query_from_paths(
            [{"edges": [_edge(
                "http://ex.org/A",
                "http://ex.org/p",
                "http://ex.org/B",
                forward=False,
            )]}],
            PREFIXES,
        )
        # Both variables present regardless of direction
        assert len(r["variable_map"]) == 2


class TestMultiEdgeChain:

    def test_chain_creates_three_variables(self):
        r = compose_query_from_paths(
            [{"edges": [
                _edge(
                    "http://ex.org/A",
                    "http://ex.org/p",
                    "http://ex.org/B",
                ),
                _edge(
                    "http://ex.org/B",
                    "http://ex.org/q",
                    "http://ex.org/C",
                ),
            ]}],
            PREFIXES,
        )
        assert len(r["variable_map"]) == 3


class TestFanPattern:
    """Two single-edge paths sharing a source should reuse vars."""

    def test_shared_source_reuses_variable(self):
        src = "http://ex.org/Gene"
        r = compose_query_from_paths(
            [
                {"edges": [_edge(
                    src, "http://ex.org/p", "http://ex.org/X",
                )]},
                {"edges": [_edge(
                    src, "http://ex.org/q", "http://ex.org/Y",
                )]},
            ],
            PREFIXES,
        )
        # Source variable should appear once, not duplicated
        vals = list(r["variable_map"].values())
        assert vals.count(src) == 1


class TestOptionsAndExport:

    def test_limit(self):
        q = compose_query_from_paths(
            [{"edges": [_edge(
                "http://ex.org/A",
                "http://ex.org/p",
                "http://ex.org/B",
            )]}],
            PREFIXES,
            {"limit": 42},
        )["query"]
        assert "LIMIT 42" in q

    def test_jsonld_export(self):
        jld = compose_query_from_paths(
            [{"edges": [_edge(
                "http://ex.org/A",
                "http://ex.org/p",
                "http://ex.org/B",
            )]}],
            PREFIXES,
        )["jsonld"]
        assert "sh:select" in jld
        assert "schema:dateCreated" in jld

    def test_empty_paths(self):
        r = compose_query_from_paths([], PREFIXES)
        assert r["variable_map"] == {}
        assert isinstance(r["query"], str)
