"""Tests for rdfsolve.utils - compact/expand round-trip and resolve."""
# ruff: noqa: D102

from __future__ import annotations

from rdfsolve.utils import compact_uri, expand_curie, resolve_curie

PREFIXES = {
    "foaf": "http://xmlns.com/foaf/0.1/",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "obo": "http://purl.obolibrary.org/obo/",
}


class TestCompactExpandRoundTrip:
    """The only thing that matters: compact -> expand is lossless."""

    def test_round_trip_slash(self):
        uri = "http://xmlns.com/foaf/0.1/Person"
        assert expand_curie(
            compact_uri(uri, PREFIXES), PREFIXES,
        ) == uri

    def test_round_trip_hash(self):
        uri = "http://www.w3.org/2000/01/rdf-schema#label"
        assert expand_curie(
            compact_uri(uri, PREFIXES), PREFIXES,
        ) == uri

    def test_unknown_prefix_passthrough(self):
        uri = "http://unknown.example.org/x"
        assert compact_uri(uri, PREFIXES) == uri
        assert expand_curie(
            "xyz:thing", PREFIXES,
        ) == "xyz:thing"


class TestResolveCurie:
    """resolve_curie is used in compose and iri modules."""

    def test_curie_to_bracketed(self):
        r = resolve_curie("foaf:Person", PREFIXES)
        assert r == "<http://xmlns.com/foaf/0.1/Person>"

    def test_full_uri_wrapped(self):
        r = resolve_curie(
            "http://example.org/x", PREFIXES,
        )
        assert r == "<http://example.org/x>"

    def test_rdf_type_shorthand(self):
        r = resolve_curie("a", PREFIXES)
        assert r is not None
        assert "rdf-syntax-ns#type" in r

    def test_blank_returns_none(self):
        assert resolve_curie("", PREFIXES) is None
