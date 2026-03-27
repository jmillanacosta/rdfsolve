"""Tests for rdfsolve.ontology (OlsClient + OntologyIndex)."""
import gzip
import pickle
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rdfsolve.ontology.index import (
    OntologyIndex,
    _build_ontology_graph,
    _normalise,
    _ontologies_matching_uris,
    build_ontology_index,
    load_ontology_index,
    save_ontology_index,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CHEBI_ID = "chebi"
GO_ID = "go"
CHEBI_BASE = "http://purl.obolibrary.org/obo/CHEBI_"
GO_BASE = "http://purl.obolibrary.org/obo/GO_"
ASPIRIN_IRI = "http://purl.obolibrary.org/obo/CHEBI_15422"
CELLULAR_COMPONENT_IRI = "http://purl.obolibrary.org/obo/GO_0005575"

_CHEBI_META: dict[str, Any] = {
    "ontologyId": CHEBI_ID,
    "preferredPrefix": "CHEBI",
    "baseUri": [CHEBI_BASE],
    "domain": "chemistry",
    "numberOfClasses": 100,
    "importsFrom": [],
    "exportsTo": [],
}

_GO_META: dict[str, Any] = {
    "ontologyId": GO_ID,
    "preferredPrefix": "GO",
    "baseUri": [GO_BASE],
    "domain": "biology",
    "numberOfClasses": 200,
    "importsFrom": [CHEBI_ID],
    "exportsTo": [],
}

_ASPIRIN_TERM: dict[str, Any] = {
    "iri": ASPIRIN_IRI,
    "label": "aspirin",
    "synonyms": ["acetylsalicylic acid", "ASA"],
}

_CC_TERM: dict[str, Any] = {
    "iri": CELLULAR_COMPONENT_IRI,
    "label": "cellular_component",
    "synonyms": ["cell part"],
}


def _make_mock_client(
    ontology_metas: list[dict[str, Any]],
    term_map: dict[tuple[str, str], dict[str, Any]] | None = None,
    ancestors_map: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
) -> MagicMock:
    """Return a MagicMock that mimics OlsClient with canned responses."""
    client = MagicMock()
    client.__enter__ = lambda s: s
    client.__exit__ = MagicMock(return_value=False)

    client.get_all_ontologies.return_value = iter(ontology_metas)
    client.get_ontology.side_effect = lambda oid: next(
        (m for m in ontology_metas if m["ontologyId"] == oid), None
    )

    term_map = term_map or {}
    ancestors_map = ancestors_map or {}

    def _get_term(oid: str, iri: str) -> dict[str, Any] | None:
        return term_map.get((oid, iri))

    def _get_terms(oid: str, search: str = "", rows: int = 20) -> list[dict[str, Any]]:
        return [t for (o, _), t in term_map.items() if o == oid]

    def _get_all_terms(oid: str, page_limit: int | None = None) -> list[dict[str, Any]]:
        return [t for (o, _), t in term_map.items() if o == oid]

    def _get_ancestors(oid: str, iri: str) -> list[dict[str, Any]]:
        return ancestors_map.get((oid, iri), [])

    client.get_term_by_iri.side_effect = _get_term
    client.get_terms.side_effect = _get_terms
    client.get_all_terms.side_effect = _get_all_terms
    client.get_ancestors.side_effect = _get_ancestors

    return client


# ---------------------------------------------------------------------------
# _normalise
# ---------------------------------------------------------------------------


class TestNormalise:
    def test_lower_case(self) -> None:
        assert _normalise("Aspirin") == "aspirin"

    def test_strips_whitespace(self) -> None:
        assert _normalise("  foo bar  ") == "foo bar"

    def test_empty_string(self) -> None:
        assert _normalise("") == ""


# ---------------------------------------------------------------------------
# _build_ontology_graph
# ---------------------------------------------------------------------------


class TestBuildOntologyGraph:
    def test_nodes_added(self) -> None:
        g = _build_ontology_graph([_CHEBI_META, _GO_META])
        assert CHEBI_ID in g.nodes
        assert GO_ID in g.nodes

    def test_import_edge(self) -> None:
        g = _build_ontology_graph([_CHEBI_META, _GO_META])
        # GO importsFrom CHEBI → edge GO→CHEBI
        assert g.has_edge(GO_ID, CHEBI_ID)

    def test_node_attributes(self) -> None:
        g = _build_ontology_graph([_CHEBI_META])
        assert g.nodes[CHEBI_ID]["preferred_prefix"] == "CHEBI"
        assert g.nodes[CHEBI_ID]["n_classes"] == 100

    def test_empty_input(self) -> None:
        g = _build_ontology_graph([])
        assert g.number_of_nodes() == 0

    def test_missing_ontology_id_skipped(self) -> None:
        g = _build_ontology_graph([{"preferredPrefix": "ANON"}])
        assert g.number_of_nodes() == 0


# ---------------------------------------------------------------------------
# _ontologies_matching_uris
# ---------------------------------------------------------------------------


class TestOntologiesMatchingUris:
    def test_matches_by_prefix(self) -> None:
        base_map = {CHEBI_BASE: CHEBI_ID, GO_BASE: GO_ID}
        result = _ontologies_matching_uris({ASPIRIN_IRI}, base_map)
        assert result == {CHEBI_ID}

    def test_no_match(self) -> None:
        base_map = {CHEBI_BASE: CHEBI_ID}
        result = _ontologies_matching_uris({"http://example.org/X_1"}, base_map)
        assert result == set()

    def test_multiple_matches(self) -> None:
        base_map = {CHEBI_BASE: CHEBI_ID, GO_BASE: GO_ID}
        result = _ontologies_matching_uris({ASPIRIN_IRI, CELLULAR_COMPONENT_IRI}, base_map)
        assert result == {CHEBI_ID, GO_ID}


# ---------------------------------------------------------------------------
# OntologyIndex — query methods
# ---------------------------------------------------------------------------


class TestOntologyIndex:
    def _make_index(self) -> OntologyIndex:
        idx = OntologyIndex()
        idx.term_to_classes["aspirin"] = [ASPIRIN_IRI]
        idx.term_to_classes["acetylsalicylic acid"] = [ASPIRIN_IRI]
        idx.class_to_ontology[ASPIRIN_IRI] = CHEBI_ID
        idx.ancestors[ASPIRIN_IRI] = ["http://purl.obolibrary.org/obo/CHEBI_36807"]
        idx.base_uri_to_ontology[CHEBI_BASE] = CHEBI_ID
        return idx

    def test_lookup_exact(self) -> None:
        idx = self._make_index()
        assert idx.lookup("aspirin") == [ASPIRIN_IRI]

    def test_lookup_case_insensitive(self) -> None:
        idx = self._make_index()
        assert idx.lookup("ASPIRIN") == [ASPIRIN_IRI]

    def test_lookup_missing(self) -> None:
        idx = self._make_index()
        assert idx.lookup("glucose") == []

    def test_ontology_for_class(self) -> None:
        idx = self._make_index()
        assert idx.ontology_for_class(ASPIRIN_IRI) == CHEBI_ID

    def test_ontology_for_class_missing(self) -> None:
        idx = self._make_index()
        assert idx.ontology_for_class("http://example.org/X") is None

    def test_ontology_for_base_uri_longest(self) -> None:
        idx = self._make_index()
        # Add a more-specific prefix
        idx.base_uri_to_ontology["http://purl.obolibrary.org/obo/CHEBI_154"] = "chebi_sub"
        result = idx.ontology_for_base_uri("http://purl.obolibrary.org/obo/CHEBI_15422")
        assert result == "chebi_sub"  # longest match wins

    def test_ontology_for_base_uri_no_match(self) -> None:
        idx = self._make_index()
        assert idx.ontology_for_base_uri("http://example.org/Z") is None

    def test_stats_keys(self) -> None:
        idx = self._make_index()
        s = idx.stats()
        assert set(s) == {"terms", "classes", "ontologies", "base_uris", "with_ancestors"}

    def test_stats_values(self) -> None:
        idx = self._make_index()
        assert idx.stats()["terms"] == 2
        assert idx.stats()["classes"] == 1
        assert idx.stats()["with_ancestors"] == 1

    def test_import_neighbours_no_graph(self) -> None:
        idx = self._make_index()
        # ontology_graph is None by default in _make_index
        assert idx.import_neighbours(CHEBI_ID) == set()

    def test_import_neighbours_with_graph(self) -> None:
        idx = self._make_index()
        idx.ontology_graph = _build_ontology_graph([_CHEBI_META, _GO_META])
        neighbours = idx.import_neighbours(GO_ID, depth=1)
        assert CHEBI_ID in neighbours

    def test_import_neighbours_unknown_node(self) -> None:
        idx = self._make_index()
        idx.ontology_graph = _build_ontology_graph([_CHEBI_META])
        assert idx.import_neighbours("nonexistent") == set()


# ---------------------------------------------------------------------------
# save / load round-trip
# ---------------------------------------------------------------------------


class TestSaveLoadRoundTrip:
    def _make_populated_index(self) -> OntologyIndex:
        idx = OntologyIndex()
        idx.term_to_classes["aspirin"] = [ASPIRIN_IRI]
        idx.class_to_ontology[ASPIRIN_IRI] = CHEBI_ID
        idx.ancestors[ASPIRIN_IRI] = ["http://purl.obolibrary.org/obo/CHEBI_36807"]
        idx.base_uri_to_ontology[CHEBI_BASE] = CHEBI_ID
        idx.ontology_graph = _build_ontology_graph([_CHEBI_META, _GO_META])
        return idx

    def test_round_trip(self) -> None:
        idx = self._make_populated_index()
        with tempfile.TemporaryDirectory() as tmpdir:
            save_ontology_index(idx, tmpdir)
            restored = load_ontology_index(tmpdir)

        assert restored.term_to_classes == idx.term_to_classes
        assert restored.class_to_ontology == idx.class_to_ontology
        assert restored.ancestors == idx.ancestors
        assert restored.base_uri_to_ontology == idx.base_uri_to_ontology

    def test_graph_restored(self) -> None:
        idx = self._make_populated_index()
        with tempfile.TemporaryDirectory() as tmpdir:
            save_ontology_index(idx, tmpdir)
            restored = load_ontology_index(tmpdir)

        assert restored.ontology_graph is not None
        assert CHEBI_ID in restored.ontology_graph.nodes
        assert GO_ID in restored.ontology_graph.nodes

    def test_missing_index_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(FileNotFoundError, match="ontology_index.pkl.gz"):
                load_ontology_index(tmpdir)

    def test_graph_missing_graceful(self) -> None:
        idx = self._make_populated_index()
        with tempfile.TemporaryDirectory() as tmpdir:
            save_ontology_index(idx, tmpdir)
            # Remove the graph file
            (Path(tmpdir) / "ontology_graph.graphml").unlink()
            restored = load_ontology_index(tmpdir)
        assert restored.ontology_graph is None

    def test_pickle_file_is_compressed(self) -> None:
        idx = self._make_populated_index()
        with tempfile.TemporaryDirectory() as tmpdir:
            save_ontology_index(idx, tmpdir)
            pkl_path = Path(tmpdir) / "ontology_index.pkl.gz"
            with gzip.open(pkl_path, "rb") as fh:
                payload = pickle.load(fh)
        assert "term_to_classes" in payload

    def test_output_directory_created(self) -> None:
        idx = self._make_populated_index()
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = Path(tmpdir) / "sub" / "dir"
            save_ontology_index(idx, nested)
            assert (nested / "ontology_index.pkl.gz").exists()


# ---------------------------------------------------------------------------
# build_ontology_index (mocked OlsClient)
# ---------------------------------------------------------------------------


_OLS_PATCH = "rdfsolve.ontology.ols_client.OlsClient"


class TestBuildOntologyIndex:
    def test_build_with_schema_uris(self) -> None:
        mock_client = _make_mock_client(
            ontology_metas=[_CHEBI_META, _GO_META],
            term_map={(CHEBI_ID, ASPIRIN_IRI): _ASPIRIN_TERM},
            ancestors_map={(CHEBI_ID, ASPIRIN_IRI): []},
        )
        with patch(_OLS_PATCH, return_value=mock_client):
            idx = build_ontology_index(schema_class_uris={ASPIRIN_IRI})

        assert idx.lookup("aspirin") == [ASPIRIN_IRI]
        assert idx.ontology_for_class(ASPIRIN_IRI) == CHEBI_ID

    def test_build_with_explicit_ontology_ids(self) -> None:
        mock_client = _make_mock_client(
            ontology_metas=[_CHEBI_META],
            term_map={(CHEBI_ID, ASPIRIN_IRI): _ASPIRIN_TERM},
        )
        with patch(_OLS_PATCH, return_value=mock_client):
            idx = build_ontology_index(ontology_ids=[CHEBI_ID])

        # get_ontology should be called (not get_all_ontologies)
        mock_client.get_ontology.assert_called_with(CHEBI_ID)
        mock_client.get_all_ontologies.assert_not_called()
        assert CHEBI_ID in idx.base_uri_to_ontology.values()

    def test_synonym_indexed(self) -> None:
        mock_client = _make_mock_client(
            ontology_metas=[_CHEBI_META],
            term_map={(CHEBI_ID, ASPIRIN_IRI): _ASPIRIN_TERM},
            ancestors_map={(CHEBI_ID, ASPIRIN_IRI): []},
        )
        with patch(_OLS_PATCH, return_value=mock_client):
            idx = build_ontology_index(schema_class_uris={ASPIRIN_IRI})

        assert idx.lookup("acetylsalicylic acid") == [ASPIRIN_IRI]
        assert idx.lookup("asa") == [ASPIRIN_IRI]

    def test_base_uri_populated(self) -> None:
        mock_client = _make_mock_client(ontology_metas=[_CHEBI_META, _GO_META])
        with patch(_OLS_PATCH, return_value=mock_client):
            idx = build_ontology_index()

        assert idx.base_uri_to_ontology.get(CHEBI_BASE) == CHEBI_ID
        assert idx.base_uri_to_ontology.get(GO_BASE) == GO_ID

    def test_ontology_graph_built(self) -> None:
        mock_client = _make_mock_client(ontology_metas=[_CHEBI_META, _GO_META])
        with patch(_OLS_PATCH, return_value=mock_client):
            idx = build_ontology_index()

        assert idx.ontology_graph is not None
        assert CHEBI_ID in idx.ontology_graph.nodes

    def test_missing_term_skipped(self) -> None:
        # get_term_by_iri returns None → class IRI should not appear
        mock_client = _make_mock_client(
            ontology_metas=[_CHEBI_META],
            term_map={},  # empty — all lookups return None
        )
        with patch(_OLS_PATCH, return_value=mock_client):
            idx = build_ontology_index(schema_class_uris={ASPIRIN_IRI})

        assert idx.ontology_for_class(ASPIRIN_IRI) is None

    def test_stats_after_build(self) -> None:
        mock_client = _make_mock_client(
            ontology_metas=[_CHEBI_META],
            term_map={(CHEBI_ID, ASPIRIN_IRI): _ASPIRIN_TERM},
            ancestors_map={(CHEBI_ID, ASPIRIN_IRI): []},
        )
        with patch(_OLS_PATCH, return_value=mock_client):
            idx = build_ontology_index(schema_class_uris={ASPIRIN_IRI})

        s = idx.stats()
        assert s["classes"] >= 1
        assert s["terms"] >= 1

    def test_build_without_schema_uris_uses_get_all_terms(self) -> None:
        # When no schema_class_uris given, _index_ontology_top_terms uses get_all_terms
        mock_client = _make_mock_client(
            ontology_metas=[_CHEBI_META],
            term_map={(CHEBI_ID, ASPIRIN_IRI): _ASPIRIN_TERM},
        )
        with patch(_OLS_PATCH, return_value=mock_client):
            idx = build_ontology_index(ontology_ids=[CHEBI_ID])

        mock_client.get_all_terms.assert_called_once_with(CHEBI_ID, page_limit=1)
        assert idx.lookup("aspirin") == [ASPIRIN_IRI]


# ---------------------------------------------------------------------------
# OlsClient (unit, mocked httpx)
# ---------------------------------------------------------------------------


class TestOlsClient:
    def _response(self, data: Any) -> MagicMock:
        """Build a mock httpx Response for the given JSON data."""
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = data
        return resp

    @pytest.fixture()
    def client(self) -> Any:
        from rdfsolve.ontology.ols_client import OlsClient

        return OlsClient(cache_dir=None)

    def test_get_all_ontologies_single_page(self, client: Any) -> None:
        page = {
            "elements": [_CHEBI_META],
            "totalElements": 1,
            "totalPages": 1,
        }
        with patch("httpx.get", return_value=self._response(page)):
            results = list(client.get_all_ontologies())
        assert len(results) == 1
        assert results[0]["ontologyId"] == CHEBI_ID

    def test_get_all_ontologies_two_pages(self, client: Any) -> None:
        page1 = {
            "elements": [_CHEBI_META],
            "totalElements": 2,
            "totalPages": 2,
        }
        page2 = {
            "elements": [_GO_META],
            "totalElements": 2,
            "totalPages": 2,
        }
        with patch("httpx.get", side_effect=[self._response(page1), self._response(page2)]):
            results = list(client.get_all_ontologies())
        assert len(results) == 2

    def test_get_terms_returns_list(self, client: Any) -> None:
        payload = {"elements": [_ASPIRIN_TERM], "totalElements": 1, "totalPages": 1}
        with patch("httpx.get", return_value=self._response(payload)):
            terms = client.get_terms(CHEBI_ID, search="aspirin")
        assert terms == [_ASPIRIN_TERM]

    def test_get_all_terms_single_page(self, client: Any) -> None:
        payload = {"elements": [_ASPIRIN_TERM], "totalElements": 1, "totalPages": 1}
        with patch("httpx.get", return_value=self._response(payload)):
            results = list(client.get_all_terms(CHEBI_ID))
        assert results == [_ASPIRIN_TERM]

    def test_get_all_terms_two_pages(self, client: Any) -> None:
        page1 = {
            "elements": [_ASPIRIN_TERM],
            "totalElements": 2,
            "totalPages": 2,
        }
        page2 = {
            "elements": [_CC_TERM],
            "totalElements": 2,
            "totalPages": 2,
        }
        with patch("httpx.get", side_effect=[self._response(page1), self._response(page2)]):
            results = list(client.get_all_terms(CHEBI_ID))
        assert len(results) == 2

    def test_get_all_terms_page_limit(self, client: Any) -> None:
        page1 = {
            "elements": [_ASPIRIN_TERM],
            "totalElements": 2,
            "totalPages": 2,
        }
        with patch("httpx.get", return_value=self._response(page1)):
            results = list(client.get_all_terms(CHEBI_ID, page_limit=1))
        assert len(results) == 1

    def test_get_term_by_iri_found(self, client: Any) -> None:
        payload = {"elements": [_ASPIRIN_TERM], "totalElements": 1, "totalPages": 1}
        with patch("httpx.get", return_value=self._response(payload)):
            term = client.get_term_by_iri(CHEBI_ID, ASPIRIN_IRI)
        assert term == _ASPIRIN_TERM

    def test_get_term_by_iri_not_found(self, client: Any) -> None:
        payload = {"elements": [], "totalElements": 0, "totalPages": 0}
        with patch("httpx.get", return_value=self._response(payload)):
            term = client.get_term_by_iri(CHEBI_ID, "http://example.org/NOPE")
        assert term is None

    def test_get_ontology_found(self, client: Any) -> None:
        # get_ontology hits /ontologies/{id} which returns the object directly
        with patch("httpx.get", return_value=self._response(_CHEBI_META)):
            meta = client.get_ontology(CHEBI_ID)
        assert meta is not None
        assert meta["ontologyId"] == CHEBI_ID

    def test_get_ontology_not_found(self, client: Any) -> None:
        with patch("httpx.get", side_effect=Exception("404")):
            meta = client.get_ontology("nonexistent")
        assert meta is None

    def test_http_error_returns_none(self, client: Any) -> None:
        import httpx

        with patch("httpx.get", side_effect=httpx.HTTPError("timeout")):
            result = client._get("http://example.org/test")  # noqa: SLF001
        assert result is None

    def test_context_manager(self) -> None:
        from rdfsolve.ontology.ols_client import OlsClient

        with OlsClient(cache_dir=None) as c:
            assert c is not None
