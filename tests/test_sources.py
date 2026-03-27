"""Tests for sources.py: loading, bioregistry enrichment, and JSON-LD export."""

from __future__ import annotations

import json

import pytest

from rdfsolve.sources import (
    SourceEntry,
    _resolve_bioregistry_prefix,
    enrich_source_with_bioregistry,
    get_bioregistry_metadata,
    load_sources,
    sources_to_jsonld,
)


# ── _resolve_bioregistry_prefix ───────────────────────────────────


class TestResolveBioregistryPrefix:
    """Unit tests for the four resolution strategies."""

    def test_exact_name_match(self) -> None:
        """Strategy 1: source name is itself a valid Bioregistry prefix."""
        entry: SourceEntry = {"name": "chebi"}
        assert _resolve_bioregistry_prefix(entry) == "chebi"

    def test_exact_name_match_hgnc(self) -> None:
        entry: SourceEntry = {"name": "hgnc"}
        assert _resolve_bioregistry_prefix(entry) == "hgnc"

    def test_root_prefix_match(self) -> None:
        """Strategy 2: root segment of dot-separated name resolves."""
        entry: SourceEntry = {"name": "drugbank.drugs"}
        assert _resolve_bioregistry_prefix(entry) == "drugbank"

    def test_root_prefix_match_chembl(self) -> None:
        entry: SourceEntry = {"name": "chembl.bigcat"}
        assert _resolve_bioregistry_prefix(entry) == "chembl"

    def test_root_prefix_match_mesh(self) -> None:
        entry: SourceEntry = {"name": "mesh.heading"}
        assert _resolve_bioregistry_prefix(entry) == "mesh"

    def test_local_provider_field(self) -> None:
        """Strategy 3: local_provider field contains the Bioregistry prefix."""
        entry: SourceEntry = {
            "name": "pubchem.ftp.anatomy",
            "local_provider": "pubchem",  # type: ignore[typeddict-unknown-key]
        }
        assert _resolve_bioregistry_prefix(entry) == "pubchem"

    def test_extra_provider_reverse_lookup(self) -> None:
        """Strategy 4: '{provider}.{dataset}' pattern via extra-provider index.

        bio2rdf.uniprot should resolve to 'uniprot' because the uniprot
        Bioregistry resource lists bio2rdf as an extra provider.
        """
        entry: SourceEntry = {"name": "bio2rdf.uniprot"}
        result = _resolve_bioregistry_prefix(entry)
        assert result == "uniprot"

    def test_extra_provider_drugbank(self) -> None:
        """bio2rdf.drugbank should resolve to 'drugbank'."""
        entry: SourceEntry = {"name": "bio2rdf.drugbank"}
        result = _resolve_bioregistry_prefix(entry)
        assert result == "drugbank"

    def test_unknown_source_returns_none(self) -> None:
        """No match → None."""
        entry: SourceEntry = {"name": "completely_unknown_xyz_dataset_9999"}
        assert _resolve_bioregistry_prefix(entry) is None

    def test_empty_name_returns_none(self) -> None:
        entry: SourceEntry = {"name": ""}
        assert _resolve_bioregistry_prefix(entry) is None


# ── get_bioregistry_metadata ──────────────────────────────────────


class TestGetBioregistryMetadata:
    def test_drugbank_basic_fields(self) -> None:
        meta = get_bioregistry_metadata("drugbank")
        assert meta["prefix"] == "drugbank"
        assert meta["name"] == "DrugBank"
        assert "homepage" in meta
        assert "domain" in meta
        assert meta["domain"] == "chemical"

    def test_drugbank_extra_providers(self) -> None:
        meta = get_bioregistry_metadata("drugbank")
        assert "extra_providers" in meta
        codes = [ep["code"] for ep in meta["extra_providers"]]
        assert "bio2rdf" in codes

    def test_drugbank_uri_prefix(self) -> None:
        meta = get_bioregistry_metadata("drugbank")
        assert "uri_prefix" in meta
        assert "drugbank" in meta["uri_prefix"].lower()

    def test_drugbank_uri_prefixes(self) -> None:
        meta = get_bioregistry_metadata("drugbank")
        assert "uri_prefixes" in meta
        assert isinstance(meta["uri_prefixes"], list)
        assert len(meta["uri_prefixes"]) > 1

    def test_chebi_publications(self) -> None:
        meta = get_bioregistry_metadata("chebi")
        assert "publications" in meta
        assert len(meta["publications"]) > 0
        assert "doi" in meta["publications"][0] or "pubmed" in meta["publications"][0]

    def test_hgnc_keywords(self) -> None:
        meta = get_bioregistry_metadata("hgnc")
        assert "keywords" in meta
        assert isinstance(meta["keywords"], list)

    def test_unknown_prefix_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown Bioregistry prefix"):
            get_bioregistry_metadata("_definitely_not_a_real_prefix_xyz")

    def test_mappings_present(self) -> None:
        meta = get_bioregistry_metadata("drugbank")
        assert "mappings" in meta
        # wikidata mapping is well-known for drugbank
        assert "wikidata" in meta["mappings"]

    def test_synonyms_present(self) -> None:
        meta = get_bioregistry_metadata("drugbank")
        assert "synonyms" in meta
        assert "DrugBank" in meta["synonyms"] or "DRUGBANK_ID" in meta["synonyms"]


# ── enrich_source_with_bioregistry ───────────────────────────────


class TestEnrichSourceWithBioregistry:
    def test_enriches_exact_match(self) -> None:
        entry: SourceEntry = {"name": "chebi"}
        prefix = enrich_source_with_bioregistry(entry)
        assert prefix == "chebi"
        assert entry.get("bioregistry_prefix") == "chebi"
        assert "bioregistry_name" in entry
        assert entry["bioregistry_name"] == "Chemical Entities of Biological Interest"

    def test_enriches_root_prefix(self) -> None:
        entry: SourceEntry = {"name": "drugbank.drugs"}
        prefix = enrich_source_with_bioregistry(entry)
        assert prefix == "drugbank"
        assert entry.get("bioregistry_prefix") == "drugbank"
        assert entry.get("bioregistry_name") == "DrugBank"
        assert "bioregistry_domain" in entry
        assert "bioregistry_uri_prefix" in entry

    def test_enriches_local_provider(self) -> None:
        entry: SourceEntry = {
            "name": "pubchem.ftp.anatomy",
            "local_provider": "pubchem",  # type: ignore[typeddict-unknown-key]
        }
        prefix = enrich_source_with_bioregistry(entry)
        assert prefix == "pubchem"
        assert entry.get("bioregistry_prefix") == "pubchem"

    def test_enriches_extra_provider_pattern(self) -> None:
        entry: SourceEntry = {"name": "bio2rdf.uniprot"}
        prefix = enrich_source_with_bioregistry(entry)
        assert prefix == "uniprot"
        assert entry.get("bioregistry_prefix") == "uniprot"

    def test_unknown_returns_none_no_modification(self) -> None:
        entry: SourceEntry = {"name": "zzz_unknown_xyz"}
        prefix = enrich_source_with_bioregistry(entry)
        assert prefix is None
        assert "bioregistry_prefix" not in entry

    def test_enrichment_includes_extra_providers_list(self) -> None:
        entry: SourceEntry = {"name": "drugbank"}
        enrich_source_with_bioregistry(entry)
        assert "bioregistry_extra_providers" in entry
        eps = entry["bioregistry_extra_providers"]
        assert isinstance(eps, list)
        codes = [ep["code"] for ep in eps]
        assert "bio2rdf" in codes

    def test_enrichment_includes_publications(self) -> None:
        entry: SourceEntry = {"name": "chebi"}
        enrich_source_with_bioregistry(entry)
        assert "bioregistry_publications" in entry
        pubs = entry["bioregistry_publications"]
        assert isinstance(pubs, list)
        assert len(pubs) > 0

    def test_enrichment_uri_prefixes_sorted(self) -> None:
        entry: SourceEntry = {"name": "mesh"}
        enrich_source_with_bioregistry(entry)
        if "bioregistry_uri_prefixes" in entry:
            prefixes = entry["bioregistry_uri_prefixes"]
            assert prefixes == sorted(prefixes)


# ── sources_to_jsonld ─────────────────────────────────────────────


class TestSourcesToJsonld:
    def test_basic_structure(self) -> None:
        entries: list[SourceEntry] = [
            {"name": "chebi", "endpoint": "https://example.org/sparql"},
        ]
        doc = sources_to_jsonld(entries)
        assert "@context" in doc
        assert "@graph" in doc
        assert len(doc["@graph"]) == 1

    def test_node_id_and_type(self) -> None:
        entries: list[SourceEntry] = [{"name": "chebi"}]
        doc = sources_to_jsonld(entries)
        node = doc["@graph"][0]
        assert node["@id"] == "https://rdfsolve.io/sources/chebi"
        assert node["@type"] == "dcat:Dataset"

    def test_enrich_false_no_bioregistry_fields(self) -> None:
        """Without enrich=True, bioregistry_* fields must not appear."""
        entries: list[SourceEntry] = [{"name": "chebi"}]
        doc = sources_to_jsonld(entries, enrich=False)
        node = doc["@graph"][0]
        assert "bioregistry_prefix" not in node
        assert "name" not in node  # no bioregistry_name was set

    def test_enrich_true_adds_metadata(self) -> None:
        """With enrich=True, bioregistry metadata is embedded."""
        entries: list[SourceEntry] = [{"name": "chebi"}]
        doc = sources_to_jsonld(entries, enrich=True)
        node = doc["@graph"][0]
        assert "bioregistry_prefix" in node
        assert node["bioregistry_prefix"] == "chebi"
        assert "name" in node
        assert "skos:exactMatch" in node

    def test_enrich_true_does_not_mutate_input(self) -> None:
        """enrich=True must not modify the original entry dict."""
        entry: SourceEntry = {"name": "chebi"}
        sources_to_jsonld([entry], enrich=True)
        assert "bioregistry_prefix" not in entry

    def test_endpoint_mapped(self) -> None:
        entries: list[SourceEntry] = [
            {"name": "chebi", "endpoint": "https://sparql.example.org/chebi"}
        ]
        doc = sources_to_jsonld(entries)
        node = doc["@graph"][0]
        assert node["endpoint"] == "https://sparql.example.org/chebi"

    def test_graph_uris_included(self) -> None:
        entries: list[SourceEntry] = [
            {"name": "chebi", "graph_uris": ["http://example.org/chebi"]}
        ]
        doc = sources_to_jsonld(entries)
        node = doc["@graph"][0]
        assert node["graph_uris"] == ["http://example.org/chebi"]

    def test_multiple_entries(self) -> None:
        entries: list[SourceEntry] = [
            {"name": "chebi"},
            {"name": "drugbank"},
            {"name": "mesh"},
        ]
        doc = sources_to_jsonld(entries, enrich=True)
        assert len(doc["@graph"]) == 3
        names_in_doc = {n["bioregistry_prefix"] for n in doc["@graph"] if "bioregistry_prefix" in n}
        assert {"chebi", "drugbank", "mesh"} <= names_in_doc

    def test_json_serialisable(self) -> None:
        """The output must be json.dumps-able without error."""
        entries: list[SourceEntry] = [{"name": "hgnc", "endpoint": "https://example.org/sparql"}]
        doc = sources_to_jsonld(entries, enrich=True)
        serialised = json.dumps(doc)
        loaded = json.loads(serialised)
        assert "@graph" in loaded

    def test_context_contains_void(self) -> None:
        doc = sources_to_jsonld([])
        assert "void" in doc["@context"]
        assert "dcat" in doc["@context"]
        assert "dcterms" in doc["@context"]

    def test_empty_entries_returns_empty_graph(self) -> None:
        doc = sources_to_jsonld([])
        assert doc["@graph"] == []

    def test_unknown_source_no_bioregistry_fields(self) -> None:
        """Unknown source with enrich=True should not crash; just omit BR fields."""
        entries: list[SourceEntry] = [{"name": "zzz_nonexistent_dataset_9999"}]
        doc = sources_to_jsonld(entries, enrich=True)
        node = doc["@graph"][0]
        assert "bioregistry_prefix" not in node


# ── Integration: load_sources + enrich ───────────────────────────


class TestLoadSourcesIntegration:
    def test_load_sources_returns_list(self) -> None:
        sources = load_sources()
        assert isinstance(sources, list)
        assert len(sources) > 0

    def test_known_source_enrichable(self) -> None:
        """At least one well-known source should resolve via bioregistry."""
        sources = load_sources()
        # find drugbank or chebi in the list
        well_known = {"chebi", "drugbank", "mesh", "hgnc", "uniprot"}
        enriched = []
        for src in sources:
            if src.get("name") in well_known:
                pfx = enrich_source_with_bioregistry(src)
                if pfx:
                    enriched.append(pfx)
                break
        assert len(enriched) > 0, "Expected at least one known source to enrich"

    def test_sources_to_jsonld_round_trip(self) -> None:
        """sources_to_jsonld output for a small slice is valid JSON."""
        sources = load_sources()[:5]
        doc = sources_to_jsonld(sources, enrich=True)
        assert len(doc["@graph"]) == 5
        # Must round-trip through json
        json.dumps(doc)
