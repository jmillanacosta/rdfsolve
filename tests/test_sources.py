"""rdfsolve.sources: the source registry, its fields and its refresh from Bioregistry."""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
import yaml

from rdfsolve.sources import enrich_registry_with_bioregistry


class _Provider:
    def __init__(self, code: str = "bio2rdf"):
        self.code = code
        self.name = "Bio2RDF"
        self.uri_format = "http://bio2rdf.org/test:$1"
        self.homepage = None
        self.description = None


class _Resource:
    domain = "biology"

    def get_name(self):
        return "Test Resource"

    def get_description(self):
        return "Description"

    def get_homepage(self):
        return "https://example.org"

    def get_license(self):
        return "CC0"

    def get_logo(self):
        return None

    def get_keywords(self):
        return {"test", "biology"}

    def get_publications(self):
        return []

    def get_uri_prefix(self):
        return "http://example.org/id/"

    def get_uri_prefixes(self):
        return {"http://example.org/id/"}

    def get_synonyms(self):
        return {"TEST"}

    def get_mappings(self):
        return {"miriam": "test"}

    def get_extra_providers(self):
        return [_Provider()]


def _fake_bioregistry(monkeypatch):
    resource = _Resource()
    module = types.ModuleType("bioregistry")
    module.get_resource = lambda prefix: resource if prefix == "test" else None
    module.get_repository = lambda prefix: "https://github.com/example/test"
    module.get_owl_download = lambda prefix: "https://reference.example/test.owl"
    module.get_rdf_download = lambda prefix: "https://reference.example/test.ttl"
    module.get_obo_download = lambda prefix: "https://reference.example/test.obo"
    module.manager = types.SimpleNamespace(registry={"test": resource})
    monkeypatch.setitem(sys.modules, "bioregistry", module)


def test_registry_refresh_can_write_copy_without_touching_source(tmp_path: Path, monkeypatch):
    _fake_bioregistry(monkeypatch)
    source = tmp_path / "sources.yaml"
    output = tmp_path / "enriched.yaml"
    original = "- name: test\n  download_owl: [https://ftp.example/test.owl]\n"
    source.write_text(original, encoding="utf-8")
    enrich_registry_with_bioregistry(source, output=output)
    assert source.read_text(encoding="utf-8") == original
    assert yaml.safe_load(output.read_text(encoding="utf-8"))[0]["bioregistry_prefix"] == "test"
    saved = yaml.safe_load(output.read_text())[0]
    assert saved["download_owl"] == ["https://ftp.example/test.owl"], "Curated download"
    assert saved["bioregistry_owl_download"] == "https://reference.example/test.owl", (
        "Reference download"
    )
    with pytest.raises(ValueError, match="separate file"):
        enrich_registry_with_bioregistry(source, output=source)
    assert source.read_text() == original, "Source registry is read-only"
    from rdfsolve.models.source_model import SourceModel
    from rdfsolve.sources import enrich_source_with_bioregistry

    explicit = SourceModel(name="unrelated", bioregistry_prefix="test")
    assert enrich_source_with_bioregistry(explicit) == "test", (
        "Curated identity takes precedence over display names"
    )
    guessed = SourceModel(name="test.component", local_provider="test")
    assert enrich_source_with_bioregistry(guessed) is None, (
        "A provider or name prefix is not a dataset identifier"
    )
    declared = SourceModel(name="bio2rdf.test")
    assert enrich_source_with_bioregistry(declared) == "test", (
        "A registry-declared provider correspondence remains usable"
    )


def test_models_keep_download_fields_for_mode_classification(tmp_path):
    from rdfsolve.sources import classify_source_mode, load_sources

    path = tmp_path / "sources.yaml"
    path.write_text(
        "- name: dump\n  download_nt: [https://example.org/a.nt.gz]\n- name: both\n  endpoint: https://example.org/sparql\n  download_ttl: https://example.org/b.ttl\n"
    )
    dump, both = load_sources(path)
    assert dump.model_extra == {"download_nt": ["https://example.org/a.nt.gz"]}
    assert (classify_source_mode(dump), classify_source_mode(both)) == ("local", "both")


def test_sidecar_supplies_namespaces_without_changing_access(tmp_path):
    import json

    import pytest

    from rdfsolve.identifiers import resolve_identifiers
    from rdfsolve.schema_models.enrichment import RdfTerm
    from rdfsolve.sources import load_sources

    path = tmp_path / "sources.yaml"
    path.write_text(
        "- name: mesh\n  bioregistry_prefix: mesh\n  endpoint: https://example.org/sparql\n"
    )
    original = path.read_bytes()
    sidecar = path.with_suffix(".metadata.json")
    records = {
        "mesh": {
            "bioregistry_prefix": "mesh",
            "bioregistry_uri_prefixes": ["http://id.nlm.nih.gov/mesh/"],
            "bioregistry_package_version": "test-snapshot",
        }
    }
    sidecar.write_text(json.dumps({"sources": records}))
    source = load_sources(path)[0]
    report = resolve_identifiers(
        [RdfTerm(kind="literal", value="mesh:D000001")],
        source,
        target_iris=["http://id.nlm.nih.gov/mesh/D000001"],
        mode="namespace",
    )
    assert report.results[0].status == "resolved", report
    assert report.registry_version == "test-snapshot"
    assert source.endpoint == "https://example.org/sparql"
    assert path.read_bytes() == original
    records["mesh"]["endpoint"] = "https://wrong.example/sparql"
    sidecar.write_text(json.dumps({"sources": records}))
    with pytest.raises(ValueError, match="metadata"):
        load_sources(path)
    records["mesh"].pop("endpoint")
    records["mesh"]["bioregistry_prefix"] = "chebi"
    sidecar.write_text(json.dumps({"sources": records}))
    with pytest.raises(ValueError, match="prefix"):
        load_sources(path)


def test_the_shipped_registry_loads_with_its_metadata_sidecar():
    """data/sources.yaml and data/sources.metadata.json agree on every prefix: a curated prefix
    change without the sidecar's record would refuse the whole registry."""
    from pathlib import Path

    from rdfsolve.sources import load_sources

    sources = load_sources(Path(__file__).parents[1] / "data" / "sources.yaml")
    assert len(sources) > 200
    assert {s.name for s in sources if s.classes_as_data} >= {"rhea", "swisslipids"}


def test_the_registry_adds_iri_formats_that_bioregistry_lacks(tmp_path, monkeypatch):
    """A source's uri_formats join Bioregistry's formats of its prefix in identifier candidates
    (Rhea's RDF writes http://rdf.rhea-db.org/<id>, which Bioregistry does not list)."""
    from rdfsolve import identifiers, sources

    registry = tmp_path / "sources.yaml"
    registry.write_text(
        "- name: rhea\n  bioregistry_prefix: rhea\n  uri_formats:\n  - http://rdf.rhea-db.org/$1\n"
    )
    monkeypatch.setattr(sources, "DEFAULT_SOURCES_YAML", registry)
    identifiers.registry_uri_formats.cache_clear()
    try:
        assert "http://rdf.rhea-db.org/21812" in identifiers.candidates("rhea:21812")[0]
    finally:
        identifiers.registry_uri_formats.cache_clear()
