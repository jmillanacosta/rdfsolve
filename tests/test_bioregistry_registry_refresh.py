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
    assert enrich_source_with_bioregistry(explicit) == "test", "Curated identity takes precedence over display names"
    guessed = SourceModel(name="test.component", local_provider="test")
    assert enrich_source_with_bioregistry(guessed) is None, "A provider or name prefix is not a dataset identifier"
    declared = SourceModel(name="bio2rdf.test")
    assert enrich_source_with_bioregistry(declared) == "test", "A registry-declared provider correspondence remains usable"
