"""Keep cross-registry fields from the source registry."""

from rdfsolve.models.source_model import SourcesRegistry
from rdfsolve.sources import load_sources

REGISTRY = """
- name: aopwikirdf
  endpoint: https://aopwiki.rdf.bigcat-bioinformatics.org/sparql/
  kg_registry_id: biobricks-aopwiki
  in_kamdar: true
  terminology_nomenclature: [phenotype, topics]
- name: plain
  endpoint: https://example.org/sparql
"""


def test_models_keep_cross_registry_fields(tmp_path):
    path = tmp_path / "sources.yaml"
    path.write_text(REGISTRY)
    first, second = SourcesRegistry.from_yaml(path).sources
    assert first.kg_registry_id == "biobricks-aopwiki"
    assert first.in_kamdar is True
    assert first.terminology_nomenclature == ["phenotype", "topics"]
    assert (second.kg_registry_id, second.in_kamdar, second.terminology_nomenclature) == (
        "",
        False,
        [],
    )


def test_entries_keep_cross_registry_fields(tmp_path):
    path = tmp_path / "sources.yaml"
    path.write_text(REGISTRY)
    first, second = load_sources(path)
    assert first["kg_registry_id"] == "biobricks-aopwiki"
    assert first["in_kamdar"] is True
    assert first["terminology_nomenclature"] == ["phenotype", "topics"]
    assert "kg_registry_id" not in second
