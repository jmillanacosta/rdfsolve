"""Keep cross-registry fields from the source registry."""

from rdfsolve.models.source_model import SourcesRegistry

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


def test_models_keep_download_fields_for_mode_classification(tmp_path):
    from rdfsolve.sources import classify_source_mode, load_sources

    path = tmp_path / "sources.yaml"
    path.write_text(
        "- name: dump\n  download_nt: [https://example.org/a.nt.gz]\n"
        "- name: both\n  endpoint: https://example.org/sparql\n  download_ttl: https://example.org/b.ttl\n"
    )
    dump, both = load_sources(path)
    assert dump.model_extra == {"download_nt": ["https://example.org/a.nt.gz"]}
    assert (classify_source_mode(dump), classify_source_mode(both)) == ("local", "both")

