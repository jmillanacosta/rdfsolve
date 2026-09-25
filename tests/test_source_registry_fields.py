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

    from rdfsolve.mappings.identifiers import resolve_identifiers
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
