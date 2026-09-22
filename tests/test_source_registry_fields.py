def test_models_keep_download_fields_for_mode_classification(tmp_path):
    from rdfsolve.sources import classify_source_mode, load_sources

    path = tmp_path / "sources.yaml"
    path.write_text(
        "- name: dump\n  download_nt: [https://example.org/a.nt.gz]\n- name: both\n  endpoint: https://example.org/sparql\n  download_ttl: https://example.org/b.ttl\n"
    )
    dump, both = load_sources(path)
    assert dump.model_extra == {"download_nt": ["https://example.org/a.nt.gz"]}
    assert (classify_source_mode(dump), classify_source_mode(both)) == ("local", "both")
