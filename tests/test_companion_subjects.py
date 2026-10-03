"""Mine chemical-group fields when their types come from companion context."""

from rdflib import Dataset

from rdfsolve import SchemaMiner


def test_classifications_with_companion_subject_types():
    data = Dataset(default_union=False)
    data.parse(data="""
        @prefix e: <urn:chemical:> .
        e:data {
            e:c1 a e:Chemical; e:group e:g1 .
            e:c2 a e:Chemical; e:group e:g2 .
        }
        e:labels { e:g1 e:label "PFAS"@en, "PFAS group"@en . e:g2 e:label "Other"@en . }
        e:types {
            e:g1 a e:Group . e:g2 a e:Group .
            e:ghost a e:Group; e:label "Ghost"@en .
            e:foreign a e:Outside; e:noise "Exclude" .
            e:g1 e:label "Context-only label"@en .
        }
        e:outside { e:g2 e:label "Outside scope"@en . }
    """, format="trig")
    results = []
    for strategy in ("two-phase", "single-pass", "one-shot"):
        with SchemaMiner.from_graph(
            data, graph_uris=["urn:chemical:data", "urn:chemical:labels"],
            type_context_graph_uris=["urn:chemical:types"], strategy=strategy,
            delay=0, enrich=True, examples_per_pattern=2,
        ) as miner:
            schema = miner.mine("chemical-classifications")
            assert miner.last_report.config["local_backend"]["engine"] == "oxigraph"
            assert miner.last_report.completion_state == "complete"
            labels = [p for p in schema.patterns if p.subject_class == "urn:chemical:Group"
                      and p.property_uri == "urn:chemical:label"]
            assert len(labels) == 1, "Discover fields on data subjects typed in companion context"
            assert labels[0].count == 3 and labels[0].graphs == {"urn:chemical:labels": 3}
            assert schema.about.class_entity_counts["urn:chemical:Group"] == 2
            assert "urn:chemical:Outside" not in schema.get_classes()
            assert not schema.structural_patterns, "Recovered typed labels need no structural fallback"
            examples = [e for e in schema.enrichment.examples
                        if e.subject_class == "urn:chemical:Group"]
            assert examples and all(e.value.value in {"PFAS", "PFAS group", "Other"} for e in examples)
            nav = schema.discover_paths(max_hops=2)
            route = next(p for p in nav.paths if p.steps[0].property_uri == "urn:chemical:group"
                         and p.steps[-1].property_uri == "urn:chemical:label")
            schema.probe_paths([route], helper=miner.helper)
            assert (route.source_count, route.matched_sources) == (2, 2)
            rows = miner.helper.select(schema.select(paths=[route]).path_query(route))
            assert len(rows["results"]["bindings"]) == 3, "Keep all real group labels"
            results.append({(p.subject_class, p.property_uri, p.object_class, p.datatype): p.count
                            for p in schema.patterns})
    assert results[0] == results[1] == results[2], "Strategies must agree on measured evidence"
