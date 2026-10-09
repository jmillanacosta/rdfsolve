"""rdfsolve.mining.ontology_as_data term probes: edges to ontology terms are read only for the
(class, property) pairs whose typed patterns have term objects, from the terms first on QLever; a
refused probe is a measurement gap (term counts are enrichment), read in batches of terms or over
a sample first; term and type bindings keep separate witnesses."""

import json

import pytest
from rdflib import Dataset, Graph, URIRef

from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.edge_graph_split import split_by_edge_graph
from rdfsolve.mining.enrichment import example_query
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.release import build_release_manifest
from rdfsolve.release.scientific_validation import (
    build_scientific_validation_plan,
    pattern_existence_query,
)
from rdfsolve.schema_models import MinedSchema
from rdfsolve.sparql_helper import EndpointError
from tests.mining.data import FIXTURE


def mine(monkeypatch, engine, refuse=False):
    with SchemaMiner.from_graph(Graph().parse(data=FIXTURE, format="turtle"), delay=0) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", engine)
        select, sent = miner.helper.select, []

        def answer(query, *args, purpose="", **kwargs):
            if purpose.startswith("ontology-terms/object"):
                sent.append(query)
                if refuse:
                    raise EndpointError("Tried to allocate 54 GB, but only 37.2 GB were available")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", answer)
        result = mine_with_ontology(
            miner, dataset_name="terms", ontology_as_data=True, ontology_term_budget=100
        )
        return result.data_schema, miner.last_report, sent


def terms(schema):
    return sorted(
        (p.subject_class, p.property_uri, p.object_class, p.datatype, p.count)
        for p in schema.term_patterns
    )


def test_term_edges_are_read_from_the_terms_first_on_qlever(monkeypatch):
    plain, _, _ = mine(monkeypatch, "virtuoso")
    fast, _, sent = mine(monkeypatch, "qlever")
    assert (
        terms(fast)
        == terms(plain)
        == [
            ("urn:ex:Participant", "urn:ex:compound", "urn:term:formic", None, 1),
            ("urn:ex:Participant", "urn:ex:compound", "urn:term:propanol", None, 1),
            (
                "urn:term:ethanol",
                "urn:ex:smiles",
                "Literal",
                "http://www.w3.org/2001/XMLSchema#string",
                1,
            ),
        ]
    )
    assert len(sent) == 1 and "<urn:ex:compound>" in sent[0], (
        "Only the typed pairs with term objects"
    )


def test_a_refused_term_probe_is_a_gap_not_a_failure(monkeypatch):
    schema, report, _ = mine(monkeypatch, "qlever", refuse=True)
    assert schema.patterns, "Typed patterns are kept"
    # Term counts are enrichment: the refused object counts are a gap; the subject rows stand.
    assert [(p.subject_class, p.property_uri) for p in schema.term_patterns] == [
        ("urn:term:ethanol", "urn:ex:smiles")
    ]
    assert report.completion_state == "complete"
    assert not report.query_failures
    assert any(g.purpose == "ontology-terms/object" for g in report.measurement_gaps)
    assert report.config["ontology_term_probe"]["state"] == "gaps"


def test_term_records_and_instances_keep_separate_witnesses(tmp_path):
    data = Dataset(default_union=False)
    data.graph(URIRef("urn:data")).parse(
        data="""
        <urn:instance> a <urn:T>; <urn:p> "instance value" .
        <urn:T> <urn:p> "record value"; <urn:rel> <urn:U>, <urn:object> .
        <urn:object> a <urn:U> .
        <urn:U> <urn:p> "other record" .
        <urn:s> a <urn:S>; <urn:link> <urn:T> .
    """,
        format="turtle",
    )
    data.graph(URIRef("urn:ontology")).parse(
        data="""
        <urn:T> a <http://www.w3.org/2002/07/owl#Class>;
            <http://www.w3.org/2000/01/rdf-schema#subClassOf> <urn:Parent> .
        <urn:U> a <http://www.w3.org/2002/07/owl#Class> .
        <urn:T> <urn:contextOnly> "excluded" .
    """,
        format="turtle",
    )
    with SchemaMiner.from_graph(
        data, graph_uris=["urn:data"], type_context_graph_uris=["urn:ontology"], delay=0
    ) as miner:
        schema = mine_with_ontology(
            miner,
            dataset_name="fixture",
            ontology_as_data=True,
            ontology_term_budget=1,
            ontology_graph_uris=["urn:ontology"],
        ).data_schema
        terms = schema.term_patterns
        assert terms, "Ontology probes must retain direct term bindings"
        assert all(p.evidence_source == "mined" for p in terms)
        assert not any(p.property_uri == "urn:contextOnly" for p in terms)
        assert "urn:T" not in schema.about.class_entity_counts, "A record is not a class population"
        assert (
            next(
                p.count
                for p in schema.patterns
                if p.subject_class == "urn:Parent" and p.property_uri == "urn:p"
            )
            == 1
        )
        assert (
            next(
                p.count
                for p in schema.raw_patterns
                if p.subject_class == "urn:T" and p.property_uri == "urn:p"
            )
            == 1
        )

        selected = {(p.subject_class, p.property_uri, p.object_binding): p for p in terms}
        expected = {
            ("urn:T", "urn:p", "type"): ("urn:T", "record value"),
            ("urn:T", "urn:rel", "term"): ("urn:T", "urn:U"),
            ("urn:T", "urn:rel", "type"): ("urn:T", "urn:object"),
            ("urn:S", "urn:link", "term"): ("urn:s", "urn:T"),
        }
        for key, witness in expected.items():
            pattern = selected[key]
            assert pattern.count == 1, f"Merged different bindings for {key}"
            query = pattern_existence_query(
                pattern, ["urn:data"], type_context_graph_scope=["urn:ontology"]
            )
            rows = miner.helper.select(query)["results"]["bindings"]
            assert [(row["s"]["value"], row["o"]["value"]) for row in rows] == [witness], key
            rows = miner.helper.select(example_query(pattern, ["urn:data"], 2, ["urn:ontology"]))[
                "results"
            ]["bindings"]
            assert [(row["subject"]["value"], row["value"]["value"]) for row in rows] == [
                witness
            ], key

        with pytest.raises(ValueError, match="term_patterns"):
            MinedSchema(patterns=terms, about=schema.about)
        with pytest.raises(ValueError, match="term"):
            type(terms[0])(
                subject_class="urn:T",
                property_uri="urn:p",
                object_class="Literal",
                object_binding="term",
            )
        part = split_by_edge_graph(schema, "urn:data", "fixture")
        assert part.term_patterns == terms
        assert split_by_edge_graph(schema, "urn:other", "empty").term_patterns == []

    folder = tmp_path / "fixture"
    folder.mkdir()
    (tmp_path / "sources.yaml").write_text("- name: fixture")
    path = folder / "fixture_local_schema.json"
    path.write_text(json.dumps(schema.to_dict()))
    restored = MinedSchema.from_json(path)
    assert restored.term_patterns == terms
    plan = build_scientific_validation_plan(
        build_release_manifest(tmp_path), tmp_path, patterns_per_schema=100
    )
    checks = [
        c
        for c in plan.pattern_checks
        if c.pattern.get("subject_class") == "urn:T" and c.pattern["property_uri"] == "urn:rel"
    ]
    assert len(checks) == 2 and len({c.check_id for c in checks}) == 2, (
        "Validation must distinguish term and type bindings with the same IRIs"
    )
