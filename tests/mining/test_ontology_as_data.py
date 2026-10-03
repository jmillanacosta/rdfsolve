"""rdfsolve.mining.ontology_as_data: ontology terms as data. Terms that type or are the objects of
records are subsumed under their ancestors until the class budget holds, keeping measured leaf
rows; a source that keeps its records as classes has them grouped under their kinds; term edges
are probed only where typed patterns have term objects, and a refused probe leaves the source
partial; term and type bindings keep separate witnesses."""

import json

import pytest
from rdflib import Dataset, Graph, URIRef
from rdflib.namespace import RDFS

from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.edge_graph_split import split_by_edge_graph
from rdfsolve.mining.enrichment import example_query
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.ontology_as_data import choose_representatives, pattern_classes
from rdfsolve.release import build_release_manifest
from rdfsolve.release.scientific_validation import (
    build_scientific_validation_plan,
    pattern_existence_query,
)
from rdfsolve.schema_models import MinedSchema
from rdfsolve.sparql_helper import EndpointError
from tests.mining.data import EX, FIXTURE, T


def _mine(budget, hierarchy=True):
    graph = Graph().parse(data=FIXTURE, format="turtle")
    if not hierarchy:
        graph.remove((None, RDFS.subClassOf, None))
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        result = mine_with_ontology(
            miner, dataset_name="fixture", ontology_as_data=True, ontology_term_budget=budget
        )
        return (result.data_schema, miner.last_report)


def _triples(schema):
    return {(p.subject_class, p.property_uri, p.object_class): p for p in schema.patterns}


def test_terms_are_subsumed_until_the_budget_holds(monkeypatch):
    from rdfsolve.mining.local_graph import LocalGraphHelper

    label_queries = []
    select = LocalGraphHelper.select

    def recorded_select(helper, query, **options):
        if options.get("purpose") == "labels":
            label_queries.append(query)
        return select(helper, query, **options)

    monkeypatch.setattr(LocalGraphHelper, "select", recorded_select)
    schema, report = _mine(budget=6)
    assert label_queries and not any(T + "ethanol" in q for q in label_queries), (
        "Retaining raw evidence must not fetch labels for each discarded leaf"
    )
    triples = _triples(schema)
    assert schema.raw_patterns is not None, "Keep typed observations before interpretation"
    observed = {(p.subject_class, p.property_uri, p.object_class): p for p in schema.raw_patterns}
    leaf = observed[T + "ethanol", EX + "mass", "Literal"]
    assert leaf.count == 1 and leaf.evidence_source == "mined", "Keep measured leaf counts"
    assert leaf.pattern_type.value == "datatype_property"
    assert (T + "alcohol", EX + "mass", "Literal") not in observed
    assert all(p.evidence_source != "inferred" for p in schema.raw_patterns)
    assert type(schema).from_dict(schema.to_dict()).raw_patterns == schema.raw_patterns
    classes = pattern_classes(schema.patterns)
    assert len(classes) <= 6
    assert {EX + "Substance", EX + "Participant", T + "alcohol", T + "acid"} <= classes
    assert not classes & {T + "ethanol", T + "methanol", T + "propanol", T + "acetic", T + "formic"}
    lifted = triples[T + "alcohol", EX + "mass", "Literal"]
    assert lifted.evidence_source == "inferred"
    assert lifted.count == 2
    terms = {(p.subject_class, p.property_uri, p.object_class): p for p in schema.term_patterns}
    assert terms[EX + "Participant", EX + "compound", T + "propanol"].object_binding == "term"
    assert terms[T + "ethanol", EX + "smiles", "Literal"].subject_binding == "term"
    assert triples[EX + "Substance", EX + "mass", "Literal"].evidence_source == "mined"
    summary = report.config["ontology_term_subsumption"]
    assert summary["subsumed"] is True
    assert summary["representatives"] == {T + "acid": 1, T + "alcohol": 2}
    saved = type(report).model_validate_json(report.model_dump_json())
    assert saved.config["ontology_term_subsumption"]["representative_members"] == {
        T + "acid": [T + "acetic"],
        T + "alcohol": [T + "ethanol", T + "methanol"],
    }, "The report must identify the terms replaced by each representative"
    assert "+ontology-as-data" in schema.about.strategy
    cycle = choose_representatives(["a", "z"], {"a": {"b"}, "b": {"a"}}, budget=1)
    assert cycle.over_budget and cycle.classes_after == 2, "Cycle cannot satisfy the budget"

    raw, report = _mine(budget=1, hierarchy=False)
    summary = report.config["ontology_term_subsumption"]
    assert summary["subsumed"] is False, "No hierarchy must not count as subsumption"
    assert summary["over_budget"] and summary["representatives"] == {}
    assert summary["representative_members"] == {}, "No hierarchy must yield no replacements"
    assert T + "ethanol" in pattern_classes(raw.patterns)
    assert "+ontology-as-data" not in raw.about.strategy

    dataset = Dataset(default_union=False)
    dataset.graph(URIRef("urn:data")).parse(
        data="""
        <urn:s1> a <urn:S>, <urn:a>; <urn:p> "one" .
        <urn:s2> a <urn:S>, <urn:b>; <urn:p> "two" .
    """,
        format="turtle",
    )
    dataset.graph(URIRef("urn:ontology")).parse(
        data="""
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        <urn:a> rdfs:subClassOf <urn:parent> .
        <urn:b> rdfs:subClassOf <urn:parent> .
        <urn:noise> a <urn:Decoy>; <urn:p> "not data" .
    """,
        format="turtle",
    )
    dataset.default_graph.add((URIRef("urn:a"), RDFS.subClassOf, URIRef("urn:wrong")))
    with SchemaMiner.from_graph(dataset, graph_uris=["urn:data"], delay=0) as miner:
        scoped = mine_with_ontology(
            miner,
            ontology_as_data=True,
            ontology_term_budget=2,
            ontology_graph_uris=["urn:ontology"],
        )
        patterns = _triples(scoped.data_schema)
        assert ("urn:parent", "urn:p", "Literal") in patterns, "Read the selected hierarchy graph"
        assert patterns["urn:parent", "urn:p", "Literal"].count == 2
        assert not {"urn:Decoy", "urn:wrong"} & pattern_classes(scoped.data_schema.patterns)
        assert miner.last_report.config["ontology_context"]["state"] == "nonempty"
        assert miner.last_report.config["ontology_term_subsumption"]["hierarchy_graph_uris"] == [
            "urn:ontology"
        ]

        def unexpected_mining():
            pytest.fail("Missing required ontology context must be checked before mining")

        monkeypatch.setattr(miner, "_run_patterns_phase", unexpected_mining)
        with pytest.raises(ValueError, match="ontology graphs"):
            mine_with_ontology(miner, ontology_as_data=True, ontology_graph_uris=["urn:missing"])
        assert miner.last_report.config["ontology_context"]["state"] == "missing"
        assert miner.last_report.finished_at

    dataset.graph(URIRef("urn:data")).parse(
        data="""
        <urn:s1> <urn:ref> <urn:a> .
        <urn:a> <urn:description> "data value" .
    """,
        format="turtle",
    )
    dataset.graph(URIRef("urn:ontology")).parse(
        data="""
        <urn:a> a <http://www.w3.org/2002/07/owl#Class>,
            <http://www.w3.org/2000/01/rdf-schema#Class> .
        <urn:a> <urn:ontologyOnly> "excluded edge" .
    """,
        format="turtle",
    )
    with SchemaMiner.from_graph(dataset, graph_uris=["urn:data"], delay=0) as miner:
        result = mine_with_ontology(
            miner,
            ontology_as_data=True,
            ontology_term_budget=20,
            ontology_graph_uris=["urn:ontology"],
        )
        patterns = {
            (p.subject_class, p.property_uri, p.object_class): p
            for p in result.data_schema.term_patterns
        }
        assert patterns["urn:S", "urn:ref", "urn:a"].count == 1, (
            "Class declarations must not multiply data edges"
        )
        assert patterns["urn:a", "urn:description", "Literal"].count == 1
        assert not any(p.property_uri == "urn:ontologyOnly" for p in result.data_schema.patterns)

        from rdfsolve.mining.edge_graph_split import split_by_edge_graph

        term_part = split_by_edge_graph(result.data_schema, "urn:data", "terms")
        attributed = {
            (p.subject_class, p.property_uri, p.object_class): p for p in term_part.term_patterns
        }
        assert attributed["urn:S", "urn:ref", "urn:a"].graphs == {"urn:data": 1}
        assert attributed["urn:a", "urn:description", "Literal"].count == 1


CLASS_LEVEL = """@prefix rh: <urn:rh:> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
rh:Reaction a rdfs:Class . rh:ReactionSide a rdfs:Class . rh:Compound a rdfs:Class .
rh:r1 a rdfs:Class ; rdfs:subClassOf rh:Reaction ; rh:side rh:r1_L ; rh:equation "A = B" .
rh:r2 a rdfs:Class ; rdfs:subClassOf rh:Reaction ; rh:side rh:r2_L ; rh:equation "C = D" .
rh:r1_L a rdfs:Class ; rdfs:subClassOf rh:ReactionSide ; rh:contains rh:c1 .
rh:r2_L a rdfs:Class ; rdfs:subClassOf rh:ReactionSide ; rh:contains rh:c2 .
rh:c1 a rdfs:Class ; rdfs:subClassOf rh:Compound ; rh:name "A" .
rh:c2 a rdfs:Class ; rdfs:subClassOf rh:Compound ; rh:name "C" .
"""


@pytest.mark.parametrize("classes_as_data", [False, True])
def test_a_source_that_keeps_its_records_as_classes(classes_as_data):
    """Every entity is a class under its kind (Rhea's model): with classes_as_data, the rows of
    these classes are grouped under their kinds (a reaction has a side, a side contains a
    compound); without it, they stay exact term rows only."""
    graph = Graph().parse(data=CLASS_LEVEL, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0, classes_as_data=classes_as_data) as miner:
        schema = mine_with_ontology(
            miner, dataset_name="class-level", ontology_as_data=True, ontology_term_budget=3
        ).data_schema
    kinds = {(p.subject_class, p.property_uri, p.object_class) for p in schema.patterns}
    expected = {
        ("urn:rh:Reaction", "urn:rh:side", "urn:rh:ReactionSide"),
        ("urn:rh:ReactionSide", "urn:rh:contains", "urn:rh:Compound"),
        ("urn:rh:Reaction", "urn:rh:equation", "Literal"),
    }
    assert (expected <= kinds) is classes_as_data
    terms = {(p.subject_class, p.property_uri, p.object_class) for p in schema.term_patterns}
    assert ("urn:rh:r1", "urn:rh:side", "urn:rh:r1_L") in terms, "Exact rows are kept either way"


@pytest.mark.parametrize("terms_first", [False, True])
def test_the_term_probes_are_valid_sparql_with_named_graphs(terms_first):
    """With data and ontology graphs selected (a local index with the endpoint's graphs), both
    forms of the term probe are valid SPARQL: QLever refused the terms-first form whose union
    stood outside its subquery."""
    from rdflib.plugins.sparql import prepareQuery

    from rdfsolve.mining.ontology_as_data import build_term_object_query, build_term_subject_query

    graphs = ["urn:graph:data", "urn:graph:ontology"]
    prepareQuery(build_term_subject_query(graphs, graphs, terms_first=terms_first))
    prepareQuery(build_term_object_query(graphs, graphs))


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


def test_a_refused_term_probe_leaves_the_source_partial(monkeypatch):
    schema, report, _ = mine(monkeypatch, "qlever", refuse=True)
    assert schema.term_patterns is None and schema.patterns, "Typed patterns are kept"
    assert report.completion_state == "partial"
    assert any(f.purpose == "ontology-terms" for f in report.query_failures)


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
