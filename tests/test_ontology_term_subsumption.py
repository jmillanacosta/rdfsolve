import pytest
from rdflib import Dataset, Graph, URIRef
from rdflib.namespace import RDFS
from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.ontology_as_data import choose_representatives, pattern_classes

T = "urn:term:"
EX = "urn:ex:"
FIXTURE = '\n@prefix ex: <urn:ex:> .\n@prefix t: <urn:term:> .\n@prefix owl: <http://www.w3.org/2002/07/owl#> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix oio: <http://www.geneontology.org/formats/oboInOwl#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\nt:chemical a owl:Class .\nt:alcohol a owl:Class ; rdfs:subClassOf t:chemical .\nt:acid a owl:Class ; rdfs:subClassOf t:chemical .\nt:ethanol a owl:Class ; rdfs:subClassOf t:alcohol ; oio:hasDbXref "x:1" ; ex:smiles "CCO" .\nt:methanol a owl:Class ; rdfs:subClassOf t:alcohol .\nt:propanol a owl:Class ; rdfs:subClassOf t:alcohol .\nt:acetic a owl:Class ; rdfs:subClassOf t:acid .\nt:formic a owl:Class ; rdfs:subClassOf t:acid .\nt:unused1 a owl:Class ; rdfs:subClassOf t:acid ; oio:hasDbXref "x:2" ;\n    rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:role ; owl:someValuesFrom t:acid ] .\nt:unused2 a owl:Class ; rdfs:subClassOf t:alcohol ; oio:hasDbXref "x:3" .\n\nex:s1 a ex:Substance , t:ethanol ; ex:mass "46"^^xsd:decimal .\nex:s2 a ex:Substance , t:methanol ; ex:mass "32"^^xsd:decimal .\nex:s3 a ex:Substance , t:acetic ; ex:mass "60"^^xsd:decimal .\nex:p1 a ex:Participant ; ex:compound t:propanol .\nex:p2 a ex:Participant ; ex:compound t:formic .\n'


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
    dataset.graph(URIRef("urn:data")).parse(data="""
        <urn:s1> a <urn:S>, <urn:a>; <urn:p> "one" .
        <urn:s2> a <urn:S>, <urn:b>; <urn:p> "two" .
    """, format="turtle")
    dataset.graph(URIRef("urn:ontology")).parse(data="""
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        <urn:a> rdfs:subClassOf <urn:parent> .
        <urn:b> rdfs:subClassOf <urn:parent> .
        <urn:noise> a <urn:Decoy>; <urn:p> "not data" .
    """, format="turtle")
    dataset.default_graph.add((URIRef("urn:a"), RDFS.subClassOf, URIRef("urn:wrong")))
    with SchemaMiner.from_graph(dataset, graph_uris=["urn:data"], delay=0) as miner:
        scoped = mine_with_ontology(miner, ontology_as_data=True,
            ontology_term_budget=2, ontology_graph_uris=["urn:ontology"])
        patterns = _triples(scoped.data_schema)
        assert ("urn:parent", "urn:p", "Literal") in patterns, "Read the selected hierarchy graph"
        assert patterns["urn:parent", "urn:p", "Literal"].count == 2
        assert not {"urn:Decoy", "urn:wrong"} & pattern_classes(scoped.data_schema.patterns)
        assert miner.last_report.config["ontology_context"]["state"] == "nonempty"
        assert miner.last_report.config["ontology_term_subsumption"]["hierarchy_graph_uris"] == ["urn:ontology"]

        def unexpected_mining():
            pytest.fail("Missing required ontology context must be checked before mining")

        monkeypatch.setattr(miner, "_run_patterns_phase", unexpected_mining)
        with pytest.raises(ValueError, match="ontology graphs"):
            mine_with_ontology(miner, ontology_as_data=True, ontology_graph_uris=["urn:missing"])
        assert miner.last_report.config["ontology_context"]["state"] == "missing"
        assert miner.last_report.finished_at

    dataset.graph(URIRef("urn:data")).parse(data='''
        <urn:s1> <urn:ref> <urn:a> .
        <urn:a> <urn:description> "data value" .
    ''', format="turtle")
    dataset.graph(URIRef("urn:ontology")).parse(data='''
        <urn:a> a <http://www.w3.org/2002/07/owl#Class>,
            <http://www.w3.org/2000/01/rdf-schema#Class> .
        <urn:a> <urn:ontologyOnly> "excluded edge" .
    ''', format="turtle")
    with SchemaMiner.from_graph(dataset, graph_uris=["urn:data"], delay=0) as miner:
        result = mine_with_ontology(miner, ontology_as_data=True, ontology_term_budget=20,
                                    ontology_graph_uris=["urn:ontology"])
        patterns = {(p.subject_class, p.property_uri, p.object_class): p for p in result.data_schema.term_patterns}
        assert patterns["urn:S", "urn:ref", "urn:a"].count == 1, "Class declarations must not multiply data edges"
        assert patterns["urn:a", "urn:description", "Literal"].count == 1
        assert not any(p.property_uri == "urn:ontologyOnly" for p in result.data_schema.patterns)

        from rdfsolve.mining.edge_graph_split import split_by_edge_graph

        term_part = split_by_edge_graph(result.data_schema, "urn:data", "terms")
        attributed = {(p.subject_class, p.property_uri, p.object_class): p for p in term_part.term_patterns}
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
