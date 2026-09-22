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
    schema, report = _mine(budget=5)
    triples = _triples(schema)
    classes = pattern_classes(schema.patterns)
    assert len(classes) <= 5
    assert {EX + "Substance", EX + "Participant", T + "alcohol", T + "acid"} <= classes
    assert not classes & {T + "ethanol", T + "methanol", T + "propanol", T + "acetic", T + "formic"}
    lifted = triples[T + "alcohol", EX + "mass", "Literal"]
    assert lifted.evidence_source == "inferred"
    assert lifted.count == 2
    assert (EX + "Participant", EX + "compound", T + "alcohol") in triples
    assert (EX + "Participant", EX + "compound", T + "acid") in triples
    assert (T + "alcohol", EX + "smiles", "Literal") in triples
    assert triples[EX + "Substance", EX + "mass", "Literal"].evidence_source == "mined"
    summary = report.config["ontology_term_subsumption"]
    assert summary["subsumed"] is True
    assert summary["representatives"] == {T + "acid": 2, T + "alcohol": 3}
    assert "+ontology-as-data" in schema.about.strategy
    cycle = choose_representatives(["a", "z"], {"a": {"b"}, "b": {"a"}}, budget=1)
    assert cycle.over_budget and cycle.classes_after == 2, "Cycle cannot satisfy the budget"

    raw, report = _mine(budget=1, hierarchy=False)
    summary = report.config["ontology_term_subsumption"]
    assert summary["subsumed"] is False, "No hierarchy must not count as subsumption"
    assert summary["over_budget"] and summary["representatives"] == {}
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
        patterns = _triples(result.data_schema)
        assert patterns["urn:S", "urn:ref", "urn:a"].count == 1, "Class declarations must not multiply data edges"
        assert patterns["urn:a", "urn:description", "Literal"].count == 1
        assert not any(p.property_uri == "urn:ontologyOnly" for p in result.data_schema.patterns)

        from rdfsolve.mining.edge_graph_split import split_by_edge_graph

        term_part = split_by_edge_graph(result.data_schema, "urn:data", "terms")
        attributed = _triples(term_part)
        assert attributed["urn:S", "urn:ref", "urn:a"].graphs == {"urn:data": 1}
        assert attributed["urn:a", "urn:description", "Literal"].count == 1
