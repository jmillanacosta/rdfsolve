from rdflib import Graph
from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.ontology_as_data import choose_representatives, pattern_classes

T = "urn:term:"
EX = "urn:ex:"
FIXTURE = '\n@prefix ex: <urn:ex:> .\n@prefix t: <urn:term:> .\n@prefix owl: <http://www.w3.org/2002/07/owl#> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix oio: <http://www.geneontology.org/formats/oboInOwl#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\nt:chemical a owl:Class .\nt:alcohol a owl:Class ; rdfs:subClassOf t:chemical .\nt:acid a owl:Class ; rdfs:subClassOf t:chemical .\nt:ethanol a owl:Class ; rdfs:subClassOf t:alcohol ; oio:hasDbXref "x:1" ; ex:smiles "CCO" .\nt:methanol a owl:Class ; rdfs:subClassOf t:alcohol .\nt:propanol a owl:Class ; rdfs:subClassOf t:alcohol .\nt:acetic a owl:Class ; rdfs:subClassOf t:acid .\nt:formic a owl:Class ; rdfs:subClassOf t:acid .\nt:unused1 a owl:Class ; rdfs:subClassOf t:acid ; oio:hasDbXref "x:2" ;\n    rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:role ; owl:someValuesFrom t:acid ] .\nt:unused2 a owl:Class ; rdfs:subClassOf t:alcohol ; oio:hasDbXref "x:3" .\n\nex:s1 a ex:Substance , t:ethanol ; ex:mass "46"^^xsd:decimal .\nex:s2 a ex:Substance , t:methanol ; ex:mass "32"^^xsd:decimal .\nex:s3 a ex:Substance , t:acetic ; ex:mass "60"^^xsd:decimal .\nex:p1 a ex:Participant ; ex:compound t:propanol .\nex:p2 a ex:Participant ; ex:compound t:formic .\n'


def _mine(budget):
    graph = Graph().parse(data=FIXTURE, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        result = mine_with_ontology(
            miner, dataset_name="fixture", ontology_as_data=True, ontology_term_budget=budget
        )
        return (result.data_schema, miner.last_report)


def _triples(schema):
    return {(p.subject_class, p.property_uri, p.object_class): p for p in schema.patterns}


def test_terms_are_subsumed_until_the_budget_holds():
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
