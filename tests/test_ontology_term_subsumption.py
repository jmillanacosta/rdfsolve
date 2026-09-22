"""Ontology terms used as data are probed per term, then subsumed within a class budget."""

from rdflib import Graph

from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.ontology_as_data import (
    choose_representatives,
    pattern_classes,
    subsume_patterns,
)
from rdfsolve.mining.two_phase_strategy import plan_class_batches
from rdfsolve.schema_models import SchemaPattern

T = "urn:term:"
EX = "urn:ex:"

# A dataset that types substances with terms, points participants at terms and
# describes one term with a data property, next to a loaded ontology whose
# unused terms carry annotations and OWL restrictions.
FIXTURE = """
@prefix ex: <urn:ex:> .
@prefix t: <urn:term:> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix oio: <http://www.geneontology.org/formats/oboInOwl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

t:chemical a owl:Class .
t:alcohol a owl:Class ; rdfs:subClassOf t:chemical .
t:acid a owl:Class ; rdfs:subClassOf t:chemical .
t:ethanol a owl:Class ; rdfs:subClassOf t:alcohol ; oio:hasDbXref "x:1" ; ex:smiles "CCO" .
t:methanol a owl:Class ; rdfs:subClassOf t:alcohol .
t:propanol a owl:Class ; rdfs:subClassOf t:alcohol .
t:acetic a owl:Class ; rdfs:subClassOf t:acid .
t:formic a owl:Class ; rdfs:subClassOf t:acid .
t:unused1 a owl:Class ; rdfs:subClassOf t:acid ; oio:hasDbXref "x:2" ;
    rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:role ; owl:someValuesFrom t:acid ] .
t:unused2 a owl:Class ; rdfs:subClassOf t:alcohol ; oio:hasDbXref "x:3" .

ex:s1 a ex:Substance , t:ethanol ; ex:mass "46"^^xsd:decimal .
ex:s2 a ex:Substance , t:methanol ; ex:mass "32"^^xsd:decimal .
ex:s3 a ex:Substance , t:acetic ; ex:mass "60"^^xsd:decimal .
ex:p1 a ex:Participant ; ex:compound t:propanol .
ex:p2 a ex:Participant ; ex:compound t:formic .
"""


def _mine(budget):
    graph = Graph().parse(data=FIXTURE, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        result = mine_with_ontology(
            miner, dataset_name="fixture", ontology_as_data=True, ontology_term_budget=budget
        )
        return result.data_schema, miner.last_report


def _triples(schema):
    return {(p.subject_class, p.property_uri, p.object_class): p for p in schema.patterns}


def test_terms_are_subsumed_until_the_budget_holds():
    schema, report = _mine(budget=5)
    triples = _triples(schema)
    classes = pattern_classes(schema.patterns)
    assert len(classes) <= 5
    assert {EX + "Substance", EX + "Participant", T + "alcohol", T + "acid"} <= classes
    assert not classes & {T + "ethanol", T + "methanol", T + "propanol", T + "acetic", T + "formic"}

    lifted = triples[(T + "alcohol", EX + "mass", "Literal")]
    assert lifted.evidence_source == "inferred"
    assert lifted.count == 2  # ethanol and methanol substances
    assert (EX + "Participant", EX + "compound", T + "alcohol") in triples
    assert (EX + "Participant", EX + "compound", T + "acid") in triples
    assert (T + "alcohol", EX + "smiles", "Literal") in triples
    assert triples[(EX + "Substance", EX + "mass", "Literal")].evidence_source == "mined"

    summary = report.config["ontology_term_subsumption"]
    assert summary["subsumed"] is True
    assert summary["representatives"] == {T + "acid": 2, T + "alcohol": 3}
    assert "+ontology-as-data" in schema.about.strategy


def test_loaded_but_unused_ontology_never_becomes_patterns():
    schema, _ = _mine(budget=5)
    for pattern in schema.patterns:
        assert "unused" not in pattern.subject_class + pattern.object_class
        assert "oboInOwl" not in pattern.property_uri
        assert pattern.property_uri != "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        assert "Restriction" not in pattern.subject_class


def test_every_term_is_kept_when_the_schema_fits_the_budget():
    schema, report = _mine(budget=300)
    classes = pattern_classes(schema.patterns)
    assert {T + "ethanol", T + "propanol", T + "formic"} <= classes
    assert report.config["ontology_term_subsumption"]["subsumed"] is False
    assert all(p.evidence_source == "mined" for p in schema.patterns)


def test_representatives_lift_deepest_first_and_respect_fixed_classes():
    parents = {
        "a1": {"a"},
        "a2": {"a"},
        "b1": {"b"},
        "a": {"root"},
        "b": {"root"},
        "own": {"root"},
    }
    chosen = choose_representatives(["a1", "a2", "b1", "own"], parents, budget=3, fixed=["own"])
    assert chosen.representative == {"a1": "a", "a2": "a", "b1": "b"}
    assert chosen.classes_after == 3
    assert chosen.levels_lifted == 1


def test_parent_choice_follows_the_level_majority_and_is_deterministic():
    parents = {"x": {"p", "q"}, "y": {"q"}, "z": {"q"}, "p": set(), "q": set()}
    chosen = choose_representatives(["x", "y", "z"], parents, budget=1)
    assert set(chosen.representative.values()) == {"q"}


def test_classes_without_parents_are_reported_over_budget():
    chosen = choose_representatives(["a", "b", "c"], {}, budget=2)
    assert chosen.over_budget is True
    assert chosen.classes_after == 3


def test_cycles_in_the_hierarchy_terminate():
    parents = {"a": {"b"}, "b": {"a"}, "c": {"a"}}
    chosen = choose_representatives(["a", "b", "c"], parents, budget=1)
    assert chosen.classes_after <= 3


def test_subsumed_patterns_merge_counts_and_graphs():
    patterns = [
        SchemaPattern(
            subject_class="urn:t1",
            property_uri="urn:p",
            object_class="Literal",
            count=2,
            graphs={"g": 2},
        ),
        SchemaPattern(
            subject_class="urn:t2",
            property_uri="urn:p",
            object_class="Literal",
            count=3,
            graphs={"g": 3},
        ),
        SchemaPattern(
            subject_class="urn:own", property_uri="urn:p", object_class="Literal", count=1
        ),
    ]
    merged = {
        p.subject_class: p
        for p in subsume_patterns(patterns, {"urn:t1": "urn:T", "urn:t2": "urn:T"})
    }
    assert merged["urn:T"].count == 5
    assert merged["urn:T"].graphs == {"g": 5}
    assert merged["urn:T"].evidence_source == "inferred"
    assert merged["urn:T"].distinct_subjects is None
    assert merged["urn:own"].evidence_source == "mined"


def test_batches_pack_light_classes_and_isolate_heavy_ones():
    classes = ["heavy"] + [f"t{i}" for i in range(1200)]
    weights = {"heavy": 5_000_000, **{f"t{i}": 10 for i in range(1200)}}
    batches = plan_class_batches(classes, weights, max_classes=500, max_instances=1_000_000)
    assert batches[0] == ["heavy"]
    assert [len(b) for b in batches[1:]] == [500, 500, 200]
    assert sorted(c for b in batches for c in b) == sorted(classes)


def test_cycle_with_an_unrelated_class_stops_above_budget():
    chosen = choose_representatives(["a", "b", "z"], {"a": {"b"}, "b": {"a"}}, budget=1)
    assert chosen.over_budget is True
    assert chosen.classes_after == 2
    assert chosen.representative["z"] == "z"
