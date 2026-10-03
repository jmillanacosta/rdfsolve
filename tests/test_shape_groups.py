"""Ontology terms without a parent, from a namespace used for typing, are grouped by the shape of
their instances (the owner decision of 2026-09-30 for PubChem, gate 4): terms whose instances have
the same set of properties (rdf:type left out) form one group, named by a hash of that set, and a term with a shape of
its own stays its own class. The report records each group with its properties, member terms,
instance count and an example instance, so that a group can be named later."""

from rdflib import Graph
from rdflib.namespace import RDFS

from rdfsolve.mining import mine_with_ontology, ontology_as_data
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.ontology_as_data import (
    choose_representatives,
    group_by_shape,
    shape_group_iri,
)
from tests.test_ontology_term_subsumption import EX, FIXTURE, T

OBO = "http://purl.obolibrary.org/obo/"


def test_parentless_terms_are_grouped_by_shape():
    data = "http://example.org/vocab#Compound"
    a, b, c, d = (OBO + f"PR_{n}" for n in "ABCD")
    terms = [a, b, c, d, OBO + "CHEBI_1", data]
    parents = {OBO + "CHEBI_1": {OBO + "CHEBI_0"}}
    shapes = {a: {"p", "q"}, b: {"q", "p"}, c: {"p"}, d: set(), data: {"p", "q"}}
    chosen = choose_representatives(terms, parents, budget=1)
    groups = group_by_shape(chosen, parents, shapes, min_terms=4)
    pq = shape_group_iri({"p", "q"})
    assert groups == {pq: frozenset({"p", "q"})}
    assert chosen.representative[a] == chosen.representative[b] == pq
    assert chosen.representative[c] == c, "A shape of one term stays its own class"
    assert chosen.representative[d] == d
    assert chosen.representative[data] == data, "A namespace with fewer terms is not grouped"
    assert chosen.representative[OBO + "CHEBI_1"] == OBO + "CHEBI_0"
    assert chosen.classes_after == 5
    assert shape_group_iri({"q", "p"}) == pq and shape_group_iri({"p"}) != pq


def test_parentless_terms_are_mined_as_shape_groups(monkeypatch):
    monkeypatch.setattr(ontology_as_data, "NAMESPACE_GROUP_MIN_TERMS", 3)
    graph = Graph().parse(
        data=FIXTURE + '\n<urn:ex:s3> <urn:ex:charge> "-1" .\n', format="turtle"
    )
    graph.remove((None, RDFS.subClassOf, None))
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        result = mine_with_ontology(
            miner,
            dataset_name="fixture",
            ontology_as_data=True,
            ontology_term_budget=1,
            ontology_group_before_mining=1,
        )
        batches = {c for batch in miner._class_batches or [] for c in batch}
        report = miner.last_report
    group = shape_group_iri({EX + "mass"})
    assert group in batches and T + "acetic" in batches and EX + "Substance" in batches
    assert not {T + "ethanol", T + "methanol"} & batches
    grouping = report.config["ontology_term_grouping"]
    assert grouping["grouping_of_terms_without_parent"] == "shape"
    assert "namespace_groups" not in grouping
    assert grouping["shape_groups"] == {
        group: {
            "properties": [EX + "mass"],
            "terms": 2,
            "namespaces": {T: 2},
            "instances": 2,
            "example_instance": grouping["shape_groups"][group]["example_instance"],
        }
    }
    assert grouping["shape_groups"][group]["example_instance"] in {EX + "s1", EX + "s2"}
    assert grouping["representative_members"][group] == [T + "ethanol", T + "methanol"]
    assert grouping["terms_in_shapes_of_one_term"] == 1
    rows = {(p.subject_class, p.property_uri, p.object_class): p for p in result.data_schema.patterns}
    assert rows[group, EX + "mass", "Literal"].count == 2
    assert rows[group, EX + "mass", "Literal"].evidence_source == "inferred"
