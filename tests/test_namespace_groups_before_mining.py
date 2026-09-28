"""Before mining, terms that no ancestor can take are mined as one group per ontology namespace,
and the report records the groups and the terms without a parent in each namespace."""

from rdflib import Graph
from rdflib.namespace import RDFS

from rdfsolve.config import mint
from rdfsolve.mining import mine_with_ontology, ontology_as_data
from rdfsolve.mining.miner import SchemaMiner
from tests.test_ontology_term_subsumption import EX, FIXTURE, T


def test_parentless_terms_are_mined_as_namespace_groups(monkeypatch):
    monkeypatch.setattr(ontology_as_data, "NAMESPACE_GROUP_MIN_TERMS", 3)
    graph = Graph().parse(data=FIXTURE, format="turtle")
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
    group = mint("term-group", T)
    assert group in batches and EX + "Substance" in batches
    assert not {T + "ethanol", T + "methanol", T + "acetic"} & batches
    grouping = report.config["ontology_term_grouping"]
    assert grouping["namespace_groups"] == {group: {"namespace": T, "terms": 3}}
    assert grouping["namespace_group_min_terms"] == 3
    owl = "http://www.w3.org/2002/07/owl#"  # owl:Class and owl:Restriction type the fixture terms
    assert grouping["terms_without_parent_by_namespace"] == {T: 3, EX: 2, owl: 2}
    rows = {(p.subject_class, p.property_uri, p.object_class): p for p in result.data_schema.patterns}
    assert rows[group, EX + "mass", "Literal"].count == 3
    assert rows[group, EX + "mass", "Literal"].evidence_source == "inferred"
