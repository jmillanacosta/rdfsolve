"""rdfsolve.mining.ontology_as_data grouping: terms are grouped under their ancestors before per-class
mining when there are too many classes, with the same rows as grouping after; parents come from the
data or from hierarchy files; terms without a parent from a typing namespace are grouped by the
shape of their instances."""

import gzip

import pytest
from rdflib import Graph
from rdflib.namespace import RDFS

from rdfsolve.mining import mine_with_ontology, ontology_as_data
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.ontology_as_data import (
    choose_representatives,
    group_by_shape,
    shape_group_iri,
)
from rdfsolve.ontology.hierarchy import read_hierarchy
from tests.mining.data import EX, FIXTURE, T

LEAVES = {T + "ethanol", T + "methanol", T + "propanol", T + "acetic", T + "formic"}


def _mine(before_mining):
    with SchemaMiner.from_graph(Graph().parse(data=FIXTURE, format="turtle"), delay=0) as miner:
        result = mine_with_ontology(
            miner,
            dataset_name="fixture",
            ontology_as_data=True,
            ontology_term_budget=6,
            ontology_group_before_mining=before_mining,
        )
        batches = [c for batch in miner._class_batches or [] for c in batch]
        return result.data_schema, miner.last_report, batches


def _rows(schema):
    return {(p.subject_class, p.property_uri, p.object_class): p.count for p in schema.patterns}


def test_grouping_before_mining_gives_the_grouped_rows_of_grouping_after():
    after, _, _ = _mine(before_mining=None)
    before, report, batches = _mine(before_mining=1)
    assert _rows(before) == _rows(after), "The same grouped rows and counts"
    assert not LEAVES & set(batches), "No term is mined as its own class"
    grouping = report.config["ontology_term_grouping"]
    assert grouping["before_mining"] is True
    assert grouping["representative_members"][T + "alcohol"] == [T + "ethanol", T + "methanol"]
    assert (T + "alcohol", EX + "mass", "Literal") in _rows(before)
    lifted = next(p for p in before.patterns if p.subject_class == T + "alcohol")
    assert lifted.evidence_source == "inferred", "A grouped row is an interpretation"


def test_hierarchy_files_are_read_as_child_parent_pairs(tmp_path):
    plain, packed = tmp_path / "a.tsv", tmp_path / "b.tsv.gz"
    plain.write_text(f"# child\tparent\n{T}ethanol\t{T}alcohol\n\n{T}alcohol\t{T}alcohol\n")
    with gzip.open(packed, "wt") as out:
        out.write(f"{T}ethanol\t{T}solvent\n{T}alcohol\t{T}chemical\n")
    assert read_hierarchy([plain, packed]) == {
        T + "ethanol": {T + "alcohol", T + "solvent"},
        T + "alcohol": {T + "chemical"},
    }, "Comments, empty lines and a term as its own parent are left out"


def test_loaded_parents_group_terms_before_shape_groups(monkeypatch, tmp_path):
    monkeypatch.setattr(ontology_as_data, "NAMESPACE_GROUP_MIN_TERMS", 3)
    graph = Graph().parse(data=FIXTURE, format="turtle")
    graph.remove((None, RDFS.subClassOf, None))
    path = tmp_path / "terms.tsv"
    path.write_text(
        f"{T}ethanol\t{T}alcohol\n{T}methanol\t{T}alcohol\n{T}acetic\t{T}acid\n{T}other\t{T}acid\n"
    )
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        mine_with_ontology(
            miner,
            dataset_name="fixture",
            ontology_as_data=True,
            ontology_term_budget=1,
            ontology_group_before_mining=1,
            ontology_hierarchy_files=[path],
        )
        batches = {c for batch in miner._class_batches or [] for c in batch}
        grouping = miner.last_report.config["ontology_term_grouping"]
    assert {T + "alcohol", T + "acid"} <= batches, "The terms are mined under their loaded parents"
    assert not {T + "ethanol", T + "methanol", T + "acetic"} & batches
    assert grouping["representative_members"][T + "alcohol"] == [T + "ethanol", T + "methanol"]
    assert grouping["shape_groups"] == {}, "No namespace keeps 3 terms without a parent"
    assert grouping["hierarchy_files"] == [{"path": str(path), "pairs": 4}]
    assert grouping["terms_with_loaded_parent_by_namespace"] == {T: 3}
    owl = "http://www.w3.org/2002/07/owl#"
    assert grouping["terms_without_parent_by_namespace"] == {EX: 2, owl: 2}


def test_the_local_stage_passes_the_hierarchy_files(monkeypatch, tmp_path):
    import rdfsolve.mining
    from scripts.pipeline_stages.config import PipelineConfig
    from scripts.pipeline_stages.local import LocalMiningStage

    class StopMiningError(Exception):
        pass

    seen = {}

    def mine(miner, **options):
        seen.update(options)
        raise StopMiningError

    monkeypatch.setattr(rdfsolve.mining, "mine_with_ontology", mine)
    files = [tmp_path / "ncit.tsv.gz"]
    config = PipelineConfig(
        base_dir=tmp_path, ontology_as_data=True, ontology_hierarchy_files=files
    )
    with pytest.raises(StopMiningError):
        LocalMiningStage(config)._mine_schema(None, "pubchem", tmp_path)
    assert seen["ontology_hierarchy_files"] == files


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
    graph = Graph().parse(data=FIXTURE + '\n<urn:ex:s3> <urn:ex:charge> "-1" .\n', format="turtle")
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
    rows = {
        (p.subject_class, p.property_uri, p.object_class): p for p in result.data_schema.patterns
    }
    assert rows[group, EX + "mass", "Literal"].count == 2
    assert rows[group, EX + "mass", "Literal"].evidence_source == "inferred"


def test_objects_typed_by_shape_grouped_terms_join_their_group(monkeypatch):
    """Terms grouped before mining also group the objects they type: no ancestor can do it after
    mining for terms without a parent (lifesciencedict: MeSH terms that type the objects of
    skos:closeMatch, one row each)."""
    monkeypatch.setattr(ontology_as_data, "NAMESPACE_GROUP_MIN_TERMS", 3)
    data = FIXTURE + "\nex:r1 a <urn:other:Reaction> ; ex:product ex:s1 , ex:s2 .\n"
    graph = Graph().parse(data=data, format="turtle")
    graph.remove((None, RDFS.subClassOf, None))
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        result = mine_with_ontology(
            miner,
            dataset_name="fixture",
            ontology_as_data=True,
            ontology_term_budget=1,
            ontology_group_before_mining=1,
        )
        report = miner.last_report
    group = shape_group_iri({EX + "mass"})
    rows = {
        p.object_class: p for p in result.data_schema.patterns if p.property_uri == EX + "product"
    }
    assert set(rows) == {EX + "Substance", group}, "No grouped term stays an object class"
    assert rows[group].subject_class == "urn:other:Reaction"
    assert rows[group].count == 2 and rows[group].count_semantics == "upper_bound"
    assert rows[group].evidence_source == "inferred"
    grouped = report.config["ontology_term_subsumption"]["object_classes_grouped_before_mining"]
    assert grouped["after"] < grouped["before"]
