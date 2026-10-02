"""Before mining, terms without a parent in the data take their parents from hierarchy files
(PubChem types proteins and findings with PR and NCIt terms, without their hierarchy), before
the terms that are left are grouped by namespace."""

import gzip

import pytest
from rdflib import Graph
from rdflib.namespace import RDFS

from rdfsolve.mining import mine_with_ontology, ontology_as_data
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.ontology.hierarchy import read_hierarchy
from tests.test_ontology_term_subsumption import EX, FIXTURE, T


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

    class Stop(Exception):
        pass

    seen = {}

    def mine(miner, **options):
        seen.update(options)
        raise Stop

    monkeypatch.setattr(rdfsolve.mining, "mine_with_ontology", mine)
    files = [tmp_path / "ncit.tsv.gz"]
    config = PipelineConfig(base_dir=tmp_path, ontology_as_data=True, ontology_hierarchy_files=files)
    with pytest.raises(Stop):
        LocalMiningStage(config)._mine_schema(None, "pubchem", tmp_path)
    assert seen["ontology_hierarchy_files"] == files
