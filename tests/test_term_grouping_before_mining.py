"""Terms are grouped under their ancestors before per-class mining when there are too many
classes to mine one by one (PubChem: about 181,000 ChEBI types). The grouped rows and counts
are the same as when the rows of each term are mined first and grouped after."""

from rdflib import Graph

from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.miner import SchemaMiner
from tests.test_ontology_term_subsumption import EX, FIXTURE, T

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
