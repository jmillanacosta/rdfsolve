"""The queries of the dataset statistics and of the class relations are recorded in the mining
report with the other queries, so that the numbers of queries sent and failed include them."""

from rdflib import Graph

from rdfsolve import SchemaMiner

DATA = '<urn:a> a <urn:A>, <urn:B> ; <urn:p> "x" .  <urn:c> a <urn:C> ; <urn:q> <urn:a> .'


def test_statistics_and_class_relation_queries_are_recorded(monkeypatch):
    with SchemaMiner.from_graph(Graph().parse(data=DATA, format="turtle"), delay=0) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        miner.mine("recorded")
        stats = miner.last_report.query_stats
    assert stats["dataset-statistics"].sent >= 3 and stats["dataset-statistics"].failed == 0
    assert stats["class-extensions"].sent == 3, "One query for each class"
