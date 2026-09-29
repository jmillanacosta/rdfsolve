"""A class batch reused from an earlier run is also checked for a class with the same members,
so that the classes are recorded as one extension and their counts are read once (Bgee reused
every batch: orth:Gene, orth:SequenceUnit, orth:GeneTreeNode, SO_0000704 and CDAO_0000140 have
the same 1,559,014 members and were not recorded). RDFLib stands in for QLever here."""

from rdflib import Graph

from rdfsolve import SchemaMiner

DATA = """
<urn:a1> a <urn:A>, <urn:B> ; <urn:p> "x" .  <urn:a2> a <urn:A>, <urn:B> ; <urn:p> "y" .
<urn:c1> a <urn:C> ; <urn:q> <urn:a1> .
"""


def mine(tmp_path, monkeypatch, name, resume=None):
    path = tmp_path / f"{name}.json"
    graph = Graph().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0, class_batch_size=1, report_path=path, resume_checkpoint=resume) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        schema = miner.mine("same")
        report = miner.last_report
    return schema, report, path.with_suffix(".checkpoint.jsonl")


def test_reused_batches_record_classes_with_the_same_members(tmp_path, monkeypatch):
    fresh, report, checkpoint = mine(tmp_path, monkeypatch, "fresh")
    shared = report.config["shared_extensions"]
    assert sorted([*shared.items()][0]) == ["urn:A", "urn:B"]
    kept = tmp_path / "kept.checkpoint.jsonl"
    kept.write_text(checkpoint.read_text())
    resumed, report, _ = mine(tmp_path, monkeypatch, "resumed", resume=kept)
    assert len(report.config["resumed_batches"]) == 3, "Every batch is reused"
    assert report.config.get("shared_extensions") == shared
    assert sorted(map(repr, resumed.patterns)) == sorted(map(repr, fresh.patterns))
