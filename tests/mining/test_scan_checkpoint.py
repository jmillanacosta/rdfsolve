"""rdfsolve.mining.scan_checkpoint: a retried scan run of the same work reads the counted patterns
and the structural census instead of computing them again, and gets the same schema."""

from __future__ import annotations

from rdflib import Dataset, URIRef

from rdfsolve.mining.miner import SchemaMiner

DATA = """
@prefix ex: <https://example.org/> .
ex:a1 a ex:Association; ex:subject ex:g1; ex:object ex:d1; ex:note "n" .
ex:g1 a ex:Gene; ex:name "G1" .
ex:d1 a ex:Disease .
ex:u1 ex:name "untyped" ; ex:link ex:x1 .
"""


def _mine(store):
    from rdfsolve.mining.scan import ScanStrategy

    data = Dataset()
    data.graph(URIRef("urn:data")).parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(data, delay=0, strategy=ScanStrategy(store=store)) as miner:
        schema = miner.mine()
        return schema, miner.last_report


def test_a_retried_run_reads_the_checkpoints_and_gives_the_same_schema(tmp_path, monkeypatch):
    from rdfsolve.mining import scan, scan_structure
    from rdfsolve.mining.scan import store_from_graph

    data = Dataset()
    data.graph(URIRef("urn:data")).parse(data=DATA, format="turtle")
    store = store_from_graph(data, tmp_path / "store")
    first, report = _mine(store)
    assert sorted((tmp_path / "store" / "checkpoints").glob("*.json"))
    assert "scan_checkpoint" not in report.config

    def refuse(*args, **kwargs):
        raise AssertionError("a checkpointed phase ran again")

    monkeypatch.setattr(scan, "count_patterns", refuse)
    monkeypatch.setattr(scan_structure, "structural_coverage", refuse)
    second, report = _mine(store)
    assert report.config["scan_checkpoint"] == {
        "scan-patterns": "read",
        "structural-patterns": "read",
    }
    dump = sorted(p.model_dump_json() for p in first.patterns)
    assert sorted(p.model_dump_json() for p in second.patterns) == dump
    assert [p.model_dump() for p in second.structural_patterns or []] == [
        p.model_dump() for p in first.structural_patterns or []
    ]


def test_checkpoints_are_off_when_the_environment_says_so(tmp_path, monkeypatch):
    from rdfsolve.mining.scan import store_from_graph

    monkeypatch.setenv("RDFSOLVE_SCAN_CHECKPOINTS", "0")
    data = Dataset()
    data.graph(URIRef("urn:data")).parse(data=DATA, format="turtle")
    store = store_from_graph(data, tmp_path / "store")
    _mine(store)
    assert not (tmp_path / "store" / "checkpoints").exists()
