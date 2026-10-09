"""A literal object of rdf:type names no class: mining skips it and reports it, not as an error."""

import pytest
from rdflib import Dataset, URIRef

from rdfsolve.mining.miner import SchemaMiner

DATA = """
@prefix ex: <https://example.org/> .
ex:a1 a ex:Association; ex:subject ex:g1; ex:object ex:d1 .
ex:g1 a ex:Genotype, "genotype"; ex:name "G1" .
ex:d1 a ex:Disease .
"""


@pytest.mark.parametrize("strategy", ["two-phase", "single-pass", "one-shot"])
def test_a_literal_type_value_is_skipped_and_reported(strategy):
    data = Dataset()
    data.graph(URIRef("urn:data")).parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(data, graph_uris=["urn:data"], strategy=strategy, delay=0) as miner:
        schema = miner.mine()
        report = miner.last_report
    triples = {(p.subject_class, p.property_uri, p.object_class) for p in schema.patterns}
    assert (
        "https://example.org/Association",
        "https://example.org/subject",
        "https://example.org/Genotype",
    ) in triples
    assert all(o != "genotype" for _, _, o in triples)
    assert report.dropped_invalid_uris == 0
    assert report.config["literal_type_values"]["count"] >= 1
    assert report.completion_state == "complete"


def test_the_scan_mines_a_subject_typed_by_a_literal_as_untyped(tmp_path):
    """monarch-kg (job 115614): the scan took every rdf:type value as a class, and the pattern
    model refused the class "strain", which failed the source. A literal type value is not a
    class: its subjects are mined as untyped subjects, the values are reported, and the source
    is mined."""
    from rdfsolve.mining.scan import ScanStrategy, store_from_graph

    data = Dataset()
    data.graph(URIRef("urn:data")).parse(
        data=DATA + 'ex:s1 a "strain"; ex:name "S1" .\n', format="turtle"
    )
    store = store_from_graph(data, tmp_path / "store")
    with SchemaMiner.from_graph(data, delay=0, strategy=ScanStrategy(store=store)) as miner:
        schema = miner.mine()
        report = miner.last_report
    classes = {p.subject_class for p in schema.patterns} | {p.object_class for p in schema.patterns}
    assert not any("genotype" in c or "strain" in c for c in classes)
    assert "https://example.org/Genotype" in classes, "A subject keeps its IRI class"
    untyped = {(p.property_uri, p.object_class) for p in schema.patterns if p.untyped_subject}
    assert ("https://example.org/name", "Literal") in untyped, "ex:s1 is an untyped subject"
    found = report.config["literal_type_values"]
    assert found["count"] == 2 and found["values"] == 2
    assert {s["value"] for s in found["samples"]} == {'"genotype"', '"strain"'}
    assert report.completion_state == "complete"
