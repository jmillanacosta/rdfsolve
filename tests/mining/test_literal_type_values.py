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
