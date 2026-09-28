"""A property whose census the endpoint refuses is counted in batches of its objects."""

import re

from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointTimeoutError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b>, <urn:d>, [ a <urn:B> ] .
<urn:b> a <urn:B> ; <urn:q> "x" .
<urn:c> <urn:p> <urn:b> .
<urn:d> a <urn:B> .
"""


def census(monkeypatch, refuse):
    """Mine DATA with the per-profile census; *refuse* decides which census queries fail."""
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        select = miner.helper.select

        def limited(query, *args, purpose="", **kwargs):
            if purpose == "structural/coverage" and refuse(query):
                raise EndpointTimeoutError("Tried to allocate 455.7 GB")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", limited)
        miner.mine("census")
        (entry,) = miner.last_report.config["structural_coverage"]
    return entry


def too_large(query):
    """Refuse the whole graph, the whole of urn:p, and each batch of more than one object."""
    head = query.split("BIND")[0]
    batch = re.search(r"VALUES \?o \{([^}]*)\}", head)
    if "?s ?p ?o ." in head:
        return True
    if "<urn:p>" in head and "isBlank" not in head:
        return batch is None or len(batch.group(1).split()) > 1
    return False


def test_a_refused_property_is_counted_in_object_batches(monkeypatch, caplog):
    monkeypatch.setattr(structural_strategy, "CENSUS_BATCH_TRIPLES", 2)
    with caplog.at_level("INFO", logger="rdfsolve.mining.structural_strategy"):
        batched = census(monkeypatch, too_large)
    log = caplog.text
    assert "counting 3 properties one at a time" in log, "The fallback to properties is logged"
    assert "property 2/3 urn:p" in log and "urn:p refused; counting 2 objects in 2 batches" in log
    monkeypatch.setattr(structural_strategy, "CENSUS_BATCH_TRIPLES", 10)
    caplog.clear()
    with caplog.at_level("INFO", logger="rdfsolve.mining.structural_strategy"):
        split = census(monkeypatch, too_large)
    assert "urn:p batch of 2 objects refused; split in two" in caplog.text, "Each split is logged"
    assert split["census_batches"] == {"urn:p": 3}, "A refused batch is split until it is counted"
    whole = census(monkeypatch, lambda query: False)
    assert batched["census_batches"] == {"urn:p": 3}, "Two single objects and the blank nodes"
    for count in ("triple_count", "covered_triples", "uncovered_triples", "untyped_subject_triples"):
        assert batched[count] == whole[count], f"{count}: the batches add up to the whole census"


def test_the_typed_test_reads_only_the_edges_of_a_batch():
    """The typed test repeats the edge and the batch, so that QLever reads the types of the
    subjects of the batch only (Bgee RO_0002206: 455.7 GB for every batch without it)."""
    query = structural_strategy._census_query(
        None, [], "false", False, "urn:p", "VALUES ?o { <urn:b> }"
    )
    typed = query.split("BIND(EXISTS {")[1].split("} AS ?typed)")[0]
    assert "?s <urn:p> ?o ." in typed and "VALUES ?o { <urn:b> }" in typed
    whole = structural_strategy._census_query(None, [], "false", False)
    assert "BIND(EXISTS { ?s a ?_type . } AS ?typed)" in whole, "The whole graph is unchanged"

