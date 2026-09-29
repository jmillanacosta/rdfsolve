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
    monkeypatch.setattr(structural_strategy, "CENSUS_MIN_TRIPLES_PER_OBJECT", 1)
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
    edge = query.split("WHERE {", 1)[1].lstrip()
    if edge.startswith("?s ?p ?o ."):
        return True
    if edge.startswith("?s <urn:p> ?o . FILTER(isBlank(?o))"):
        return False
    if edge.startswith("?s <urn:p> ?o ."):
        batch = re.match(r"\?s <urn:p> \?o \. VALUES \?o \{([^}]*)\}", edge)
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
    queries = structural_strategy._census_queries(
        None, [], "false", False, "urn:p", "VALUES ?o { <urn:b> }"
    )
    untyped = next(q for q in queries if "?untypedTriples" in q)
    typed = untyped.split("FILTER NOT EXISTS {")[1]
    assert "?s <urn:p> ?o ." in typed and "VALUES ?o { <urn:b> }" in typed
    whole = next(
        q for q in structural_strategy._census_queries(None, [], "false", False)
        if "?untypedTriples" in q
    )
    assert "FILTER NOT EXISTS { ?s a ?_type . }" in whole, "The whole graph reads all types"


def test_the_census_counts_with_filters():
    """Virtuoso gives wrong counts for a group by BIND(EXISTS ...) (AOP-Wiki prov:used: 1 of 2).
    Uncovered edges are counted with FILTER(!test), the filter of the discovery: Rhea counts
    550,753 covered rdf:type edges of 550,634 with FILTER(test), and 0 with FILTER(!test)."""
    match = "(EXISTS { ?s a ?_t } || EXISTS { ?o a ?_t })"
    for scope in ((), ("urn:p", "VALUES ?o { <urn:b> }")):
        queries = structural_strategy._census_queries(None, [], match, False, *scope)
        assert len(queries) == 3, "All triples, triples of untyped subjects, covered triples"
        assert not any("BIND" in q or "GROUP BY" in q for q in queries)
        assert any(f"FILTER(!{match})" in q for q in queries)
        assert not any(f"FILTER({match})" in q for q in queries), "No covered count"
    local = structural_strategy._census_queries(None, [], match, True)
    assert len(local) == 2, "A local census counts no coverage; discovery finds the rest"
    untested = structural_strategy._census_queries(None, [], "false", False, "urn:p")
    assert not any("FILTER(!false)" in q or "FILTER(false)" in q for q in untested), (
        "Without typed profiles every edge is uncovered; Rhea answers FILTER(false) with no row")
