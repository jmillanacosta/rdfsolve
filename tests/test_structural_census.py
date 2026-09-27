"""A census that is too large for the endpoint is counted again one property at a time."""

from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointTimeoutError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> ; <urn:q> "x" .
<urn:c> <urn:p> <urn:b> .
"""


def test_a_refused_census_is_counted_by_property(monkeypatch):
    # The local helper is taken for a remote one, so that the per-profile census runs.
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        select = miner.helper.select_with_fallback
        refused = []

        def limited(query, *args, purpose="", **kwargs):
            if purpose == "structural/coverage" and "?s ?p ?o ." in query.split("BIND")[0]:
                refused.append(query)
                raise EndpointTimeoutError("Query cost/time limit")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select_with_fallback", limited)
        miner.mine("census")
        report = miner.last_report
    (entry,) = report.config["structural_coverage"]
    assert refused, "The whole census was tried first"
    assert entry["census"] == "per_property"
    assert entry["triple_count"] == 5, "The per-property counts add up to every triple"
    assert entry["untyped_subject_triples"] == 1
    assert entry["covered_triples"] + entry["uncovered_triples"] == 5
