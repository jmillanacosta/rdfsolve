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


def census(monkeypatch, refuse):
    """Mine DATA with the per-profile census; with *refuse*, the whole census is refused."""
    # The local helper is taken for a remote one, so that the per-profile census runs.
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        select, paged = miner.helper.select, miner.helper.select_with_fallback
        refused = []

        def limited(query, *args, purpose="", **kwargs):
            whole = "?s ?p ?o ." in query.split("BIND")[0]
            if refuse and purpose == "structural/coverage" and whole:
                refused.append(query)
                raise EndpointTimeoutError("Query cost/time limit")
            return select(query, *args, purpose=purpose, **kwargs)

        def not_paged(query, *args, purpose="", **kwargs):
            # Pages of an aggregate of a few groups cost as much as the whole query; the
            # paged form also fails to compile on Virtuoso (SQ156).
            assert purpose != "structural/coverage", "A census is sent as one query"
            return paged(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", limited)
        monkeypatch.setattr(miner.helper, "select_with_fallback", not_paged)
        miner.mine("census")
        (entry,) = miner.last_report.config["structural_coverage"]
    return entry, refused


def test_a_refused_census_is_counted_by_property(monkeypatch):
    entry, refused = census(monkeypatch, refuse=True)
    whole, _ = census(monkeypatch, refuse=False)
    assert refused, "The whole census was tried first"
    assert (entry["census"], whole["census"]) == ("per_property", "whole_graph")
    assert entry["covered_triples"] == whole["covered_triples"], "Both censuses count the same"
    assert entry["triple_count"] == 5, "The per-property counts add up to every triple"
    assert entry["untyped_subject_triples"] == 1
    assert entry["covered_triples"] + entry["uncovered_triples"] == 5


def test_the_coverage_test_does_not_rebind_outer_variables():
    """Virtuoso rejects VALUES that bind an outer variable inside EXISTS (SP031), and IF around
    EXISTS (SQ156)."""
    import re

    from rdfsolve.mining.typed_coverage import typed_match

    match = typed_match([("urn:A", "urn:p", "urn:B", None)], None, None)
    headers = re.findall(r"VALUES \(([^)]*)\)", match)
    assert headers and not any({"?s", "?p", "?o"} & set(h.split()) for h in headers), (
        "Outer variables are compared, not bound"
    )
    assert "IF(EXISTS" not in match.replace(" ", "")
    body = match.split("EXISTS {", 1)[1]
    assert body.lstrip().startswith("?s ?p ?o ."), "Engines that join EXISTS need the pattern"


def test_the_coverage_test_of_one_property_reads_only_that_property():
    """QLever evaluates the group of EXISTS on its own: a constant property keeps it small
    (Bgee RO_0002162: 20 s, not 217 s, with the same counts)."""
    from rdfsolve.mining.typed_coverage import typed_match

    match = typed_match([("urn:A", "urn:p", "urn:B", None)], None, None, predicate="urn:p")
    body = match.split("EXISTS {", 1)[1]
    assert body.lstrip().startswith("?s <urn:p> ?o ."), "The group reads one property"
    assert "?p " not in body and "?p)" not in body, "No outer variable is bound again (SP031)"
