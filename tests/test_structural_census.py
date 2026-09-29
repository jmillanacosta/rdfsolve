"""Through an endpoint, the census and the discovery of uncovered edges are sent one property at
a time with the one-property test. Virtuoso evaluates the whole-graph test, with ?p a variable,
wrongly and without an error (AOP-Wiki: the whole-graph discovery gave 9 rows for 8 properties,
the discovery of virtrdf:item alone 41), and a whole-graph test of a large graph joins every
triple with every typed profile (Bgee: no answer in 2 h)."""

from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> ; <urn:q> "x" .
<urn:c> <urn:p> <urn:b> .
"""


def census(monkeypatch, *, endpoint=True):
    """Mine DATA; with *endpoint*, the local helper is taken for an endpoint helper."""
    with monkeypatch.context() as patch, SchemaMiner.from_graph(
        Dataset().parse(data=DATA, format="turtle"), delay=0
    ) as miner:
        if endpoint:
            patch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
        select, paged = miner.helper.select, miner.helper.select_with_fallback
        sent = []

        def recorded(query, *args, purpose="", **kwargs):
            sent.append((purpose, query))
            return select(query, *args, purpose=purpose, **kwargs)

        def recorded_pages(query, *args, purpose="", **kwargs):
            # Pages of an aggregate cost as much as the whole query; the paged form also fails
            # to compile on Virtuoso (SQ156).
            assert purpose != "structural/coverage", "A census is sent as one query"
            sent.append((purpose, query))
            return paged(query, *args, purpose=purpose, **kwargs)

        patch.setattr(miner.helper, "select", recorded)
        patch.setattr(miner.helper, "select_with_fallback", recorded_pages)
        result = miner.mine("census")
        (entry,) = miner.last_report.config["structural_coverage"]
    return entry, sent, result


def test_an_endpoint_census_is_counted_by_property(monkeypatch):
    entry, sent, result = census(monkeypatch)
    local, _, local_result = census(monkeypatch, endpoint=False)
    census_queries = [q for purpose, q in sent if purpose == "structural/coverage"]
    assert census_queries and not any("?s ?p ?o ." in q for q in census_queries)
    assert entry["census"] == "per_property" and local["census"] == "whole_graph"
    for count in ("triple_count", "covered_triples", "uncovered_triples", "untyped_subject_triples"):
        assert entry[count] == local[count], f"{count}: the same as the local census"
    assert entry["census_properties"]["urn:p"] == {
        "triples": 2, "untypedTriples": 1, "uncoveredTriples": 1
    }, "The counts of each property are recorded"
    assert sorted(p.model_dump_json() for p in result.structural_patterns) == sorted(
        p.model_dump_json() for p in local_result.structural_patterns
    ), "The same structural patterns as the local discovery"


def test_uncovered_edges_are_discovered_by_property(monkeypatch):
    entry, sent, _ = census(monkeypatch)
    discovery = {q for purpose, q in sent if purpose == "structural/discovery"}
    uncovered = {prop for prop, n in entry["census_properties"].items() if n["uncoveredTriples"]}
    assert uncovered and len(discovery) == len(uncovered), "One query for each property with uncovered edges"
    assert {q.split("VALUES ?p { <", 1)[1].split(">", 1)[0] for q in discovery} == uncovered


def test_the_coverage_test_does_not_rebind_outer_variables():
    """Virtuoso rejects VALUES that bind an outer variable inside EXISTS (SP031), and IF around
    EXISTS (SQ156)."""
    from rdfsolve.mining.typed_coverage import typed_match

    match = typed_match([("urn:A", "urn:p", "urn:B", None)], None, None)
    assert "VALUES" not in match and "?p = <urn:p>" in match, "The property is compared"
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
