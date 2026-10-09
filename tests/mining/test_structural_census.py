"""rdfsolve.mining.structural_strategy census: through an endpoint the census and the discovery of
uncovered edges are sent one property at a time; a refused property is counted in batches of its
objects, split until counted, or recorded as not checked; counts are kept in the checkpoint and
taken again on resume."""

import re
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointTimeoutError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> ; <urn:q> "x" .
<urn:c> <urn:p> <urn:b> .
"""


COUNTS = ("triple_count", "covered_triples", "uncovered_triples", "untyped_subject_triples")


@pytest.fixture(autouse=True)
def one_property_at_a_time(monkeypatch):
    """These tests check the census of one property, sent alone; test_structural_census_batched
    checks that several properties in one query give the same counts."""
    monkeypatch.setattr(structural_strategy, "CENSUS_PROPERTIES_PER_QUERY", 1)


def census(monkeypatch, *, endpoint=True):
    """Mine DATA; with *endpoint*, the local helper is taken for an endpoint helper."""
    with (
        monkeypatch.context() as patch,
        SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner,
    ):
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
    for count in COUNTS:
        assert entry[count] == local[count], f"{count}: the same as the local census"
    assert entry["census_properties"]["urn:p"] == {
        "triples": 2,
        "untypedTriples": 1,
        "uncoveredTriples": 1,
    }, "The counts of each property are recorded"
    assert sorted(p.model_dump_json() for p in result.structural_patterns) == sorted(
        p.model_dump_json() for p in local_result.structural_patterns
    ), "The same structural patterns as the local discovery"


def test_uncovered_edges_are_discovered_by_property(monkeypatch):
    entry, sent, _ = census(monkeypatch)
    discovery = {q for purpose, q in sent if purpose == "structural/discovery"}
    uncovered = {prop for prop, n in entry["census_properties"].items() if n["uncoveredTriples"]}
    assert uncovered and len(discovery) == len(uncovered), (
        "One query for each property with uncovered edges"
    )
    assert {q.split("VALUES ?p { <", 1)[1].split(">", 1)[0] for q in discovery} == uncovered


BATCH_DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b>, <urn:d>, [ a <urn:B> ] .
<urn:b> a <urn:B> ; <urn:q> "x" .
<urn:c> <urn:p> <urn:b> .
<urn:d> a <urn:B> .
"""


def batched_census(monkeypatch, refuse):
    """Mine BATCH_DATA with the per-profile census; *refuse* decides which census queries fail."""
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    monkeypatch.setattr(structural_strategy, "CENSUS_MIN_TRIPLES_PER_OBJECT", 1)
    with SchemaMiner.from_graph(
        Dataset().parse(data=BATCH_DATA, format="turtle"), delay=0
    ) as miner:
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
    """Refuse the whole graph, the whole of urn:p, and each batch of more than one object. QLever
    evaluates EXISTS with a large VALUES wrongly, so a batch is a FILTER IN."""
    assert "VALUES ?o" not in query
    edge = query.split("WHERE {", 1)[1].lstrip()
    if edge.startswith("?s ?p ?o ."):
        return True
    if edge.startswith("?s <urn:p> ?o . FILTER(isBlank(?o))"):
        return False
    if edge.startswith("?s <urn:p> ?o ."):
        batch = re.match(r"\?s <urn:p> \?o \. FILTER\(\?o IN \(([^)]*)\)\)", edge)
        return batch is None or len(batch.group(1).split(",")) > 1
    return False


def test_a_refused_property_is_counted_in_object_batches(monkeypatch, caplog):
    monkeypatch.setattr(structural_strategy, "CENSUS_BATCH_TRIPLES", 2)
    with caplog.at_level("INFO", logger="rdfsolve.mining.structural_strategy"):
        batched = batched_census(monkeypatch, too_large)
    log = caplog.text
    assert "counting 3 properties one at a time" in log, "The fallback to properties is logged"
    assert "property 2/3 urn:p" in log and "urn:p refused; counting 2 objects in 2 batches" in log
    monkeypatch.setattr(structural_strategy, "CENSUS_BATCH_TRIPLES", 10)
    caplog.clear()
    with caplog.at_level("INFO", logger="rdfsolve.mining.structural_strategy"):
        split = batched_census(monkeypatch, too_large)
    assert "urn:p batch of 2 objects refused; split in two" in caplog.text, "Each split is logged"
    assert split["census_batches"] == {"urn:p": 3}, "A refused batch is split until it is counted"
    whole = batched_census(monkeypatch, lambda query: False)
    assert batched["census_batches"] == {"urn:p": 3}, "Two single objects and the blank nodes"
    for count in COUNTS:
        assert batched[count] == whole[count], f"{count}: the batches add up to the whole census"


def test_the_typed_test_reads_only_the_edges_of_a_batch():
    """The typed test repeats the edge and the batch, so that QLever reads the types of the
    subjects of the batch only, not every type triple."""
    queries = structural_strategy._census_queries(
        None, [], "false", False, "urn:p", "VALUES ?o { <urn:b> }"
    )
    untyped = next(q for q in queries if "?untypedTriples" in q)
    typed = untyped.split("FILTER NOT EXISTS {")[1]
    assert "?s <urn:p> ?o ." in typed and "VALUES ?o { <urn:b> }" in typed
    whole = next(
        q
        for q in structural_strategy._census_queries(None, [], "false", False)
        if "?untypedTriples" in q
    )
    assert "FILTER NOT EXISTS { ?s a ?_type . }" in whole, "The whole graph reads all types"


def test_the_census_counts_with_filters():
    """Virtuoso gives wrong counts for a group by BIND(EXISTS ...). Uncovered edges are counted
    with FILTER(!test), the filter of the discovery; a covered count with FILTER(test) can
    exceed the edges on Virtuoso."""
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
        "Without typed profiles every edge is uncovered; Rhea answers FILTER(false) with no row"
    )


REFUSED_DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> ; <urn:c> <urn:x1>, <urn:x2>, <urn:x3>, <urn:x4> .
<urn:b> a <urn:B> .
<urn:u> <urn:p> <urn:b> .
"""


def refused_census(monkeypatch, minimum, error):
    """Mine REFUSED_DATA through an endpoint whose census of urn:c fails with *error*."""
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    monkeypatch.setattr(structural_strategy, "CENSUS_MIN_TRIPLES_PER_OBJECT", minimum)
    with SchemaMiner.from_graph(
        Dataset().parse(data=REFUSED_DATA, format="turtle"), delay=0
    ) as miner:
        select, paged = miner.helper.select, miner.helper.select_with_fallback
        sent = []

        def limited(query, *args, purpose="", **kwargs):
            sent.append((purpose, query))
            if purpose == "structural/coverage" and "<urn:c>" in query:
                raise EndpointTimeoutError(error)
            return select(query, *args, purpose=purpose, **kwargs)

        def limited_pages(query, *args, purpose="", **kwargs):
            sent.append((purpose, query))
            return paged(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", limited)
        monkeypatch.setattr(miner.helper, "select_with_fallback", limited_pages)
        result = miner.mine("refused")
        (entry,) = miner.last_report.config["structural_coverage"]
        return entry, miner.last_report.completion_state, sent, result


def test_a_refused_property_with_one_triple_per_object_is_not_checked(monkeypatch):
    entry, state, sent, result = refused_census(
        monkeypatch, 2, "Query cost/time limit: ANYTIME timeout"
    )
    counted = entry["census_properties"]["urn:c"]
    assert counted["triples"] == 4 and "per object" in counted["refused"]
    assert entry["unchecked_triples"] == 4 and entry["census_refused"] == ["urn:c"]
    assert entry["covered_triples"] + entry["uncovered_triples"] + 4 == entry["triple_count"]
    assert entry["state"] == "partial" and state == "partial", "Partial, not failed"
    assert not any("GROUP BY ?o" in q and "<urn:c>" in q for _, q in sent), "No object listing"
    assert {p.property_uri for p in result.structural_patterns} == {"urn:p"}, "urn:u is found"


def test_a_refused_batch_of_one_object_leaves_the_property_not_checked(monkeypatch):
    """Batches of objects refused down to one object: the property is not checked, and the
    source is partial."""
    entry, state, _, _ = refused_census(monkeypatch, 1, "Connection closed without a response")
    assert "one object" in entry["census_properties"]["urn:c"]["refused"]
    assert entry["unchecked_triples"] == 4 and state == "partial"


QUERIES = ["SELECT (COUNT(*) AS ?triples) WHERE { ?s <urn:p> ?o }"]


def _context(resumed):
    lines = []
    report = SimpleNamespace(
        checkpoint=lambda phase, classes, rows, state="complete": lines.append(
            (phase, classes, rows)
        )
    )
    return SimpleNamespace(report=report, resumed=resumed), lines


def test_a_count_is_kept_and_taken_again_when_resumed(monkeypatch):
    sent = []

    def select(context, query, purpose, **options):
        sent.append(query)
        return [{"triples": {"value": "7"}}]

    monkeypatch.setattr(structural_strategy, "_select", select)
    context, lines = _context({})
    assert structural_strategy._count(context, QUERIES) == {"triples": 7}
    ((phase, key, rows),) = lines
    assert phase == "census" and key[0].startswith("census|") and rows == [{"triples": 7}]
    again, kept = _context({tuple(key): rows})
    assert structural_strategy._count(again, QUERIES) == {"triples": 7}
    assert len(sent) == 1, "The resumed count is not asked again"
    assert kept == lines, "The count is kept for the next run too"


def test_the_census_lines_of_a_checkpoint_are_read_on_resume(tmp_path):
    import json

    path = tmp_path / "x_report.checkpoint.jsonl"
    path.write_text(
        json.dumps(
            {
                "phase": "census",
                "classes": ["census|abc"],
                "rows": [{"triples": 7}],
                "state": "complete",
            }
        )
        + "\n"
    )
    miner = SchemaMiner.__new__(SchemaMiner)
    miner._resume_failed = set()
    miner._rc = SimpleNamespace(report=SimpleNamespace(config={}))
    batches = miner._resumed_batches(str(path), path.read_text())
    assert batches[("census|abc",)] == [{"triples": 7}]


def test_a_refused_property_is_counted_from_its_untyped_subjects(monkeypatch):
    """When every discovered class was mined, every typed edge is covered: a property whose
    coverage test is refused is counted from the edges of its untyped subjects."""
    entry, state, _, _ = refused_census(monkeypatch, 2, "Query cost/time limit")
    assert "per object" in entry["census_properties"]["urn:c"]["refused"], "All queries refused"
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    monkeypatch.setattr(structural_strategy, "CENSUS_MIN_TRIPLES_PER_OBJECT", 2)
    with SchemaMiner.from_graph(
        Dataset().parse(data=REFUSED_DATA, format="turtle"), delay=0
    ) as miner:
        select = miner.helper.select

        def coverage_refused(query, *args, purpose="", **kwargs):
            if (
                purpose == "structural/coverage"
                and "<urn:c>" in query
                and "uncoveredTriples" in query
            ):
                raise EndpointTimeoutError("Query cost/time limit")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", coverage_refused)
        miner.mine("untyped")
        (entry,) = miner.last_report.config["structural_coverage"]
        state = miner.last_report.completion_state
    assert entry["census_properties"]["urn:c"] == {
        "triples": 4,
        "untypedTriples": 0,
        "uncoveredTriples": 0,
        "census": "untyped subjects",
    }
    assert not entry.get("unchecked_triples") and state == "complete"


def _census_sent(monkeypatch, refuse):
    """Mine REFUSED_DATA through an endpoint that refuses the test of each edge of *refuse*."""
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(
        Dataset().parse(data=REFUSED_DATA, format="turtle"), delay=0
    ) as miner:
        select = miner.helper.select
        sent = []

        def edge_test_refused(query, *args, purpose="", **kwargs):
            if purpose == "structural/coverage" and "uncoveredTriples" in query:
                sent.append(query)
                if refuse and f"<{refuse}>" in query:
                    raise EndpointTimeoutError("Query cost/time limit")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", edge_test_refused)
        miner.mine("skipped")
        (entry,) = miner.last_report.config["structural_coverage"]
        return entry, miner.last_report.completion_state, sent


def test_the_test_of_each_edge_is_skipped_after_a_refusal_when_typed_edges_are_covered(
    monkeypatch,
):
    """Every discovered class was mined and the test of urn:c was refused: urn:p, the next
    property, is counted from its untyped subjects without its test, with the same counts."""
    tested, state, sent = _census_sent(monkeypatch, None)
    assert "census_edge_test" not in tested and state == "complete", "No refusal, no skip"
    assert any("<urn:p>" in q for q in sent), "Without a refusal each edge is tested"
    entry, state, sent = _census_sent(monkeypatch, "urn:c")
    assert not any("<urn:p>" in q for q in sent), "No test of each edge after the refusal"
    assert entry["census_edge_test"] == {
        "state": "skipped",
        "reason": "typed_edges_covered",
        "refused": "urn:c",
        "properties": ["urn:p"],
    }, "The skipped properties are recorded"
    assert entry["census_properties"]["urn:p"] == {
        **tested["census_properties"]["urn:p"],
        "census": "untyped subjects",
    }, "The same counts as the test of each edge (urn:u is untyped)"
    assert entry["census_properties"]["urn:p"]["uncoveredTriples"] == 1
    for count in ("triple_count", "covered_triples", "uncovered_triples", "unchecked_triples"):
        assert entry[count] == tested[count], count
    assert entry["state"] == tested["state"] and state == "complete"
