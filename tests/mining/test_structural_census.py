"""rdfsolve.mining.structural_strategy census: through an endpoint the census and the discovery of
uncovered edges are sent one property at a time; a refused property is counted in batches of its
objects, split until counted, or recorded as not checked; counts are kept in the checkpoint and
taken again on resume."""

import re
from types import SimpleNamespace

from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointTimeoutError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> ; <urn:q> "x" .
<urn:c> <urn:p> <urn:b> .
"""


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
    for count in (
        "triple_count",
        "covered_triples",
        "uncovered_triples",
        "untyped_subject_triples",
    ):
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
    evaluates EXISTS with a VALUES of 768 objects wrongly (Bgee), so a batch is a FILTER IN."""
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
    for count in (
        "triple_count",
        "covered_triples",
        "uncovered_triples",
        "untyped_subject_triples",
    ):
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
        q
        for q in structural_strategy._census_queries(None, [], "false", False)
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
        "Without typed profiles every edge is uncovered; Rhea answers FILTER(false) with no row"
    )


REFUSED_DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> ; <urn:c> <urn:x1>, <urn:x2>, <urn:x3>, <urn:x4> .
<urn:b> a <urn:B> .
<urn:u> <urn:p> <urn:b> .
"""


def test_a_refused_property_with_one_triple_per_object_is_not_checked(monkeypatch):
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    monkeypatch.setattr(structural_strategy, "CENSUS_MIN_TRIPLES_PER_OBJECT", 2)
    with SchemaMiner.from_graph(
        Dataset().parse(data=REFUSED_DATA, format="turtle"), delay=0
    ) as miner:
        select, paged = miner.helper.select, miner.helper.select_with_fallback
        sent = []

        def limited(query, *args, purpose="", **kwargs):
            sent.append((purpose, query))
            if purpose == "structural/coverage" and "<urn:c>" in query:
                raise EndpointTimeoutError("Query cost/time limit: Virtuoso ANYTIME timeout")
            return select(query, *args, purpose=purpose, **kwargs)

        def limited_pages(query, *args, purpose="", **kwargs):
            sent.append((purpose, query))
            return paged(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", limited)
        monkeypatch.setattr(miner.helper, "select_with_fallback", limited_pages)
        result = miner.mine("refused")
        (entry,) = miner.last_report.config["structural_coverage"]
        state = miner.last_report.completion_state
    counted = entry["census_properties"]["urn:c"]
    assert counted["triples"] == 4 and "per object" in counted["refused"]
    assert entry["unchecked_triples"] == 4 and entry["census_refused"] == ["urn:c"]
    assert entry["covered_triples"] + entry["uncovered_triples"] + 4 == entry["triple_count"]
    assert entry["state"] == "partial" and state == "partial", "Partial, not failed"
    assert not any("GROUP BY ?o" in q and "<urn:c>" in q for _, q in sent), "No object listing"
    assert {p.property_uri for p in result.structural_patterns} == {"urn:p"}, "urn:u is found"


def test_a_refused_batch_of_one_object_leaves_the_property_not_checked(monkeypatch):
    """SIBiLS rdf:type: batches of classes were refused down to one class (the proxy drops a
    query after 900 s); the property is then not checked, and the source is partial."""
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    monkeypatch.setattr(structural_strategy, "CENSUS_MIN_TRIPLES_PER_OBJECT", 1)
    with SchemaMiner.from_graph(
        Dataset().parse(data=REFUSED_DATA, format="turtle"), delay=0
    ) as miner:
        select = miner.helper.select

        def limited(query, *args, purpose="", **kwargs):
            if purpose == "structural/coverage" and "<urn:c>" in query:
                raise EndpointTimeoutError("Connection closed without a response after 902 s")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", limited)
        miner.mine("refused batches")
        (entry,) = miner.last_report.config["structural_coverage"]
        state = miner.last_report.completion_state
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
