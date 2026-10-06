"""rdfsolve.mining.structural_strategy: the census of several properties in one query gives the
counts of one property at a time; a refused batch is split in halves, a rejected form turns
batching off, a missing count is counted alone, the checkpoint keys are those of each property;
the typed test and the union stay shallow (Virtuoso SQ074); a parser limit is not retried; a
refused census leaves the source partial with its typed patterns."""

import re
from unittest.mock import Mock

import pytest
import requests
from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.report_tracking import ReportCollector
from rdfsolve.mining.typed_coverage import ALTERNATIVES_PER_GROUP, typed_match
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointTimeoutError,
    QueryParserLimitError,
    SparqlHelper,
)


def _data() -> str:
    """Typed and untyped subjects, typed and untyped objects, literals with datatypes and
    languages, blank nodes, and 40 properties, so that the census needs several batches."""
    lines = [
        '<urn:a> a <urn:A> ; <urn:p> <urn:b>, <urn:d>, [ a <urn:B> ] ; <urn:q> "x", "y"@en .',
        "<urn:b> a <urn:B> ; <urn:q> 1 .",
        "<urn:c> <urn:p> <urn:b> ; <urn:r> _:x .",
        '_:x <urn:q> "z" .',
        "<urn:d> a <urn:B> .",
    ]
    for i in range(40):
        kind = "<urn:A>" if i % 3 else "<urn:B>"
        lines.append(f'<urn:s{i}> a {kind} ; <urn:p{i:02d}> <urn:b>, "v{i}" .')
        if i % 4 == 0:
            lines.append(f"<urn:t{i}> <urn:p{i:02d}> <urn:a> .")
    return "\n".join(lines)


DATA = _data()


def mine(monkeypatch, per_query, refuse=None, rows=None):
    """Mine DATA through a fake remote endpoint; return the census entry, the census queries
    sent, the census batching record and the checkpoint lines of the census."""
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    monkeypatch.setattr(structural_strategy, "CENSUS_PROPERTIES_PER_QUERY", per_query)
    lines = []
    checkpoint = ReportCollector.checkpoint

    def keep(self, phase, classes, found, state="complete"):
        if phase == "census":
            lines.append((tuple(classes), found))
        return checkpoint(self, phase, classes, found, state)

    monkeypatch.setattr(ReportCollector, "checkpoint", keep)
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        select = miner.helper.select
        sent = []

        def endpoint(query, *args, purpose="", **kwargs):
            if purpose == "structural/coverage":
                sent.append(query)
                if refuse is not None:
                    refuse(query)
            answer = select(query, *args, purpose=purpose, **kwargs)
            if rows is not None and "?_census" in query:
                answer["results"]["bindings"] = rows(answer["results"]["bindings"])
            return answer

        monkeypatch.setattr(miner.helper, "select", endpoint)
        result = miner.mine("batched")
        (entry,) = miner.last_report.config["structural_coverage"]
        batching = miner.last_report.config.get("census_batching")
        state = miner.last_report.completion_state
    return entry, sent, batching, lines, result, state


COUNTS = ("triple_count", "covered_triples", "uncovered_triples", "untyped_subject_triples")


def _same(entry, alone):
    assert entry["census_properties"] == alone["census_properties"]
    for count in COUNTS:
        assert entry[count] == alone[count], count
    assert entry["state"] == alone["state"]


def _properties(query: str) -> int:
    """Return the number of properties that a batched census query counts."""
    return len(set(re.findall(r"WHERE \{ \?s (<[^>]+>) \?o", query)))


@pytest.fixture
def alone(monkeypatch):
    """The census of one property at a time."""
    return mine(monkeypatch, 1)


@pytest.mark.parametrize("per_query", [2, 3, 16, 100])
def test_the_batched_census_gives_the_counts_of_one_property_at_a_time(
    monkeypatch, alone, per_query
):
    entry, sent, batching, _, result, state = mine(monkeypatch, per_query)
    _same(entry, alone[0])
    assert len(entry["census_properties"]) == 44, "43 properties and rdf:type"
    assert len(sent) < len(alone[1]), "Fewer requests"
    assert batching["state"] == "on" and batching["properties"] == 44
    # The examples name blank nodes by labels of each parse.
    assert sorted(p.model_dump_json(exclude={"examples"}) for p in result.structural_patterns) == (
        sorted(p.model_dump_json(exclude={"examples"}) for p in alone[4].structural_patterns)
    ), "The same structural patterns"
    assert state == alone[5] == "complete"
    if per_query == 16:
        assert [_properties(q) for q in sent] == [16, 16, 12], "Three batches, 44 properties"


def test_a_refused_batch_is_split_in_halves(monkeypatch, alone):
    def refuse(query):
        if "?_census" in query and _properties(query) > 5:
            raise EndpointTimeoutError("Virtuoso S1T00 Error SR171: Transaction timed out")

    entry, sent, batching, *_ = mine(monkeypatch, 16, refuse)
    _same(entry, alone[0])
    assert batching["split"] >= 3 and batching["state"] == "on"
    assert max(_properties(q) for q in sent if "?_census" in q) == 16


def test_a_parser_limit_splits_the_batch(monkeypatch, alone):
    def refuse(query):
        if "?_census" in query and _properties(query) > 2:
            raise QueryParserLimitError("Virtuoso 37000 Error SQ074: Too many opened parentheses")

    entry, _, batching, *_ = mine(monkeypatch, 16, refuse)
    _same(entry, alone[0])
    assert batching["split"] > 0


def test_an_endpoint_that_rejects_the_batched_form_is_counted_one_property_at_a_time(
    monkeypatch, alone
):
    def refuse(query):
        if "?_census" in query:
            raise EndpointError("HTTP 400: query rejected: subquery not supported")

    entry, sent, batching, *_ = mine(monkeypatch, 16, refuse)
    _same(entry, alone[0])
    assert batching["state"] == "off" and "rejected" in batching["reason"]
    assert sum("?_census" in q for q in sent) == 1, "Rejected once, then not sent again"


def test_a_missing_count_is_counted_alone(monkeypatch, alone):
    """An engine that answers an empty count with no row (Rhea) leaves the property to the
    query of that property alone."""
    entry, _, batching, *_ = mine(monkeypatch, 16, rows=lambda rows: rows[1:])
    _same(entry, alone[0])
    assert batching["properties"] == 44 - 3, "One property of each batch counted alone"


def test_the_counts_are_kept_under_the_keys_of_each_property(monkeypatch, alone):
    _, _, _, lines, *_ = mine(monkeypatch, 16)
    assert dict(lines) == dict(alone[3]), "A resumed run, batched or not, takes them again"


def test_a_resumed_census_sends_no_batch(monkeypatch, alone):
    resumed = dict(alone[3])
    original = structural_strategy._census_batch

    def with_resume(context, *args):
        context.resumed = resumed
        return original(context, *args)

    monkeypatch.setattr(structural_strategy, "_census_batch", with_resume)
    entry, sent, *_ = mine(monkeypatch, 16)
    _same(entry, alone[0])
    assert not any("?_census" in q for q in sent)


def test_the_union_of_a_batch_stays_shallow(monkeypatch):
    queries = {
        f"urn:p{i}": structural_strategy._census_queries(None, [], "false", False, f"urn:p{i}")
        for i in range(200)
    }
    monkeypatch.setattr(structural_strategy, "CENSUS_UNION_GROUP", 4)
    deep, _ = structural_strategy._batched_census_query(None, [], queries)
    monkeypatch.setattr(structural_strategy, "CENSUS_UNION_GROUP", 600)
    flat, _ = structural_strategy._batched_census_query(None, [], queries)

    def unions_in_a_row(query):
        depth, runs, best = 0, {}, 0
        for part in re.findall(r"\{|\}|UNION", query):
            if part == "{":
                depth += 1
            elif part == "}":
                runs[depth] = 0
                depth -= 1
            else:
                runs[depth] = runs.get(depth, 0) + 1
                best = max(best, runs[depth])
        return best

    assert unions_in_a_row(deep) <= 4 and unions_in_a_row(flat) == 599


def test_the_typed_test_of_many_profiles_stays_shallow():
    """GlyCoNAVI GlycoSample#Date: 195 subject classes with the same literal test made 195
    alternatives, refused by Virtuoso (SQ074); one IN list is one alternative."""
    same = [(f"urn:C{i}", "urn:p", "Literal", "xsd:string") for i in range(195)]
    test = typed_match(same, None, None, "urn:p")
    assert test.count("||") == 0 and test.count("?_subjectType IN") == 1
    varied = [(f"urn:C{i}", "urn:p", f"urn:O{i}", None) for i in range(200)]
    test = typed_match(varied, None, None, "urn:p")
    for chain in re.findall(r"\(([^()]*(?:\([^()]*\)[^()]*)*)\)", test):
        assert chain.count("||") < ALTERNATIVES_PER_GROUP


def test_a_long_typed_test_counts_the_same(monkeypatch):
    """The grouped and nested test covers the same edges as one alternative per profile."""
    lines = ['<urn:u> <urn:p> "free" .', "<urn:z> a <urn:Z> ; <urn:p> 7 ."]
    for i in range(150):
        lines.append(f'<urn:s{i}> a <urn:C{i}> ; <urn:p> "v", <urn:o{i}> .')
        lines.append(f"<urn:o{i}> a <urn:O{i % 7}> .")
    graph = Dataset().parse(data="\n".join(lines), format="turtle")
    keys = [
        (f"urn:C{i}", "urn:p", "Literal", "http://www.w3.org/2001/XMLSchema#string")
        for i in range(150)
    ]
    keys += [(f"urn:C{i}", "urn:p", f"urn:O{i % 7}", None) for i in range(0, 150, 2)]
    query = structural_strategy._census_queries(
        None, [], typed_match(keys, None, None, "urn:p"), False, "urn:p"
    )[-1]
    (row,) = graph.query(query)
    # Uncovered: urn:u (untyped), urn:z's integer, and the IRI objects of odd urn:s{i}.
    assert int(row[0]) == 2 + 75


@pytest.mark.parametrize(
    "body",
    [
        b"Virtuoso 37000 Error SQ074: Line 210: Too many opened parentheses\n\nSPARQL query:\n",
        b"Virtuoso 42000 Error SQ200: Stack Overflow in cost model\n\nSPARQL query:\n",
    ],
)
def test_a_parser_limit_is_not_retried(monkeypatch, body):
    """GlyCoNAVI answered SQ074 three times to the same census query (job 115329): the caller
    gets QueryParserLimitError at once and sends smaller queries."""
    response = requests.Response()
    response.status_code = 500
    response.headers["Content-Type"] = "text/plain"
    response._content = body + b"SELECT * WHERE { ?s ?p ?o }"
    response._content_consumed = True
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    with SparqlHelper("https://example.org/sparql", max_retries=3, initial_backoff=0) as helper:
        request = Mock(return_value=response)
        monkeypatch.setattr(helper._session, "request", request)
        with pytest.raises(QueryParserLimitError):
            helper.select("SELECT * WHERE { ?s ?p ?o }")
    assert request.call_count == 1


def test_a_property_refused_by_the_parser_is_not_read_in_batches_of_objects(monkeypatch):
    def refuse(query):
        if "<urn:p>" in query and "uncoveredTriples" in query:
            raise QueryParserLimitError("Virtuoso 37000 Error SQ074")

    entry, sent, *_ = mine(monkeypatch, 1, refuse)
    # Every class was mined, so the refused test falls back to the untyped subjects.
    assert entry["census_properties"]["urn:p"]["census"] == "untyped subjects"
    assert not any("FILTER(?o IN" in q for q in sent), "Batches of objects keep the same test"


def test_a_refused_census_leaves_the_source_partial(monkeypatch):
    """GlyCoNAVI ended FAILED after 1025 s for one refused census query; the typed patterns do
    not depend on the census, so the source keeps them and ends partial."""

    def refuse(query):
        raise EndpointError("HTTP 500: Virtuoso 37000 Error SQ999: something else")

    entry, _, _, _, result, state = mine(monkeypatch, 16, refuse)
    assert state == "partial" and entry["state"] == "failed"
    assert result.patterns and not result.structural_patterns
