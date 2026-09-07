"""Check that required query failures reach mining reports."""

from unittest.mock import Mock

import pytest

from rdfsolve.miner import SchemaMiner
from rdfsolve.mining.one_shot_strategy import OneShotStrategy
from rdfsolve.mining.pattern_enrichment import enrich_patterns_with_counts
from rdfsolve.mining.strategy import MiningContext
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
from rdfsolve.schema_models import SchemaPattern
from rdfsolve.sparql_helper import EndpointError, PaginationTruncatedError


def miner_and_context():
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    context = MiningContext(
        helper=Mock(),
        graph_uris=["urn:graph"],
        report=miner._report,
        collect_bindings=Mock(),
        class_batch_size=2,
    )
    return miner, context


def test_two_phase_records_failed_required_queries():
    miner, context = miner_and_context()
    context.helper.select.side_effect = EndpointError("offline")
    patterns, reason = TwoPhaseStrategy()._run_phase2_batches(["urn:A"], ["urn:graph"], context)
    assert patterns == []
    assert reason
    assert len(miner._report.report.query_failures) == 4
    assert miner._report.report.total_queries_failed == 4


def test_one_shot_records_failed_required_queries():
    miner, context = miner_and_context()
    context.helper.select.side_effect = EndpointError("offline")
    assert OneShotStrategy().mine(context) == []
    assert miner._report.report.abort_reason
    assert len(miner._report.report.query_failures) == 4


def test_count_failure_leaves_count_unknown():
    miner, context = miner_and_context()
    context.helper.select.side_effect = EndpointError("offline")
    original = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")
    patterns = enrich_patterns_with_counts(
        [original],
        context.helper,
        context.graph_uris,
        context.report,
        context.collect_bindings,
        class_batch_size=2,
        class_chunk_size=100,
        unsafe_paging=False,
        delay=0,
    )
    assert patterns[0].count is None
    assert miner._report.report.abort_reason
    assert miner._report.report.query_failures


def test_collector_retains_rows_before_truncation():
    miner, _ = miner_and_context()
    row = {"class": {"type": "uri", "value": "urn:A"}}

    def pages(*args, **kwargs):
        yield [row]
        raise PaginationTruncatedError("cut", offset=1)

    miner._helper.select_chunked = pages
    with pytest.raises(PaginationTruncatedError) as caught:
        miner._collect_bindings("query", "test", 1)
    assert caught.value.partial_rows == [row]


def test_page_budget_does_not_report_completion():
    miner, _ = miner_and_context()
    miner._helper.select = Mock(return_value={"results": {"bindings": [{"x": {"value": "1"}}]}})
    pages = miner._helper.select_chunked(
        "SELECT ?x WHERE {{ ?x ?p ?o }} LIMIT {limit} OFFSET {offset}",
        chunk_size=1,
        delay_between_chunks=0,
        max_pages=1,
    )
    assert len(next(pages)) == 1
    with pytest.raises(PaginationTruncatedError, match="limit of 1 pages"):
        next(pages)


def test_report_serializes_failed_scope():
    miner, context = miner_and_context()
    context.helper.select.side_effect = EndpointError("offline")
    TwoPhaseStrategy()._run_phase2_batches(["urn:A"], ["urn:graph"], context)
    failure = miner._report.report.model_dump()["query_failures"][0]
    assert failure["category"] == "endpoint"
    assert failure["classes"] == ["urn:A"]
    assert failure["graph_uris"] == ["urn:graph"]


@pytest.mark.parametrize("raw", [{}, {"results": {}}, {"results": {"bindings": "invalid"}}])
def test_malformed_page_is_not_empty_success(raw):
    miner, _ = miner_and_context()
    miner._helper.select = Mock(return_value=raw)
    pages = miner._helper.select_chunked("query", delay_between_chunks=0)
    with pytest.raises(PaginationTruncatedError, match="Expected SELECT result bindings"):
        next(pages)


def test_pagination_does_not_hide_programming_errors():
    miner, _ = miner_and_context()
    miner._helper.select = Mock(side_effect=RuntimeError("bug"))
    with pytest.raises(RuntimeError, match="bug"):
        next(miner._helper.select_chunked("query"))
