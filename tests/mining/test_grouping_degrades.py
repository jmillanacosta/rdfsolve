"""Grouping ontology terms before mining is an optimisation: a refused query there splits its
batch, and a grouping that still fails leaves the source partial with the reason, not failed
(GO-CAM, job 115306: 1,717,993 classes; the first superclass batch exceeded 64 MiB)."""

from __future__ import annotations

import re
from types import SimpleNamespace
from unittest.mock import Mock

from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
from rdfsolve.ontology.hierarchy import fetch_superclasses
from rdfsolve.sparql_helper import EndpointError, ResponseLimitError

HUGE = "urn:huge"


class _Endpoint:
    """Answers subClassOf for small batches; a batch with HUGE is over the response limit."""

    def __init__(self):
        self.sizes = []

    def select(self, query, purpose=""):
        terms = re.findall(r"<(urn:[^>]+)>", query.split("WHERE", 1)[1])
        self.sizes.append(len(terms))
        if HUGE in terms:
            raise ResponseLimitError("Decompressed response exceeds 67108864 bytes")
        rows = [
            {"c": {"value": t}, "parent": {"value": "urn:parent"}}
            for t in terms
            if t != "urn:parent"
        ]
        return {"results": {"bindings": rows}}


def test_a_superclass_batch_that_is_too_large_is_split():
    endpoint, unreadable = _Endpoint(), set()
    terms = [f"urn:t{i}" for i in range(7)] + [HUGE]
    parents = fetch_superclasses(endpoint, terms, batch_size=8, unreadable=unreadable)
    assert unreadable == {HUGE} and parents[HUGE] == set()
    assert all(parents[t] == {"urn:parent"} for t in terms if t != HUGE)
    assert endpoint.sizes[0] == 8 and 1 in endpoint.sizes


def test_a_failed_grouping_leaves_the_source_partial_not_failed(monkeypatch):
    def refuse(*args, **kwargs):
        raise EndpointError("HTTP 500: refused")

    monkeypatch.setattr("rdfsolve.ontology.hierarchy.fetch_superclasses", refuse)
    report = Mock()
    report.report.config = {}
    context = SimpleNamespace(
        ontology_term_budget=100,
        group_before_mining=10,
        report=report,
        helper=None,
        ontology_graph_uris=None,
    )
    classes = [f"urn:c{i}" for i in range(50)]
    assert TwoPhaseStrategy()._group_terms(classes, context) == []
    (outcome,) = report.record_outcome.call_args.args
    assert outcome.state == "failed"
    assert outcome.failures[0].purpose == "ontology-terms/group-before-mining"
    assert report.report.config["ontology_term_grouping"]["state"] == "failed"
    assert report.report.config["ontology_term_grouping"]["classes"] == 50
