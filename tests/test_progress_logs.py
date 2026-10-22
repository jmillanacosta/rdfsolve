"""Progress in the job log: each query is logged with its purpose and time, a query that runs
long is logged while it runs, and each phase of mining is logged when it starts and ends."""

import logging
import time

from rdflib import Graph

from rdfsolve import sparql_helper
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import SparqlHelper


def test_a_query_is_logged_with_its_purpose_and_time_and_while_it_runs(monkeypatch, caplog):
    helper = SparqlHelper("https://example.org/sparql")
    monkeypatch.setattr(sparql_helper, "HEARTBEAT_S", 0.05)

    def answer(query, *args, **kwargs):
        time.sleep(0.2)
        return {"results": {"bindings": []}}

    monkeypatch.setattr(helper, "_execute_request", answer)
    with caplog.at_level(logging.INFO, logger="rdfsolve.sparql_helper"):
        helper.select("SELECT * WHERE { ?s ?p ?o }", purpose="counts/typed-object")
    lines = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("SELECT [counts/typed-object] still running after") for m in lines)
    assert any(
        m.startswith("SELECT [counts/typed-object] completed in") and m.endswith(" s")
        for m in lines
    )


def test_each_mining_phase_is_logged_when_it_starts_and_ends(caplog):
    graph = Graph().parse(data='<urn:a> a <urn:A> ; <urn:p> "x" .', format="turtle")
    with (
        caplog.at_level(logging.INFO, logger="rdfsolve.mining.report_tracking"),
        SchemaMiner.from_graph(graph, delay=0) as miner,
    ):
        miner.mine("phases")
        phases = [p.name for p in miner.last_report.phases]
    lines = [r.getMessage() for r in caplog.records]
    assert phases and "counts" in phases
    for name in phases:
        assert f"Phase {name} started" in lines
        assert any(m.startswith(f"Phase {name} finished:") for m in lines), name
