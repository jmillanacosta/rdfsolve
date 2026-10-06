"""The Virtuoso graph exclusion sends input:default-graph-exclude only, and is checked first:
a pragma that empties or slows the endpoint's answers is not used.

The endpoint is a local HTTP server that reads Virtuoso's input:*-graph-exclude pragmas over an
rdflib dataset. Its modes: "virtuoso" (as documented), "dbpedia" (as dbpedia.org on 2026-10-06:
a query with input:named-graph-exclude has an empty default graph, HTTP 200, no error), "empty"
(any pragma empties the answer) and "slow" (any pragma adds 0.5 s).
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
from rdflib import Dataset, URIRef

from rdfsolve.sparql_helper import (
    SparqlHelper,
    graph_exclusion_prologue,
    has_dataset_clause,
)

DATA = "http://data.example/"
SYSTEM = "http://www.openlinksw.com/schemas/virtrdf#"
GRAPHS = {
    DATA: "<urn:s1> a <urn:Gene> ; <urn:p> <urn:s2> . <urn:s2> a <urn:Protein> .",
    SYSTEM: "<urn:m1> a <urn:virtrdf:QuadMap> ; <urn:virtrdf:qmTable> <urn:m2> .",
}
_DEFINE = re.compile(r"^\s*DEFINE\s+input:(default|named)-graph-exclude\s+<([^>]*)>\s*$", re.M)


class _Virtuoso(BaseHTTPRequestHandler):
    """Answer SELECT and ASK over GRAPHS, reading the graph exclusion pragmas."""

    def log_message(self, *args):
        """Silence the test server."""

    def _answer(self, query: str) -> None:
        self.server.queries.append(query)
        mode = self.server.mode
        default_out = {g for kind, g in _DEFINE.findall(query) if kind == "default"}
        named_out = {g for kind, g in _DEFINE.findall(query) if kind == "named"}
        body_query = _DEFINE.sub("", query)
        pragmas = bool(default_out or named_out)
        if mode == "slow" and pragmas:
            time.sleep(0.5)
        if mode == "empty" and pragmas:
            body_query = "SELECT ?s WHERE { FILTER(false) }"
        dataset = Dataset()
        for graph, turtle in GRAPHS.items():
            if graph not in named_out:
                dataset.graph(URIRef(graph)).parse(data=turtle, format="turtle")
            # dbpedia.org, 2026-10-06: input:named-graph-exclude empties the default graph.
            emptied = mode == "dbpedia" and named_out
            if graph not in default_out and not emptied:
                dataset.default_graph.parse(data=turtle, format="turtle")
        result = dataset.query(body_query)
        if result.type == "ASK":
            # dbpedia.org answered ASK true with the pragma where SELECT answered no row.
            payload = {"head": {}, "boolean": bool(result.askAnswer) or mode == "dbpedia"}
        elif result.type == "CONSTRUCT":
            payload = {"head": {"vars": []}, "results": {"bindings": []}}
        else:
            rows = [
                {
                    str(v): {"type": "uri", "value": str(row[v])}
                    if isinstance(row[v], URIRef)
                    else {"type": "literal", "value": str(row[v])}
                    for v in result.vars
                    if row[v] is not None
                }
                for row in result
            ]
            rows = [r for r in rows if "urn:x-rdflib:default" not in json.dumps(r)]
            payload = {"head": {"vars": [str(v) for v in result.vars]}, "results": {"bindings": rows}}
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/sparql-results+json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        """Answer a GET request."""
        self._answer(parse_qs(urlsplit(self.path).query)["query"][0])

    def do_POST(self):
        """Answer a POST request."""
        data = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode()
        if self.headers.get("Content-Type", "").startswith("application/sparql-query"):
            self._answer(data)
        else:
            self._answer(parse_qs(data)["query"][0])


@contextmanager
def _endpoint(mode: str) -> Iterator[tuple[str, ThreadingHTTPServer]]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Virtuoso)
    server.mode, server.queries = mode, []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/sparql", server
    finally:
        server.shutdown()
        server.server_close()


def _values(helper: SparqlHelper, query: str, var: str) -> set[str]:
    return {b[var]["value"] for b in helper.select(query)["results"]["bindings"] if var in b}


CLASSES = "SELECT DISTINCT ?c WHERE { ?s a ?c }"


def _miner(url: str):
    from rdfsolve.mining.miner import SchemaMiner
    from rdfsolve.schema_models._constants import SUGGESTED_SERVICE_GRAPHS

    return SchemaMiner(endpoint_url=url, excluded_graph_prefixes=SUGGESTED_SERVICE_GRAPHS, delay=0)


@pytest.fixture(autouse=True)
def _graphs_listed(monkeypatch):
    from rdfsolve import void_retrieval

    monkeypatch.setattr(void_retrieval, "discover_graph_names", lambda *a, **k: list(GRAPHS))


@pytest.mark.parametrize("mode", ["virtuoso", "dbpedia"])
def test_the_engine_graphs_are_left_out_of_the_default_graph_only(mode):
    """dbpedia.org answered no row to a query with input:named-graph-exclude (2026-10-06), so
    only input:default-graph-exclude is sent, and the mined classes are the data classes."""
    with _endpoint(mode) as (url, server):
        miner = _miner(url)
        assert _values(miner.helper, CLASSES, "c") >= {"urn:virtrdf:QuadMap", "urn:Gene"}
        schema = miner.mine("engine")
        record = miner.last_report.config["excluded_graphs"]
        assert record["state"] == "excluded" and record["graph_uris"] == [SYSTEM]
        assert record["quirks"] == []
        assert record["engine_classes"]["engine_only"] == ["urn:virtrdf:QuadMap"]
        classes = {p.subject_class for p in schema.patterns} | {
            p.object_class for p in schema.patterns
        }
        assert {"urn:Gene", "urn:Protein"} <= classes
        assert not any("virtrdf" in str(c) for c in classes)
        assert not any("virtrdf" in p.property_uri for p in schema.patterns)
        assert not any("named-graph-exclude" in q for q in server.queries)
        assert any("default-graph-exclude" in q for q in server.queries)
        # The class listing is read without the prologue and cleaned of the engine classes.
        listings = [q for q in server.queries if re.search(r"\{ \?s a \?class \. \}", q)]
        assert listings and not any("DEFINE" in q for q in listings)
        patterns = [q for q in server.queries if "GROUP BY ?class ?p" in q]
        assert patterns and all("DEFINE input:default-graph-exclude" in q for q in patterns)
    assert miner.helper.excluded_graphs == [] and miner.helper.engine_only_classes is None


@pytest.mark.parametrize(
    ("mode", "state", "effect"),
    [
        ("empty", "emptied_by_pragma", "empties a non-empty answer"),
        ("slow", "slowed_by_pragma", "makes a one-row query slow"),
    ],
)
def test_a_pragma_that_empties_or_slows_answers_is_not_used(monkeypatch, mode, state, effect):
    from rdfsolve.mining.miner import SchemaMiner

    monkeypatch.setattr(SchemaMiner, "EXCLUSION_SLOW_S", 0.2)
    with _endpoint(mode) as (url, server):
        miner = _miner(url)
        with miner._session("engine"):
            record = miner.last_report.config["excluded_graphs"]
            assert record["state"] == state
            assert record["quirks"] == [{"pragma": "input:default-graph-exclude", "effect": effect}]
            assert miner.helper.excluded_graphs == []
            server.queries.clear()
            miner.helper.select(CLASSES)
            assert "DEFINE" not in server.queries[-1]


def test_a_dataset_clause_in_a_literal_or_comment_is_not_one():
    assert has_dataset_clause("SELECT ?s FROM <http://g/> WHERE { ?s ?p ?o }")
    assert has_dataset_clause("SELECT ?s FROM NAMED ex:g WHERE { GRAPH ?g { ?s ?p ?o } }")
    assert has_dataset_clause("select ?s from<http://g/> where { ?s ?p ?o }")
    assert not has_dataset_clause('SELECT ?s WHERE { ?s ?p "FROM <http://g/>" }')
    assert not has_dataset_clause('SELECT ?s WHERE { ?s ?p """x " FROM <http://g/>""" }')
    assert not has_dataset_clause("SELECT ?s WHERE { ?s ?p ?o } # FROM <http://g/>")
    assert not has_dataset_clause("SELECT ?from WHERE { ?from ex:from <http://g/> }")


def test_the_prologue_never_names_the_named_graphs():
    assert graph_exclusion_prologue([SYSTEM]) == f"DEFINE input:default-graph-exclude <{SYSTEM}>\n"
