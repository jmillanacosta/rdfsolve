"""rdfsolve.sparql_helper against endpoints that redirect, cap their results, say they are
busy, or answer a heavy query with an empty body (remote rehearsal of 2026-10-06)."""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper

CLASSES = [f"urn:c{i:04d}" for i in range(2500)]
CAP = 1000


def _rows(query: str) -> list[dict]:
    """Answer a class listing as a capped Virtuoso does: never more than CAP rows."""
    offset = re.search(r"OFFSET\s+(\d+)", query)
    limit = re.search(r"LIMIT\s+(\d+)", query)
    start = int(offset.group(1)) if offset else 0
    end = start + int(limit.group(1)) if limit else len(CLASSES)
    return [{"class": {"type": "uri", "value": c}} for c in CLASSES[start:end][:CAP]]


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence the test server
        pass

    def _answer(self, query: str | None) -> None:
        path = urlsplit(self.path).path
        if path == "/old":  # http -> https of AgroLD: the endpoint moved
            self.send_response(302)
            self.send_header("Location", "/sparql")
            self.end_headers()
            return
        if path == "/busy":
            self.server.busy += 1
            if self.server.busy == 1:
                body = b"<html>Too many concurrent queries. Please try again later.</html>"
                self.send_response(503)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
        if path == "/empty":
            self.send_response(200)
            self.send_header("Content-Type", "application/sparql-results+json")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if not query:  # a redirected POST that became a GET without its query
            self.send_response(406)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.server.queries.append(query)
        body = json.dumps(
            {"head": {"vars": ["class"]}, "results": {"bindings": _rows(query)}}
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/sparql-results+json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._answer(parse_qs(urlsplit(self.path).query).get("query", [None])[0])

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        data = self.rfile.read(length).decode()
        if self.headers.get("Content-Type", "").startswith("application/sparql-query"):
            self._answer(data)
        else:
            self._answer(parse_qs(data).get("query", [None])[0])


@contextmanager
def _server() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.queries, server.busy = [], 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", server
    finally:
        server.shutdown()
        server.server_close()


def test_a_redirected_post_is_sent_again_with_its_query_to_the_new_url():
    """AgroLD answers a POST to its http URL with 302 to https; requests would follow it as a
    GET without the query (HTTP 406). The request is sent again, with its method and body, and
    the new URL is kept and recorded."""
    with _server() as (base, server), SparqlHelper(f"{base}/old", use_post=True) as helper:
        rows = helper.select("SELECT ?class WHERE { ?s a ?class } LIMIT 3")
        assert len(rows["results"]["bindings"]) == 3
        assert helper.endpoint_url == f"{base}/sparql"
        assert helper.redirected_from == f"{base}/old"
        helper.select("SELECT ?class WHERE { ?s a ?class } LIMIT 1")
        assert len(server.queries) == 2


def test_a_listing_cut_at_a_server_cap_is_read_again_in_pages_of_the_cap():
    """FANAVI answers 1000 of its 33,358 classes, whatever the LIMIT, with HTTP 200: class
    discovery pages the listing by the cap, and select_chunked does not take a short page of a
    common cap for the end."""
    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

    with _server() as (base, _), SparqlHelper(f"{base}/sparql") as helper:
        pages = list(
            helper.select_chunked(
                SparqlHelper.prepare_paginated_query(
                    "SELECT DISTINCT ?class WHERE { ?s a ?class } ORDER BY ?class"
                ),
                chunk_size=10_000,
            )
        )
        assert sum(len(p) for p in pages) == len(CLASSES)

        def collect(query, purpose, size):
            return [
                row
                for page in helper.select_chunked(query, chunk_size=size or 10_000, purpose=purpose)
                for row in page
            ]

        context = SimpleNamespace(
            helper=helper,
            class_chunk_size=None,
            graph_uris=None,
            type_context_graph_uris=None,
            chunk_size=10_000,
            report=Mock(),
            collect_bindings=collect,
        )
        classes = TwoPhaseStrategy()._discover_classes(context)
    assert sorted(classes) == CLASSES


def test_a_busy_server_is_waited_for_and_a_proxy_error_is_not(monkeypatch):
    """SwissLipids: HTTP 503 "Too many concurrent queries. Please try again later." is waited
    out with a long backoff (shared by the host), then the query is sent again; a 503 that does
    not say the server is busy (a proxy that cannot resolve the host) is not waited for."""
    defer = Mock()
    monkeypatch.setattr("rdfsolve._http_policy.defer_host", defer)
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    with _server() as (base, server), SparqlHelper(f"{base}/busy", max_retries=1) as helper:
        rows = helper.select("SELECT ?class WHERE { ?s a ?class } LIMIT 2")
        assert len(rows["results"]["bindings"]) == 2 and server.busy == 2
        assert ("127.0.0.1", SparqlHelper.OVERLOAD_BACKOFF_S) in [
            c.args for c in defer.call_args_list
        ]
    helper = SparqlHelper("https://example.org/sparql", max_retries=1)
    assert not helper._overloaded(503, "the requested url could not be retrieved")
    assert helper._overloaded(503, "too many concurrent queries. please try again later.")
    assert not helper._overloaded(503, "estimated execution time exceeds the limit")


def test_an_empty_answer_is_a_cut_not_repeated_unchanged():
    """FANAVI answers its heavy queries with an empty HTTP 200: the caller is told to make the
    query smaller (EndpointTimeoutError), and the query is not sent again unchanged."""
    with _server() as (base, _), SparqlHelper(f"{base}/empty", max_retries=3) as helper:
        request = Mock(wraps=helper._session.request)
        helper._session.request = request
        with pytest.raises(EndpointTimeoutError, match="empty response"):
            helper.select("SELECT ?class WHERE { ?s a ?class }")
        assert request.call_count == 1


def test_requests_is_not_asked_to_follow_redirects(monkeypatch):
    """Redirects are followed by the helper (with the body), never by requests."""
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/sparql-results+json"
    response._content = b'{"head": {"vars": []}, "results": {"bindings": []}}'
    response._content_consumed = True
    with SparqlHelper("https://example.org/sparql") as helper:
        request = Mock(return_value=response)
        monkeypatch.setattr(helper._session, "request", request)
        monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
        helper.select("SELECT * WHERE { ?s ?p ?o } LIMIT 1")
    assert request.call_args.kwargs["allow_redirects"] is False


class _NoListing:
    """A helper whose graph listing is cut (STRING: HTTP 502 at 62 s)."""

    def __init__(self, described: list[str]):
        self.described = described

    @contextmanager
    def budget(self, seconds):
        yield

    def prepare_paginated_query(self, query):
        return SparqlHelper.prepare_paginated_query(query)

    def select_chunked(self, *args, **kwargs):
        from rdfsolve.sparql_helper import PaginationTruncatedError

        raise PaginationTruncatedError("Pagination abandoned at offset 0: HTTP 502", offset=0)
        yield  # pragma: no cover

    def select(self, query, purpose=""):
        rows = [{"g": {"type": "uri", "value": g}} for g in self.described]
        return {"results": {"bindings": rows}}


def test_graph_names_come_from_the_service_description_when_not_listed():
    from rdfsolve.mining.graph_selection import discover_data_graphs
    from rdfsolve.sparql_helper import PaginationTruncatedError

    helper = _NoListing(["urn:data", "http://www.openlinksw.com/schemas/virtrdf#"])
    assert discover_data_graphs(helper, excluded_prefixes=("http://www.openlinksw.com/",)) == [
        "urn:data"
    ]
    with pytest.raises(PaginationTruncatedError):
        discover_data_graphs(_NoListing([]))


def test_unlisted_named_graphs_leave_the_source_partial_not_failed():
    """The default graph has no typed data and the graphs are not listed: no exception (the
    source is not failed for an optional step), but an unresolved failure is recorded, so the
    source is not complete and apparently empty."""
    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

    report = Mock()
    report.report.config = {}
    context = SimpleNamespace(
        helper=_NoListing([]),
        excluded_graph_prefixes=(),
        report=report,
        type_context_graph_uris=None,
        ontology_graph_uris=None,
    )
    assert TwoPhaseStrategy()._discover_classes_in_named_graphs(context) == []
    (outcome,) = report.record_outcome.call_args.args
    assert outcome.state == "failed" and outcome.failures[0].purpose == "void/graph-discovery"
    assert report.report.config["named_graph_discovery"]["state"] == "graphs_not_listed"


@pytest.mark.parametrize(
    "body",
    [
        b"Virtuoso S1T00 Error SR171: Transaction timed out",
        b"Virtuoso 22026 Error SR319: Max row length is exceeded when trying to store a string",
        b"Virtuoso 42000 Error D1CTX: Hash dictionary is full, exceeded 2000000 entries",
        b"Virtuoso 42000 Error SQ200: Stack Overflow in cost model",
    ],
)
def test_a_virtuoso_refusal_that_repeats_is_not_retried(monkeypatch, body):
    """IDEAL (SR171), GlyTouCan (SR319), RIKEN BRC (D1CTX on a CONSTRUCT of its VoID graph, job
    115326) and SIBiLS (SQ200) refuse the same query every time: the caller gets
    EndpointTimeoutError at once and runs its fallback."""
    response = requests.Response()
    response.status_code = 500
    response.headers["Content-Type"] = "text/plain"
    response._content = body
    response._content_consumed = True
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    with SparqlHelper("https://example.org/sparql", max_retries=3, initial_backoff=0) as helper:
        request = Mock(return_value=response)
        monkeypatch.setattr(helper._session, "request", request)
        with pytest.raises(EndpointTimeoutError):
            helper.select("SELECT * WHERE { ?s ?p ?o }")
    assert request.call_count == 1


# A gateway in front of the server: each path answers with its scripted errors first, then
# with the graphs (STRING: HTTP 502 at 62 s for the graph listing, job 115325).
SQUID = (
    b"<html><title>ERROR: The requested URL could not be retrieved</title>"
    b"Unable to determine IP address from host name ERR_DNS_FAIL</html>"
)
GATEWAY_SCRIPTS: dict[str, list[tuple[int, bytes, dict[str, str]]]] = {
    "/busy-502": [(502, b"<html>Upstream server overloaded, try again later</html>", {})],
    "/retry-504": [(504, b"<html>Gateway Timeout</html>", {"Retry-After": "7"})],
    "/bare-502": [(502, b"<html><center>502 Bad Gateway</center>nginx</html>", {})] * 9,
    "/listing-502": [(502, b"<html><center>502 Bad Gateway</center>nginx</html>", {})] * 2,
    "/squid": [(502, SQUID, {})] * 9,
    "/always-502": [(502, b"<html>502 Bad Gateway</html>", {})] * 99,
}


class _Gateway(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence the test server
        pass

    def _send(self, status: int, body: bytes, headers: dict[str, str]) -> None:
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlsplit(self.path).path
        query = parse_qs(urlsplit(self.path).query).get("query", [""])[0]
        self.server.requests.append((path, query))
        script = self.server.scripts.setdefault(path, list(GATEWAY_SCRIPTS.get(path, [])))
        if script:
            self._send(*script.pop(0))
            return
        graphs = [] if "service-description" in query else ["urn:g1", "urn:g2"]
        rows = [{"g": {"type": "uri", "value": g}} for g in graphs]
        if "OFFSET 0" not in query and "service-description" not in query:
            rows = []  # one page of graphs
        body = json.dumps({"head": {"vars": ["g"]}, "results": {"bindings": rows}}).encode()
        self._send(200, body, {"Content-Type": "application/sparql-results+json"})


@contextmanager
def _gateway(monkeypatch) -> Iterator[tuple[str, ThreadingHTTPServer, Mock]]:
    defer = Mock()
    monkeypatch.setattr("rdfsolve._http_policy.defer_host", defer)
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Gateway)
    server.requests, server.scripts = [], {}
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", server, defer
    finally:
        server.shutdown()
        server.server_close()


def _waits(defer: Mock) -> list[float]:
    return [call.args[1] for call in defer.call_args_list]


def test_a_gateway_that_says_the_host_is_busy_is_waited_out(monkeypatch):
    """A 502 whose body says the server is overloaded, or a 504 with Retry-After, is waited
    out like a busy 503, whatever the retries of the step."""
    with _gateway(monkeypatch) as (base, server, defer):
        with SparqlHelper(f"{base}/busy-502", max_retries=1) as helper:
            helper.select("SELECT ?g WHERE { GRAPH ?g {} } OFFSET 0")
        assert _waits(defer) == [SparqlHelper.OVERLOAD_BACKOFF_S]
        defer.reset_mock()
        with SparqlHelper(f"{base}/retry-504", max_retries=1) as helper:
            helper.select("SELECT ?g WHERE { GRAPH ?g {} } OFFSET 0")
        assert _waits(defer) == [7.0]
    assert [p for p, _ in server.requests].count("/busy-502") == 2


def test_a_bare_gateway_error_is_marked_and_a_proxy_dns_failure_is_not(monkeypatch):
    """A bare 502 still goes to the caller at once (it makes the query smaller), marked as a
    possible overload; bio2rdf's squid page, a proxy that cannot reach the host, is neither
    waited for nor marked."""
    from rdfsolve.sparql_helper import SparqlHelperError

    with _gateway(monkeypatch) as (base, server, defer):
        with (
            SparqlHelper(f"{base}/bare-502", max_retries=3) as helper,
            pytest.raises(EndpointTimeoutError) as bare,
        ):
            helper.select("SELECT * WHERE { ?s ?p ?o }")
        assert bare.value.gateway_overload and bare.value.status_code == 502
        with (
            SparqlHelper(f"{base}/squid", max_retries=3) as helper,
            pytest.raises(SparqlHelperError) as squid,
        ):
            helper.select("SELECT * WHERE { ?s ?p ?o }")
        assert not squid.value.gateway_overload
        assert defer.call_count == 0
    paths = [p for p, _ in server.requests]
    assert paths.count("/bare-502") == 1 and paths.count("/squid") == 1
    helper = SparqlHelper("https://example.org/sparql")
    assert not helper._overloaded(502, SQUID.decode().lower() + " overloaded")
    assert not helper._gateway_overload(504, "estimated execution time exceeds the limit")


def test_graph_discovery_waits_out_an_overloaded_gateway(monkeypatch):
    """STRING: the graph listing got HTTP 502 and the source, whose data is only in named
    graphs, mined 0 classes. The host is now waited out (30 s, then 60 s, shared by the host)
    and the listing is sent again."""
    from rdfsolve.mining.graph_selection import discover_data_graphs

    with (
        _gateway(monkeypatch) as (base, _, defer),
        SparqlHelper(f"{base}/listing-502", max_retries=3) as helper,
    ):
        assert discover_data_graphs(helper) == ["urn:g1", "urn:g2"]
    assert _waits(defer) == [30.0, 60.0]
    assert [call.args[0] for call in defer.call_args_list] == ["127.0.0.1"] * 2


def test_graph_discovery_does_not_wait_for_a_proxy_that_cannot_reach_the_host(monkeypatch):
    """A proxy DNS failure is raised at once (after the service-description fallback)."""
    from rdfsolve.mining.graph_selection import discover_data_graphs
    from rdfsolve.sparql_helper import SparqlHelperError

    with (
        _gateway(monkeypatch) as (base, _, defer),
        SparqlHelper(f"{base}/squid", max_retries=1) as helper,
        pytest.raises(SparqlHelperError),
    ):
        discover_data_graphs(helper)
    assert defer.call_count == 0


def test_graph_discovery_gives_up_after_the_busy_host_waits(monkeypatch):
    """A gateway that stays overloaded is waited out OVERLOAD_RETRIES times, then the source
    ends as before (service description, else the refusal)."""
    from rdfsolve.mining.graph_selection import discover_data_graphs
    from rdfsolve.sparql_helper import SparqlHelperError

    with (
        _gateway(monkeypatch) as (base, _, defer),
        SparqlHelper(f"{base}/always-502", max_retries=1) as helper,
        pytest.raises(SparqlHelperError),
    ):
        discover_data_graphs(helper)
    assert _waits(defer) == [30.0, 60.0, 120.0, 240.0]
