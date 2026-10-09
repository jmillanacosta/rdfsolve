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
from unittest.mock import MagicMock, Mock
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
        from rdfsolve.sparql_helper import PaginationTruncatedError

        if purpose == "void/graph-discovery":
            raise PaginationTruncatedError("HTTP 502", offset=0)
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
# STRING's Cloudflare answer to a census query that ran about 61 s (job 115591).
CLOUDFLARE_502 = (
    b'{"type":"https://developers.cloudflare.com/support/troubleshooting/http-status-codes/'
    b'cloudflare-5xx-errors/error-502/","title":"Error 502: Bad gateway","status":502,'
    b'"detail":"The origin web server returned an invalid or incomplete response to Cloudflare.'
    b' This typically indicates the origin is overloaded."}'
)
GATEWAY_SCRIPTS: dict[str, list[tuple[int, bytes, dict[str, str]]]] = {
    "/busy-502": [(502, b"<html>Upstream server overloaded, try again later</html>", {})],
    "/retry-504": [(504, b"<html>Gateway Timeout</html>", {"Retry-After": "7"})],
    "/bare-502": [(502, b"<html><center>502 Bad Gateway</center>nginx</html>", {})] * 9,
    # two listings and the trivial query between them get a gateway error, then all answer
    "/listing-502": [(502, b"<html><center>502 Bad Gateway</center>nginx</html>", {})] * 3,
    "/squid": [(502, SQUID, {})] * 9,
    "/always-502": [(502, b"<html>502 Bad Gateway</html>", {})] * 99,
    "/cloudflare-502": [(502, CLOUDFLARE_502, {"Content-Type": "application/json"})] * 99,
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
        if "OFFSET" in query and "OFFSET 0" not in query:
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


def test_a_late_busy_gateway_answer_twice_is_a_timeout_of_the_query(monkeypatch, caplog):
    """STRING (job 115591): Cloudflare answered one census query with "502 ... the origin is
    overloaded" after about 61 s, five times, and the busy-host waits ended in
    EndpointRateLimitError, which failed the source. A busy answer that comes late twice in a
    row is the gateway's timer: the query is not repeated a third time, and the caller gets
    EndpointTimeoutError (it makes the query smaller or samples it). The next query cut at the
    same time is given up at once. A quick busy answer is still waited out."""
    with _gateway(monkeypatch) as (base, server, defer):
        with SparqlHelper(f"{base}/cloudflare-502", max_retries=3) as helper:
            helper.GATEWAY_CUT_AFTER_S = 0.0  # every answer counts as late
            with caplog.at_level("WARNING"), pytest.raises(EndpointTimeoutError) as cut:
                helper.select("SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:big> ?o }")
            assert "Gateway timeout: HTTP 502" in str(cut.value) and cut.value.status_code == 502
            assert not cut.value.gateway_overload, "A timeout of the query, not a busy host"
            assert _waits(defer) == [SparqlHelper.OVERLOAD_BACKOFF_S], "One wait, not five"
            assert [p for p, _ in server.requests].count("/cloudflare-502") == 2
            warned = [r for r in caplog.records if "treated as a gateway timeout" in r.message]
            assert len(warned) == 1
            with pytest.raises(EndpointTimeoutError, match="Gateway timeout"):
                helper.select("SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:big2> ?o }")
            assert _waits(defer) == [SparqlHelper.OVERLOAD_BACKOFF_S], "Known cut: no wait"
            assert [p for p, _ in server.requests].count("/cloudflare-502") == 3
        defer.reset_mock()
        server.scripts.clear()
        server.scripts["/cloudflare-502"] = [
            (502, CLOUDFLARE_502, {"Content-Type": "application/json"})
        ] * 2
        with SparqlHelper(f"{base}/cloudflare-502", max_retries=1) as helper:
            # Quick busy answers (the default threshold): waited out, then answered.
            assert helper.select("SELECT ?g WHERE { GRAPH ?g {} } OFFSET 0")
        assert _waits(defer) == [30.0, 60.0]


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
    graphs, mined 0 classes. The host is now waited out (30 s; then, as it does not
    answer a trivial query either, 60 s; shared by the host) and the listing is sent again."""
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


# pdbj.bmrb (job 115329): Apache answers the class listing with "502 Proxy Error" at 121-126 s
# on every try, while a trivial query answers in 1 s.
PROXY_ERROR = (
    b"<html><title>502 Proxy Error</title>The proxy server received an invalid response from "
    b"an upstream server. Reason: Error reading from remote server</html>"
)


class _ClassGateway(BaseHTTPRequestHandler):
    """/flaky: two gateway errors, then the classes; /cut: the listing is always cut, ASK
    answers; /sampled: the listing is cut unless it reads a sample (a LIMIT sub-select), ASK
    answers; /down: every query gets a gateway error."""

    def log_message(self, *args):  # silence the test server
        pass

    def do_GET(self):
        path = urlsplit(self.path).path
        query = parse_qs(urlsplit(self.path).query).get("query", [""])[0]
        self.server.requests.append((path, query))
        ask = query.lstrip().upper().startswith("ASK")
        listing = [q for p, q in self.server.requests if p == path and "ASK" not in q.upper()]
        if (
            path == "/down"
            or (path == "/cut" and not ask)
            or (path == "/sampled" and not ask and "} LIMIT " not in query)
            or (path == "/flaky" and len(listing) <= 2)
        ):
            self.send_response(502)
            self.send_header("Content-Length", str(len(PROXY_ERROR)))
            self.end_headers()
            self.wfile.write(PROXY_ERROR)
            return
        if ask:
            payload = {"head": {}, "boolean": True}
        else:
            rows = [{"class": {"type": "uri", "value": c}} for c in ("urn:A", "urn:B")]
            if path == "/sampled":
                rows = rows[:1]  # the sample holds only the type statements of urn:A
            if "OFFSET" in query and "OFFSET 0" not in query:
                rows = []
            payload = {"head": {"vars": ["class"]}, "results": {"bindings": rows}}
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/sparql-results+json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@contextmanager
def _class_gateway(monkeypatch) -> Iterator[tuple[str, ThreadingHTTPServer, Mock]]:
    defer = Mock()
    monkeypatch.setattr("rdfsolve._http_policy.defer_host", defer)
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ClassGateway)
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", server, defer
    finally:
        server.shutdown()
        server.server_close()


def _class_context(helper: SparqlHelper, class_chunk_size: int | None = None) -> SimpleNamespace:
    def collect(query, purpose, size):
        return [
            row
            for page in helper.select_chunked(
                query, chunk_size=size or 10_000, purpose=purpose, max_page_retries=0
            )
            for row in page
        ]

    report = MagicMock()
    report.report.config = {}
    return SimpleNamespace(
        helper=helper,
        class_chunk_size=class_chunk_size,
        graph_uris=None,
        type_context_graph_uris=None,
        chunk_size=10_000,
        report=report,
        collect_bindings=collect,
    )


@pytest.mark.parametrize("class_chunk_size", [None, 1000])
def test_class_discovery_waits_out_an_overloaded_gateway(monkeypatch, class_chunk_size):
    """The plain listing and its paged fallback (or the paged listing) get a gateway error:
    the host is waited out (30 s) and the listing is sent again, instead of losing the source."""
    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

    with (
        _class_gateway(monkeypatch) as (base, _, defer),
        SparqlHelper(f"{base}/flaky", max_retries=1) as helper,
    ):
        classes = TwoPhaseStrategy()._discover_classes(_class_context(helper, class_chunk_size))
    assert classes == ["urn:A", "urn:B"]
    assert _waits(defer)[0] == 30.0


def test_class_discovery_stops_waiting_when_the_host_answers_a_trivial_query(monkeypatch):
    """pdbj.bmrb: the listing is cut by the gateway's timer every time, while ASK answers at
    once. One repeat is made; then the host is shown not to be overloaded, without the
    remaining waits. The samples are cut too: the listing is a gap (no class is mined, the
    source goes on), not an error that fails the source."""
    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

    with (
        _class_gateway(monkeypatch) as (base, server, defer),
        SparqlHelper(f"{base}/cut", max_retries=1) as helper,
    ):
        context = _class_context(helper)
        assert TwoPhaseStrategy()._discover_classes(context) == []
    assert _waits(defer) == [30.0]
    assert sum("ASK" in q for _, q in server.requests) == 2, "The wait, then the gap"
    assert context.report.report.config["class_listing"]["state"] == "refused"
    (outcome,) = [c.args[0] for c in context.report.record_outcome.call_args_list]
    assert outcome.state == "partial" and outcome.failures[0].purpose == "two-phase/classes"


@pytest.mark.parametrize("class_chunk_size", [None, 1000])
def test_a_class_listing_cut_every_time_is_read_over_a_sample(monkeypatch, class_chunk_size):
    """pdbj.bmrb (job 115591): every form of the class listing got the proxy's 502 and the
    source ended FAILED after 1295 s. The classes of a sample of the type statements (a LIMIT
    sub-select) are listed instead; they are mined, recorded as a lower bound."""
    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

    with (
        _class_gateway(monkeypatch) as (base, server, _),
        SparqlHelper(f"{base}/sampled", max_retries=1) as helper,
    ):
        context = _class_context(helper, class_chunk_size)
        context.discovered_classes = None
        assert TwoPhaseStrategy()._discover_classes(context) == ["urn:A"]
    sampled = [q for _, q in server.requests if "} LIMIT " in q]
    assert sampled and "LIMIT 100000" in sampled[0], "The first sample: 100k statements"
    listing = context.report.report.config["class_listing"]
    assert listing["state"] == "sampled" and listing["count_bound"] == "lower_bound"
    assert listing["sample"]["size"] == 100_000
    (outcome,) = [c.args[0] for c in context.report.record_outcome.call_args_list]
    assert outcome.failures[0].category == "sampled" and outcome.samples
    assert context.discovered_classes is None, "Not every class was listed"


def test_class_discovery_waits_while_the_host_answers_nothing(monkeypatch):
    """A host whose gateway refuses every query, even ASK, is waited out 4 times."""
    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
    from rdfsolve.sparql_helper import SparqlHelperError

    with (
        _class_gateway(monkeypatch) as (base, _, defer),
        SparqlHelper(f"{base}/down", max_retries=1) as helper,
        pytest.raises(SparqlHelperError),
    ):
        TwoPhaseStrategy()._discover_classes(_class_context(helper))
    assert _waits(defer) == [30.0, 60.0, 120.0, 240.0]


# STRING (job 115333): the ordered, paged graph listing reads every quad and is cut by the
# gateway; the unordered DISTINCT answers at once. forum (job 115328): the paged class listing
# read 89,375 classes, then a page got a 502 and every class read was thrown away.
FORUM_CLASSES = [f"urn:class{i:03d}" for i in range(50)]


class _Listings(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence the test server
        pass

    def _json(self, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/sparql-results+json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _refuse(self) -> None:
        self.send_response(502)
        self.send_header("Content-Length", str(len(PROXY_ERROR)))
        self.end_headers()
        self.wfile.write(PROXY_ERROR)

    def do_GET(self):
        path = urlsplit(self.path).path
        query = parse_qs(urlsplit(self.path).query).get("query", [""])[0]
        self.server.requests.append((path, query))
        offset = re.search(r"OFFSET\s+(\d+)", query)
        limit = re.search(r"LIMIT\s+(\d+)", query)
        if path == "/string":  # the ordered listing is cut; the unordered one answers
            if "ORDER BY" in query:
                self._refuse()
                return
            rows = [{"g": {"type": "uri", "value": g}} for g in ("urn:g2", "urn:g1")]
            self._json({"head": {"vars": ["g"]}, "results": {"bindings": rows}})
            return
        if path == "/many-graphs":  # 1500 graphs behind a cap of 1000 rows
            graphs = [f"urn:g{i:04d}" for i in range(1500)]
            start = int(offset.group(1)) if offset else 0
            end = start + int(limit.group(1)) if limit else len(graphs)
            rows = [{"g": {"type": "uri", "value": g}} for g in graphs[start:end][:1000]]
            self._json({"head": {"vars": ["g"]}, "results": {"bindings": rows}})
            return
        if path == "/forum":  # the plain listing is refused; pages from offset 20 are cut
            if not offset or int(offset.group(1)) >= 20:
                self._refuse()
                return
            start = int(offset.group(1))
            page = FORUM_CLASSES[start : start + int(limit.group(1))]
            rows = [{"class": {"type": "uri", "value": c}} for c in page]
            self._json({"head": {"vars": ["class"]}, "results": {"bindings": rows}})
            return
        self._refuse()


@contextmanager
def _listings(monkeypatch) -> Iterator[tuple[str, ThreadingHTTPServer, Mock]]:
    defer = Mock()
    monkeypatch.setattr("rdfsolve._http_policy.defer_host", defer)
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Listings)
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", server, defer
    finally:
        server.shutdown()
        server.server_close()


def test_the_graph_listing_is_sent_unordered_first(monkeypatch):
    """STRING: the unordered DISTINCT answers in one query, and the ordered form, which the
    gateway cuts, is never sent."""
    from rdfsolve.mining.graph_selection import discover_data_graphs

    with (
        _listings(monkeypatch) as (base, server, defer),
        SparqlHelper(f"{base}/string", max_retries=1) as helper,
    ):
        assert discover_data_graphs(helper) == ["urn:g1", "urn:g2"]
    assert len(server.requests) == 1 and "ORDER BY" not in server.requests[0][1]
    assert defer.call_count == 0


def test_a_graph_listing_at_a_server_cap_is_read_again_in_ordered_pages(monkeypatch):
    """An unordered answer of exactly 1000 graphs may be cut at a cap: the ordered pages are
    read, and every graph is listed."""
    from rdfsolve.void_retrieval import discover_graph_names

    with (
        _listings(monkeypatch) as (base, server, _),
        SparqlHelper(f"{base}/many-graphs", max_retries=1) as helper,
    ):
        names = discover_graph_names(helper, batch_size=500, max_pages=10)
    assert len(names) == 1500
    assert "ORDER BY" not in server.requests[0][1]
    assert all("ORDER BY" in q for _, q in server.requests[1:])


def _forum_context(helper: SparqlHelper, class_chunk_size: int | None) -> SimpleNamespace:
    from rdfsolve.sparql_helper import PaginationTruncatedError

    def collect(query, purpose, size):
        """The miner's _collect_bindings: the pages read travel with the truncation."""
        rows: list = []
        try:
            for page in helper.select_chunked(
                query, chunk_size=10, purpose=purpose, max_page_retries=0
            ):
                rows.extend(page)
        except PaginationTruncatedError as error:
            error.partial_rows = rows + error.partial_rows
            raise
        return rows

    report = Mock()
    report.report.config = {}
    return SimpleNamespace(
        helper=helper,
        class_chunk_size=class_chunk_size,
        graph_uris=None,
        type_context_graph_uris=None,
        chunk_size=10,
        report=report,
        collect_bindings=collect,
    )


@pytest.mark.parametrize("class_chunk_size", [None, 10])
def test_a_class_listing_cut_mid_way_keeps_the_classes_read(monkeypatch, class_chunk_size):
    """forum: the pages read before the failing page are kept and mined; the listing is
    recorded as truncated at its offset (the source ends PARTIAL), and the host is not waited
    out, since the classes read are the answer."""
    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

    with (
        _listings(monkeypatch) as (base, _, defer),
        SparqlHelper(f"{base}/forum", max_retries=1) as helper,
    ):
        context = _forum_context(helper, class_chunk_size)
        classes = TwoPhaseStrategy()._discover_classes(context)
    assert classes == FORUM_CLASSES[:20]
    (outcome,) = context.report.record_outcome.call_args.args
    assert outcome.state == "partial" and outcome.failures[0].category == "truncated"
    assert "offset 20" in outcome.failures[0].message
    listing = context.report.report.config["class_listing"]
    assert listing["state"] == "truncated" and listing["offset"] == 20
    assert listing["rows_read"] == 20 and listing["count_bound"] == "lower_bound"
    assert defer.call_count == 0
