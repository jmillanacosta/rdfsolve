"""rdfsolve.sparql_helper paging: HTTP limits and cooldowns, grouped cursor paging, and paged
selects."""

import gzip
import json
import re
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from time import monotonic, sleep

import pytest
from rdflib import Graph, Literal, URIRef

from rdfsolve import _http_policy, sparql_helper
from rdfsolve._host_gate import HostBusyError, host_request
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointTimeoutError,
    QueryError,
    ResponseLimitError,
    SparqlHelper,
)


def test_decompressed_response_limit_does_not_retry(monkeypatch):
    monkeypatch.setattr(_http_policy, "_next_request", {})
    original = (Path(__file__).parent / "test_data" / "aopwikirdf_generated_void.ttl").read_bytes()
    compressed = gzip.compress(original)
    limit = len(compressed) + 100
    assert limit < len(original)
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            sleep(0.1)
            self.send_response(200)
            self.send_header("Content-Type", "text/turtle; charset=utf-8")
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(compressed)))
            self.end_headers()
            sleep(0.1)
            self.wfile.write(compressed)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with SparqlHelper(
            f"http://127.0.0.1:{server.server_port}", timeout=0.02, max_response_bytes=limit
        ) as helper:
            helper.enable_query_collection()
            with pytest.raises(ResponseLimitError):
                helper._execute("CONSTRUCT WHERE { ?s ?p ?o }", "text/turtle", parse_json=False)
            assert len(calls) == 1
            record = helper.get_collected_queries()[0]
            assert record.elapsed_seconds >= record.request_seconds > 0
            assert record.wait_seconds >= 0
            elapsed = record.request_seconds
            helper.max_response_bytes = len(original)
            assert (
                helper._get_query("CONSTRUCT WHERE { ?s ?p ?o }", "text/turtle")
                == original.decode()
            )
            assert record.request_seconds == elapsed
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_silent_servers_end_reads_by_option_and_dead_peers_are_probed(monkeypatch):
    monkeypatch.setattr(_http_policy, "_next_request", {})

    class Silent(BaseHTTPRequestHandler):
        def do_GET(self):
            sleep(3)  # longer than the read timeout below

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Silent)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        with SparqlHelper(
            url, timeout=1, read_timeout=0.3, max_retries=1, initial_backoff=0.01
        ) as helper:
            started = monotonic()
            with pytest.raises(EndpointError):
                helper.ask("ASK {}")
            assert monotonic() - started < 2.5, "A silent server does not hold the read"
            options = helper._session.get_adapter(url).poolmanager.connection_pool_kw[
                "socket_options"
            ]
            assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options, "Dead peers are probed"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class Trickle(BaseHTTPRequestHandler):
    """Send a byte every 0.2 s, in the headers or in the body, for up to 30 s."""

    in_headers = False

    def do_GET(self):
        try:
            if self.in_headers:
                self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                for byte in b"X-Slow: " + b"x" * 150:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    sleep(0.2)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/sparql-results+json")
            self.end_headers()  # no length: the body ends when the connection closes
            self.wfile.write(b'{"head": {"vars": []}, "results": {"bindings": [')
            for _ in range(150):
                self.wfile.write(b" ")
                self.wfile.flush()
                sleep(0.2)
            self.wfile.write(b"]}}")
        except OSError:
            pass

    def log_message(self, *args):
        pass


class TrickledHeaders(Trickle):
    in_headers = True


@pytest.mark.parametrize("handler", [Trickle, TrickledHeaders], ids=["body", "headers"])
def test_a_server_that_trickles_bytes_is_ended_at_the_deadline(monkeypatch, handler):
    monkeypatch.setattr(_http_policy, "_next_request", {})
    monkeypatch.setattr(sparql_helper, "DEADLINE_GRACE_S", 0.5)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with SparqlHelper(
            f"http://127.0.0.1:{server.server_port}", timeout=5, max_retries=1
        ) as helper:
            helper.enable_query_collection()
            assert helper.read_timeout is None, "Only the deadline can end this read"
            # Each silence (0.2 s) is shorter than the read timeout of the budget (1 s).
            started = monotonic()
            with (
                helper.budget(1.0),
                pytest.raises(EndpointTimeoutError, match=r"deadline of 1\.5 s"),
            ):
                helper.select("SELECT * WHERE { ?s ?p ?o }", purpose="trickle")
            assert monotonic() - started < 5, "The deadline ends the request, not the server"
            record = helper.get_collected_queries()[-1]
            assert record.error == "EndpointTimeoutError" and record.cut is None, (
                "The client's own limit is not a cut of the endpoint"
            )
            assert helper.deadline is None and helper.read_timeout is None, "Budget restored"
            helper.deadline = 0.5
            started = monotonic()
            with pytest.raises(EndpointTimeoutError):
                helper.select("SELECT * WHERE { ?s ?p ?o }", purpose="trickle")
            assert monotonic() - started < 4, "An explicit deadline holds outside budgets"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_requests_through_an_http_proxy_are_watched_too():
    adapter = SparqlHelper("https://example.org/sparql")._session.get_adapter("https://x")
    pools = adapter.proxy_manager_for("http://proxy.example.org:3128").pool_classes_by_scheme
    assert pools["https"] is sparql_helper._WatchedHTTPSConnectionPool
    assert adapter.poolmanager.pool_classes_by_scheme["http"] is (
        sparql_helper._WatchedHTTPConnectionPool
    )


def test_a_cost_limit_in_a_client_error_page_is_a_timeout(monkeypatch):
    monkeypatch.setattr(_http_policy, "_next_request", {})
    calls = []
    page = (
        b'<!DOCTYPE html SYSTEM "about:legacy-compat"><html><head><title>Endpoint</title>'
        b"</head><body><p>Query evaluation exception. { &quot;exception&quot;: &quot;Tried to "
        b"allocate 397.1 GB, but only 90.6 GB were available&quot;, &quot;query&quot;: "
        b"&quot;SELECT ...&quot; }</p></body></html>"
    )

    class Refusing(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.command)
            self.send_response(400)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)

        def do_POST(self):
            self.do_GET()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Refusing)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with (
            SparqlHelper(f"http://127.0.0.1:{server.server_port}", max_retries=1) as helper,
            pytest.raises(EndpointTimeoutError, match=r"Tried to allocate 397\.1 GB"),
        ):
            helper.select("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }")
        assert calls == ["GET"], "The same query is not sent again by POST"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_a_cooldown_that_the_server_asks_for_has_its_own_wait_budget(monkeypatch):
    monkeypatch.setattr(_http_policy, "_next_request", {})
    _http_policy.defer_host("example.org", 1.0)  # as after a 429 with Retry-After: 1
    with pytest.raises(HostBusyError), host_request("example.org", timeout=0.1):
        pass
    started = monotonic()
    with host_request("example.org", timeout=0.1, cooldown_wait=3):
        pass
    assert monotonic() - started >= 0.8, "The cooldown is waited out, not failed"
    assert SparqlHelper("https://example.org/sparql").rate_limit_wait >= 60, "Its own budget"


GRAPH = Graph()
for i in range(7):
    for j in range(i + 1):
        GRAPH.add((URIRef(f"urn:s{j}"), URIRef("urn:p"), URIRef(f"urn:o{i}")))


class SortLimitedEndpoint(SparqlHelper):
    """Answer from GRAPH; refuse the unpaged query and any page past offset 2, as Virtuoso does."""

    def select(self, query, purpose=""):
        offset = re.search(r"OFFSET\s+(\d+)", query)
        if offset is None and "__rdfsolve_cursor" not in query:
            raise EndpointTimeoutError("Query cost/time limit: Virtuoso S1T00 Error SR171")
        if offset and int(offset[1]) + 2 > 2:
            raise EndpointTimeoutError(
                "Query cost/time limit: Virtuoso 22023 Error SR353: Sorted TOP clause specifies"
                " more then 10001 rows to sort. Only 10000 are allowed."
            )
        return json.loads(GRAPH.query(query).serialize(format="json"))


def test_a_grouped_query_is_paged_by_cursor_after_the_sort_limit(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    helper = SortLimitedEndpoint("https://example.org/sparql")
    helper.inter_request_delay = helper.select_page_cooldown = 0
    helper.select_page_size = 2
    helper.select_page_retries = 0
    query = "SELECT ?o (COUNT(*) AS ?n) WHERE { ?s <urn:p> ?o } GROUP BY ?o"
    rows = helper.select_with_fallback(query, purpose="structural/objects")["results"]["bindings"]
    counts = {r["o"]["value"]: int(r["n"]["value"]) for r in rows}
    assert counts == {f"urn:o{i}": i + 1 for i in range(7)}, "Every group, once"
    assert helper.last_select_execution["strategy"] == "cursor_recovery"


@pytest.fixture
def paged(monkeypatch):
    """A helper whose requests run on a local graph, two rows per page."""
    graph = Graph()
    for i in range(5):
        for j in range(i + 1):
            graph.add((URIRef(f"urn:s{j}"), URIRef("urn:p"), Literal(f"k{i}")))
    helper = SparqlHelper("http://example.invalid/sparql", inter_request_delay=0)
    helper.select_page_size = 2
    sent = []

    def select(query, **_):
        sent.append(query)
        return json.loads(graph.query(query).serialize(format="json"))

    monkeypatch.setattr(helper, "select", select)
    return helper, sent


def test_grouped_pages_are_ordered_by_group_keys_not_by_aggregates(paged):
    helper, sent = paged
    query = (
        "SELECT ?k (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s <urn:p> ?k } "
        "GROUP BY ?k HAVING (COUNT(DISTINCT ?s) > 1)"
    )
    rows = helper.select_with_fallback(query, exhaustive=True)["results"]["bindings"]
    assert {r["k"]["value"]: r["n"]["value"] for r in rows} == {
        "k1": "2",
        "k2": "3",
        "k3": "4",
        "k4": "5",
    }
    assert len(sent) == 3, "Two full pages and one empty page"
    assert all("?n" not in q.split("ORDER BY")[1] for q in sent), (
        "Virtuoso refuses aggregate aliases"
    )


def test_one_group_needs_no_order_and_unnamed_group_keys_cannot_be_paged(paged):
    helper, sent = paged
    rows = helper.select_with_fallback(
        "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?k }", exhaustive=True
    )["results"]["bindings"]
    assert [r["c"]["value"] for r in rows] == ["15"]
    assert "ORDER BY" not in sent[0]
    named = "SELECT ?g (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?k } GROUP BY (STR(?k) AS ?g)"
    assert len(helper.select_with_fallback(named, exhaustive=True)["results"]["bindings"]) == 5
    with pytest.raises(QueryError, match="GROUP BY"):
        helper.select_with_fallback(
            "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?k } GROUP BY STR(?k)", exhaustive=True
        )
