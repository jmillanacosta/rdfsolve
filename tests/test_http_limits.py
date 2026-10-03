import gzip
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from time import monotonic, sleep

import pytest
from rdfsolve.sparql_helper import EndpointError, ResponseLimitError, SparqlHelper

from rdfsolve import _http_policy
from rdfsolve._host_gate import HostBusyError, host_request


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
        with SparqlHelper(url, timeout=1, read_timeout=0.3, max_retries=1, initial_backoff=0.01) as helper:
            started = monotonic()
            with pytest.raises(EndpointError):
                helper.ask("ASK {}")
            assert monotonic() - started < 2.5, "A silent server does not hold the read"
            options = helper._session.get_adapter(url).poolmanager.connection_pool_kw["socket_options"]
            assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options, "Dead peers are probed"
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
