"""Check HTTP resource limits with the saved AOPWiki RDF fixture."""

import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Thread

import pytest
import requests

from rdfsolve import _http_policy
from rdfsolve.sparql_helper import EndpointRateLimitError, ResponseLimitError, SparqlHelper


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
            self.send_response(200)
            self.send_header("Content-Type", "text/turtle; charset=utf-8")
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(compressed)))
            self.end_headers()
            self.wfile.write(compressed)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with SparqlHelper(f"http://127.0.0.1:{server.server_port}", max_response_bytes=limit) as helper:
            helper.enable_query_collection()
            with pytest.raises(ResponseLimitError):
                helper._execute("CONSTRUCT WHERE { ?s ?p ?o }", "text/turtle", parse_json=False)
            assert len(calls) == 1
            record = helper.get_collected_queries()[0]
            assert record.elapsed_seconds >= record.request_seconds > 0
            assert record.wait_seconds >= 0
            elapsed = record.request_seconds
            helper.max_response_bytes = len(original)
            assert helper._get_query("CONSTRUCT WHERE { ?s ?p ?o }", "text/turtle") == original.decode()
            assert record.request_seconds == elapsed
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_remote_retry_after_applies_to_other_helpers(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    monkeypatch.setattr(_http_policy, "_next_request", {})
    calls = []

    def limited(*args, **kwargs):
        calls.append(args)
        response = requests.Response()
        response.status_code = 429
        response.headers["Retry-After"] = "60"
        response.raw = BytesIO(b"")
        return response

    with SparqlHelper("https://example.org/one", timeout=0.01) as first:
        monkeypatch.setattr(first._session, "request", limited)
        with pytest.raises(EndpointRateLimitError):
            first.select("SELECT * WHERE {}")
    with SparqlHelper("https://example.org/two", timeout=0.01) as second:
        monkeypatch.setattr(second._session, "request", limited)
        with pytest.raises(EndpointRateLimitError):
            second.select("SELECT * WHERE {}")
    assert len(calls) == 1
