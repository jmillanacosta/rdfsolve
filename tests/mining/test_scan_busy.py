"""rdfsolve.mining.scan and qlever.lifecycle: a local server whose query slots are all taken
answers 429; the export sends the query again, and the server gets slots for every stream."""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from rdfsolve.mining import scan
from rdfsolve.qlever import lifecycle


def test_a_busy_server_is_asked_again(monkeypatch):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            calls.append(1)
            self.send_response(429 if len(calls) < 3 else 200)
            self.end_headers()
            self.wfile.write(b"?s\n")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(scan.time, "sleep", lambda seconds: None)
    try:
        with scan._post(f"http://127.0.0.1:{server.server_port}", "ASK {}", "text/plain") as r:
            assert r.read() == b"?s\n"
    finally:
        server.shutdown()
    assert len(calls) == 3


@pytest.mark.parametrize(("workers", "slots"), [("1", 4), ("4", 10), ("16", 34)])
def test_the_server_has_two_slots_per_export_stream(monkeypatch, workers, slots):
    monkeypatch.setenv("RDFSOLVE_SCAN_WORKERS", workers)
    assert lifecycle.simultaneous_queries() == slots
