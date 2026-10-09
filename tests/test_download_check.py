"""rdfsolve.download_check: every download URL of a registry is asked whether it exists (L28:
CoXPresdb's entry named files its Zenodo record does not hold; the download left an empty file)."""

import http.server
import threading

import pytest

from rdfsolve.download_check import (
    check_download,
    check_downloads,
    registry_downloads,
    source_status,
)


class Handler(http.server.BaseHTTPRequestHandler):
    busy = 1

    def log_message(self, *args):
        pass

    def _answer(self, body: bool):
        path = self.path
        if path == "/busy" and Handler.busy:
            Handler.busy -= 1
            self.send_response(429)
            self.send_header("Retry-After", "0")
            self.end_headers()
            return
        if path == "/nohead" and self.command == "HEAD":
            self.send_response(405)
            self.end_headers()
            return
        sizes = {"/data.nt": 10, "/empty.nt": 0, "/nohead": 5, "/busy": 3, "/folder/": 7}
        if path == "/moved.nt":
            self.send_response(302)
            self.send_header("Location", "/data.nt")
            self.end_headers()
            return
        if path not in sizes:
            self.send_response(404)
            self.end_headers()
            return
        size = sizes[path]
        if self.headers.get("Range") and size:
            self.send_response(206)
            self.send_header("Content-Range", f"bytes 0-0/{size}")
            self.send_header("Content-Length", "1")
            self.end_headers()
            if body:
                self.wfile.write(b"x")
            return
        self.send_response(200)
        self.send_header("Content-Length", str(size))
        self.end_headers()
        if body:
            self.wfile.write(b"x" * size)

    def do_HEAD(self):
        self._answer(False)

    def do_GET(self):
        self._answer(True)


@pytest.fixture
def site(monkeypatch):
    for name in ("no_proxy", "NO_PROXY"):
        monkeypatch.setenv(name, "127.0.0.1,localhost")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def test_each_download_is_ok_empty_missing_or_unreachable(site):
    status = {
        path: check_download(site + path, interval=0, timeout=5)
        for path in ("/data.nt", "/empty.nt", "/gone.nt", "/moved.nt", "/folder/")
    }
    assert status["/data.nt"].status == "ok" and status["/data.nt"].content_length == 10
    assert status["/empty.nt"].status == "empty"
    assert status["/gone.nt"].status == "missing" and status["/gone.nt"].http_status == 404
    assert status["/moved.nt"].status == "ok" and status["/moved.nt"].final_url == site + "/data.nt"
    assert status["/folder/"].status == "ok", "A published folder: its listing answers"
    closed = check_download("http://127.0.0.1:9/data.nt", interval=0, timeout=5)
    assert closed.status == "unreachable" and closed.error


def test_a_server_that_refuses_head_is_asked_for_one_byte(site):
    check = check_download(site + "/nohead", interval=0, timeout=5)
    assert check.status == "ok" and check.method == "GET" and check.content_length == 5


def test_a_server_that_asks_for_time_is_asked_again(site):
    Handler.busy = 1
    check = check_download(site + "/busy", interval=0, timeout=5)
    assert check.status == "ok" and check.http_status == 200


def test_every_download_of_the_registry_is_checked_once_and_reported_per_source(site):
    entries = [
        {"name": "a", "download_nt": site + "/data.nt", "download_ttl": [site + "/gone.nt"]},
        {
            "name": "b",
            "graph_sources": {"urn:g": {"download_rdf": [site + "/data.nt"]}},
            "local_tar_url": site + "/empty.nt",
        },
    ]
    items = list(registry_downloads(entries))
    assert [(s, f) for s, f, _ in items] == [
        ("a", "download_nt"),
        ("a", "download_ttl"),
        ("b", "graph:urn:g/download_rdf"),
        ("b", "local_tar_url"),
    ]
    checks = check_downloads(items, interval=0, timeout=5)
    assert [c.status for c in checks] == ["ok", "missing", "ok", "empty"]
    report = source_status(checks)
    assert report["a"]["status"] == "missing" and report["a"]["failing"] == [site + "/gone.nt"]
    assert report["b"]["status"] == "empty" and report["b"]["counts"] == {"ok": 1, "empty": 1}
