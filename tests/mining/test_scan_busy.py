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


def test_a_result_cut_while_sending_is_refused():
    cut = b"?s\n<urn:a>\n<urn:b\n!!!!>># An error has occurred while exporting the query result.\n"
    with pytest.raises(RuntimeError, match="cut the result"):
        scan._check_trailer(cut, "SELECT ?s WHERE { ?s ?p ?o }")
    scan._check_trailer(b"?s\n<urn:a>\n", "SELECT ?s WHERE { ?s ?p ?o }")


def test_a_query_the_server_timed_out_is_not_sent_again(monkeypatch):
    """QLever answers 429 to a query stopped at its time limit; that is not a busy server."""
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            calls.append(1)
            self.send_response(429)
            self.end_headers()
            self.wfile.write(b'{"exception": "Operation timed out. Last operation: Sort"}')

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(scan.time, "sleep", lambda seconds: None)
    try:
        with pytest.raises(scan.QueryTimeoutError):
            scan._post(f"http://127.0.0.1:{server.server_port}", "ASK {}", "text/plain")
    finally:
        server.shutdown()
    assert len(calls) == 1


def _fake_counts(monkeypatch, graphs, by_predicate):
    """Answer the count queries of _graph_counts: the grouped one times out."""
    import polars as pl

    asked = []

    def counts(endpoint, query, path):
        asked.append(query)
        if "GROUP BY ?g ?p" in query:
            raise scan.QueryTimeoutError(query)
        predicate = query.split("<")[1].split(">")[0]
        return [((f"<{g}>",), n) for g, n in by_predicate[predicate].items()]

    def listing(endpoint, query, path):
        asked.append(query)
        pl.DataFrame({"g": [f"<{g}>" for g in graphs]}).write_parquet(path)
        return len(graphs)

    monkeypatch.setattr(scan, "_counts", counts)
    monkeypatch.setattr(scan, "_tsv_to_parquet", listing)
    return asked


def test_one_graph_takes_the_counts_by_predicate(monkeypatch, tmp_path):
    asked = _fake_counts(monkeypatch, ["urn:g"], {})
    sizes = {"urn:p": 3, "urn:q": 5}
    assert scan._graph_counts("x", tmp_path / "c.parquet", sizes, {}) == {"urn:g": sizes}
    assert len(asked) == 2, "The grouped count and the list of graphs"


@pytest.mark.parametrize(
    ("graphs", "unnamed"), [(["urn:g", "urn:h"], {}), (["urn:g"], {"urn:p": 1})]
)
def test_several_graphs_are_counted_by_predicate(monkeypatch, tmp_path, graphs, unnamed):
    by_predicate = {"urn:p": {"urn:g": 2, "urn:h": 1}, "urn:q": {"urn:h": 5}}
    _fake_counts(monkeypatch, graphs, by_predicate)
    sizes = {"urn:p": 3, "urn:q": 5}
    assert scan._graph_counts("x", tmp_path / "c.parquet", sizes, unnamed) == {
        "urn:g": {"urn:p": 2},
        "urn:h": {"urn:p": 1, "urn:q": 5},
    }


def test_a_count_by_graph_that_times_out_is_made_in_slices(monkeypatch, tmp_path):
    """The count of one predicate by graph is summed over slices of its scan."""
    scan_rows = ["urn:g"] * 5 + ["urn:h"] * 4  # the graph of each row of urn:p, in scan order

    def counts(endpoint, query, path):
        if "LIMIT" not in query and "OFFSET" not in query:
            raise scan.QueryTimeoutError(query)
        offset = int(query.split(" OFFSET ")[1].split()[0])
        limit = int(query.split(" LIMIT ")[1].split()[0]) if " LIMIT " in query else None
        if limit is None and offset < 6:
            raise scan.QueryTimeoutError(query)
        selected = scan_rows[offset : None if limit is None else offset + limit]
        return [((f"<{g}>",), selected.count(g)) for g in sorted(set(selected))]

    monkeypatch.setattr(scan, "_counts", counts)
    monkeypatch.setattr(scan, "SPLIT_ROWS", 1)
    found = scan._predicate_graph_counts("x", tmp_path / "c.parquet", "urn:p", 0, None, 8)
    assert found == {"<urn:g>": 5, "<urn:h>": 4}, "The open last slice reads beyond the count"


def test_the_client_holds_no_more_queries_than_its_slots(monkeypatch):
    """Each open response holds a slot until closed; a failed query gives its slot back; a busy
    server is waited on for BUSY_WAIT after the last query of the process ended."""
    import urllib.error

    busy = [True]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(429 if busy[0] else 200)
            self.end_headers()
            self.wfile.write(b"no free slot" if busy[0] else b"?s\n")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setenv("RDFSOLVE_SCAN_WORKERS", "1")
    monkeypatch.setattr(scan, "BUSY_WAIT", 0.0)
    monkeypatch.setattr(scan.time, "sleep", lambda seconds: None)
    try:
        with pytest.raises(urllib.error.HTTPError):
            scan._post(url, "ASK {}", "text/plain")
        busy[0] = False
        first = scan._post(url, "ASK {}", "text/plain")
        second = scan._post(url, "ASK {}", "text/plain")
        slots = scan._query_slots()
        assert not slots.acquire(blocking=False), "Two slots for one stream, both taken"
        first.close()
        assert slots.acquire(blocking=False)
        slots.release()
        second.close()
    finally:
        server.shutdown()
