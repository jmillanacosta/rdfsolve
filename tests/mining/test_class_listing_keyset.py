"""Class listings read by key (keyset paging) against a local SPARQL endpoint backed by
rdflib: the same classes as OFFSET paging, and the switch to keys when an OFFSET page fails
deep in the listing (forum, job 115328: pages beyond offset 80,000 took about 5 min and got
HTTP 502)."""

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
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF

from rdfsolve.mining.query_builders import _build_class_discovery_query
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
from rdfsolve.sparql_helper import SparqlHelper

# Classes whose keys sort in different orders as IRIs, as strings and as encoded keys, with
# characters that a FILTER on a key must compare correctly, and type values that are not IRIs.
CLASSES = [f"http://example.org/C{i}" for i in range(40)] + [
    "http://example.org/Ä",
    "http://example.org/a%20b",
    "http://example.org/q'uote",
    "http://example.org/z",
    "urn:x:1",
    "urn:x:10",
    "urn:x:2",
    "https://example.org/S",
]
LITERAL_TYPES = [
    Literal("strain"),
    Literal("strain", lang="en"),
    Literal("7", datatype=RDF.XMLLiteral),
]
TYPE_VALUES = len(CLASSES) + len(LITERAL_TYPES)


def _graph() -> Graph:
    graph = Graph()
    for i, cls in enumerate(CLASSES):
        for j in range(3):  # several members: DISTINCT matters
            subject = URIRef(f"http://example.org/s{i}-{j}")
            graph.add((subject, RDF.type, URIRef(cls)))
            graph.add((subject, URIRef("http://example.org/p"), Literal(j)))
    for i, value in enumerate(LITERAL_TYPES):
        graph.add((URIRef(f"http://example.org/l{i}"), RDF.type, value))
    return graph


class _Endpoint(BaseHTTPRequestHandler):
    """Answers SELECT with rdflib. With a depth, an OFFSET page beyond it gets "502 Proxy
    Error" (an engine that pays for every row skipped); "refuse_keys" refuses the key pages
    after the first; the plain listing is refused when "paged_only" is set."""

    def log_message(self, *args):  # silence the test server
        pass

    def _refuse(self) -> None:
        body = b"<html><title>502 Proxy Error</title>Error reading from remote server</html>"
        self.send_response(502)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        query = parse_qs(urlsplit(self.path).query)["query"][0]
        server = self.server
        server.queries.append(query)
        offset = re.search(r"OFFSET\s+(\d+)", query)
        keyed = "__rdfsolve_cursor" in query
        if server.paged_only and "LIMIT" not in query:
            self._refuse()
            return
        if offset and server.depth is not None and int(offset.group(1)) >= server.depth:
            self._refuse()
            return
        if keyed and server.refuse_keys and "FILTER(true)" not in query.replace(" ", ""):
            self._refuse()
            return
        result = server.graph.query(query)
        body = result.serialize(format="json")
        self.send_response(200)
        self.send_header("Content-Type", "application/sparql-results+json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@contextmanager
def _endpoint(
    monkeypatch, *, depth: int | None = None, refuse_keys: bool = False, paged_only: bool = False
) -> Iterator[tuple[str, ThreadingHTTPServer]]:
    monkeypatch.setattr("rdfsolve._http_policy.wait_for_host", lambda *args: True)
    monkeypatch.setattr("rdfsolve._http_policy.defer_host", Mock())
    # No cooling pause after a refused page (select_chunked's wait_after_timeout).
    monkeypatch.setattr("rdfsolve.sparql_helper.time.sleep", lambda seconds: None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Endpoint)
    server.graph, server.queries = _graph(), []
    server.depth, server.refuse_keys, server.paged_only = depth, refuse_keys, paged_only
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/sparql", server
    finally:
        server.shutdown()
        server.server_close()


def _key(row: dict) -> str:
    return json.dumps(row, sort_keys=True)


def _read(helper: SparqlHelper, size: int, pagination: str) -> list[dict]:
    rows: list[dict] = []
    for page in helper.select_chunked(
        _build_class_discovery_query(None),
        chunk_size=size,
        delay_between_chunks=0.0,
        pagination=pagination,  # type: ignore[arg-type]
        cursor_keys=["class"] if pagination == "cursor" else None,
    ):
        rows.extend(page)
    return rows


@pytest.mark.parametrize("size", [1, 7, 10, 1000])
def test_keyset_pages_list_the_same_classes_as_offset_pages(monkeypatch, size):
    """Proof on a local endpoint: keyset and OFFSET pages give the same rows, each once, and
    the rows of the unpaged listing, for IRIs, odd characters and literal type values."""
    with _endpoint(monkeypatch) as (url, _), SparqlHelper(url, max_retries=1) as helper:
        whole = helper.select("SELECT DISTINCT ?class WHERE { ?s a ?class }")["results"]
        by_offset = _read(helper, size, "offset")
        by_key = _read(helper, size, "cursor")
    expected = sorted(_key(row) for row in whole["bindings"])
    assert len(expected) == TYPE_VALUES
    assert sorted(_key(row) for row in by_offset) == expected
    assert sorted(_key(row) for row in by_key) == expected


def _context(helper: SparqlHelper, class_chunk_size: int | None, pagination: str = "offset"):
    from rdfsolve.sparql_helper import PaginationTruncatedError

    def collect(query, purpose, size):
        """The miner's _collect_bindings: the pages read travel with a truncation."""
        rows: list = []
        try:
            for page in helper.select_chunked(
                query,
                chunk_size=size or 10,
                purpose=purpose,
                delay_between_chunks=0.0,
                pagination=pagination,
                cursor_keys=["class"] if pagination == "cursor" else None,
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
        pagination=pagination,
    )


@pytest.mark.parametrize("class_chunk_size", [None, 10])
def test_a_deep_offset_failure_switches_the_class_listing_to_keys(monkeypatch, class_chunk_size):
    """forum: OFFSET pages from offset 20 are refused. The listing is read again by key and is
    complete: every class, no truncation recorded, and the switch is in the report."""
    with (
        _endpoint(monkeypatch, depth=20, paged_only=True) as (url, server),
        SparqlHelper(url, max_retries=1, initial_backoff=0) as helper,
    ):
        context = _context(helper, class_chunk_size)
        classes = TwoPhaseStrategy()._discover_classes(context)
    assert sorted(classes) == sorted(CLASSES)
    assert context.skipped_type_values == len(LITERAL_TYPES)
    context.report.record_outcome.assert_not_called()
    listing = context.report.report.config["class_listing"]
    assert listing["state"] == "complete" and listing["read_by"] == "keyset"
    assert listing["offset_failure"]["offset"] == 20
    assert any("__rdfsolve_cursor" in q for q in server.queries)


def test_when_keys_fail_too_the_classes_read_by_both_are_kept(monkeypatch):
    """OFFSET pages fail at 20 and key pages after the first: the rows read by both are kept
    once each, and the listing is recorded as truncated, with what the keys read."""
    with (
        _endpoint(monkeypatch, depth=20, refuse_keys=True, paged_only=True) as (url, _),
        SparqlHelper(url, max_retries=1, initial_backoff=0) as helper,
    ):
        context = _context(helper, 10)
        classes = TwoPhaseStrategy()._discover_classes(context)
    assert len(classes) == len(set(classes)) and set(classes) < set(CLASSES)
    (outcome,) = context.report.record_outcome.call_args.args
    assert outcome.state == "partial" and outcome.failures[0].category == "truncated"
    listing = context.report.report.config["class_listing"]
    assert listing["state"] == "truncated" and listing["keyset"]["state"] == "truncated"
    assert listing["keyset"]["rows_read"] == 10


def test_a_failure_on_the_first_page_is_not_read_by_key(monkeypatch):
    """Only a failure beyond the first page is the cost of skipping rows; a listing refused at
    once is not read by key (the gateway wait applies). It is read over a sample of the type
    statements instead of failing the source (pdbj.bmrb, job 115591)."""
    with (
        _endpoint(monkeypatch, depth=0, paged_only=True) as (url, server),
        SparqlHelper(url, max_retries=1, initial_backoff=0) as helper,
    ):
        context = _context(helper, 10)
        classes = TwoPhaseStrategy()._discover_classes(context)
    assert not any("__rdfsolve_cursor" in q for q in server.queries)
    assert sorted(classes) == sorted(CLASSES), "The sample holds every type statement here"
    assert context.report.report.config["class_listing"]["state"] == "sampled"


def test_a_cursor_run_does_not_read_the_listing_twice(monkeypatch):
    """With --pagination cursor the listing is already read by key: a failure is not
    repeated by key; the rows read are kept as a truncated listing."""
    with (
        _endpoint(monkeypatch, refuse_keys=True, paged_only=True) as (url, _),
        SparqlHelper(url, max_retries=1, initial_backoff=0) as helper,
    ):
        context = _context(helper, 10, pagination="cursor")
        classes = TwoPhaseStrategy()._discover_classes(context)
    assert len(classes) <= 10
    assert context.report.report.config["class_listing"]["state"] == "truncated"
    assert "keyset" not in context.report.report.config["class_listing"]
