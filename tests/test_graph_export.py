"""Export the graphs of a SPARQL endpoint: an Oxigraph store behind a local HTTP server."""

from __future__ import annotations

import gzip
import io
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlsplit

import pyoxigraph
import pytest

from rdfsolve.graph_export import GraphExporter, count_file_triples, source_complete

EX = "http://example.org/"


class _Endpoint(BaseHTTPRequestHandler):
    """Answer SPARQL from an Oxigraph store; cut CONSTRUCT results as Virtuoso does."""

    store: ClassVar[pyoxigraph.Store]
    row_cap: ClassVar[int | None] = None
    content_type: ClassVar[str] = "application/n-triples"
    busy: ClassVar[int] = 0
    hide: ClassVar[str] = ""
    garbage: ClassVar[bytes] = b""
    refuse_accept: ClassVar[str] = ""
    refuse: ClassVar[tuple[str, ...]] = ()
    queries: ClassVar[list[str]] = []
    # Text of a CONSTRUCT body written otherwise (an engine's serialization of a value).
    substitute: ClassVar[tuple[bytes, bytes]] = (b"", b"")
    # A CONSTRUCT that holds this text is cut halfway: after a whole line, with TRAILER
    # (a server's message), or, without a trailer, mid-statement.
    cut_on: ClassVar[str] = ""
    trailer: ClassVar[bytes] = b""

    def log_message(self, *args: Any) -> None:
        """Keep the test output quiet."""

    def do_GET(self) -> None:
        """Answer one query."""
        query = parse_qs(urlsplit(self.path).query)["query"][0]
        type(self).queries.append(query)
        if "CONSTRUCT" in query and self.headers.get("Accept") == self.refuse_accept:
            self._send(406, "text/plain", b"Not Acceptable")
            return
        if "CONSTRUCT" in query and type(self).busy > 0:
            type(self).busy -= 1
            self._send(502, "text/plain", b"Bad gateway")
            return
        if self.hide and self.hide in query and "COUNT" not in query:
            query = query.replace(self.hide, "<http://example.org/nothing>")
        if any(word in query for word in self.refuse):
            self._send(500, "text/plain", b"Virtuoso 42000 Error: refused")
            return
        result = self.store.query(query)
        if isinstance(result, pyoxigraph.QueryTriples):
            triples = list(result)
            if self.row_cap is not None:
                # Virtuoso answers ResultSetMaxRows + 1 rows.
                triples = triples[: self.row_cap + 1]
            body = io.BytesIO()
            pyoxigraph.serialize(triples, body, pyoxigraph.RdfFormat.N_TRIPLES)
            garbage = self.garbage if "g1" in query else b""
            data = body.getvalue()
            if self.substitute[0]:
                data = data.replace(*self.substitute)
            if self.cut_on and self.cut_on in query:
                half = data.rfind(b"\n", 0, len(data) // 2) + 1
                data = data[:half] + self.trailer if self.trailer else data[: half + 12]
            self._send(200, self.content_type, data + garbage)
            return
        if isinstance(result, pyoxigraph.QueryBoolean):
            self._send(
                200,
                "application/sparql-results+json",
                json.dumps({"boolean": bool(result)}).encode(),
            )
            return
        body = io.BytesIO()
        result.serialize(body, pyoxigraph.QueryResultsFormat.JSON)
        self._send(200, "application/sparql-results+json", body.getvalue())

    def _send(self, status: int, media: str, body: bytes) -> None:
        self.send_response(status)
        if media:
            self.send_header("Content-Type", media)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _store() -> pyoxigraph.Store:
    store = pyoxigraph.Store()
    data = []
    for g, n in (("g1", 12), ("g2", 3)):
        graph = pyoxigraph.NamedNode(EX + g)
        for i in range(n):
            s = pyoxigraph.NamedNode(f"{EX}{g}/s{i}")
            data.append(
                pyoxigraph.Quad(
                    s, pyoxigraph.NamedNode(EX + "p"), pyoxigraph.Literal(str(i)), graph
                )
            )
            if i % 2:
                data.append(
                    pyoxigraph.Quad(
                        s, pyoxigraph.NamedNode(EX + "q"), pyoxigraph.NamedNode(EX + "o"), graph
                    )
                )
    # One triple that only the default graph holds.
    data.append(
        pyoxigraph.Quad(
            pyoxigraph.NamedNode(EX + "d"),
            pyoxigraph.NamedNode(EX + "p"),
            pyoxigraph.Literal("default"),
            pyoxigraph.DefaultGraph(),
        )
    )
    store.extend(data)
    return store


@pytest.fixture
def endpoint() -> Iterator[type[_Endpoint]]:
    """Serve a fresh store on a free local port."""
    handler = type("Endpoint", (_Endpoint,), {"store": _store(), "queries": []})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    handler.url = f"http://127.0.0.1:{server.server_address[1]}/sparql"  # type: ignore[attr-defined]
    try:
        yield handler
    finally:
        server.shutdown()


def _export(endpoint: type[_Endpoint], tmp_path: Path, **options: Any) -> dict[str, Any]:
    entry = {"name": "toy", "endpoint": endpoint.url}  # type: ignore[attr-defined]
    return GraphExporter(entry, tmp_path / "toy", delay=0.0, **options).run()


def _by_graph(manifest: dict[str, Any]) -> dict[str | None, dict[str, Any]]:
    return {g["graph"]: g for g in manifest["graphs"]}


def test_whole_graphs_and_default_remainder_are_verified(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """Each graph comes in one CONSTRUCT whose parsed count equals the endpoint's COUNT."""
    manifest = _export(endpoint, tmp_path)
    graphs = _by_graph(manifest)
    assert manifest["outcome"] == "complete" and source_complete(manifest)
    assert graphs[EX + "g1"]["count"] == 18 and graphs[EX + "g1"]["triples"] == 18
    assert graphs[EX + "g1"]["outcome"] == "retrieved/verified"
    assert graphs[EX + "g1"]["method"] == "whole"
    # Oxigraph's default graph is not the union: its own triple is exported apart.
    assert graphs[None]["role"] == "default-extra" and graphs[None]["triples"] == 1
    piece = graphs[EX + "g2"]["pieces"][0]
    path = tmp_path / "toy" / piece["file"]
    assert count_file_triples(path, "N_TRIPLES") == (4, 0, None, 0)
    assert len(piece["sha256"]) == 64
    assert json.loads((tmp_path / "toy" / "manifest.json").read_text())["outcome"] == "complete"


def test_cut_results_are_split_by_predicate_and_paged(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """A server row cap sends the graph to predicates, then to unordered pages that are merged."""
    endpoint.row_cap = 5
    manifest = _export(endpoint, tmp_path)
    g1 = _by_graph(manifest)[EX + "g1"]
    assert g1["outcome"] == "retrieved/verified", g1["reason"]
    assert g1["method"] == "by-predicate"
    kept = [p for p in g1["pieces"] if p["error"] is None]
    assert sum(p["triples"] for p in kept) == 18
    merged = [p for p in kept if p["pages"] > 1]
    assert merged and merged[0]["triples"] == 12
    with gzip.open(tmp_path / "toy" / merged[0]["file"], "rt") as stream:
        lines = stream.read().splitlines()
    assert len(lines) == len(set(lines)) == 12
    # Page files are merged away; only listed files stay on disk.
    on_disk = {p.name for p in (tmp_path / "toy").glob("*.gz")}
    listed = {p["file"] for g in manifest["graphs"] for p in g["pieces"] if p["file"]}
    assert on_disk == listed


def test_refused_count_gives_unverified(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """Without a COUNT, a clean transfer under the cap is retrieved but unverified."""
    endpoint.refuse = ("COUNT",)
    manifest = _export(endpoint, tmp_path)
    graphs = _by_graph(manifest)
    assert graphs[EX + "g2"]["count"] is None
    assert graphs[EX + "g2"]["outcome"] == "retrieved/unverified"
    assert manifest["listing"]["method"] == "distinct"


def test_refused_construct_fails_the_source(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """A graph whose CONSTRUCTs are all refused fails, and the source is not complete."""
    endpoint.refuse = ("CONSTRUCT",)
    manifest = _export(endpoint, tmp_path)
    assert manifest["outcome"] == "partial" and not source_complete(manifest)
    assert all(g["outcome"] == "failed" for g in manifest["graphs"])


def test_byte_budget_stops_the_source(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """A source past its byte budget is stopped and recorded."""
    manifest = _export(endpoint, tmp_path, max_bytes=1)
    assert manifest["outcome"] == "stopped" and "byte" in manifest["reason"]


def test_resume_keeps_retrieved_graphs(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """A second run reuses graphs retrieved before and sends no CONSTRUCT for them."""
    _export(endpoint, tmp_path)
    endpoint.queries.clear()
    manifest = _export(endpoint, tmp_path)
    assert source_complete(manifest)
    assert not [q for q in endpoint.queries if "CONSTRUCT" in q]
    assert manifest["bytes"] > 0 and manifest["requests"] == 0 and manifest["requests_before"] == 3


def test_registry_graphs_are_taken_as_given(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """An entry that names its graphs exports those only, without the default graph."""
    entry = {"name": "toy", "endpoint": endpoint.url, "graph_uris": [EX + "g2"]}  # type: ignore[attr-defined]
    manifest = GraphExporter(entry, tmp_path / "toy", delay=0.0).run()
    assert [g["graph"] for g in manifest["graphs"]] == [EX + "g2"]
    assert source_complete(manifest)


def test_complete_export_becomes_local_inputs(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """A complete export gives registry fields that validate, and a work folder with its files."""
    from rdfsolve.graph_export import prepare_workdir
    from rdfsolve.models.source_model import SourceModel
    from rdfsolve.qlever.downloads import (
        download_paths,
        entry_download_urls,
        needs_download,
        read_record,
    )

    entry = {"name": "toy", "endpoint": endpoint.url, "graph_uris": [EX + "g1", EX + "g2"]}  # type: ignore[attr-defined]
    GraphExporter(entry, tmp_path / "toy", delay=0.0).run()
    workdir = tmp_path / "work"
    fields = prepare_workdir(tmp_path / "toy", workdir)
    assert fields["graph_uris"] == [EX + "g1", EX + "g2"]
    assert fields["endpoint_export"]["verified"] and fields["endpoint_export"]["triples"] == 22
    model = SourceModel.model_validate({**entry, **fields})
    assert set(model.graph_sources) == {EX + "g1", EX + "g2"}
    found = download_paths(workdir, {**entry, **fields})
    assert found and all(path is not None and path.is_file() for path in found.values())
    record = read_record(workdir)
    # The record lists the graphs' URLs, the list the pipeline compares it with.
    urls = entry_download_urls({**entry, **fields})
    assert record is not None and set(record["urls"]) == set(urls) == set(found)
    assert not needs_download(workdir, urls, has_inputs=True)
    assert len(json.loads((workdir / "export_inputs.json").read_text())["files"]) == 2


def test_incomplete_export_is_not_local(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """An export with a failed graph gives no local inputs."""
    from rdfsolve.graph_export import exported_entry

    endpoint.refuse = ("CONSTRUCT",)
    _export(endpoint, tmp_path)
    with pytest.raises(ValueError, match="not complete"):
        exported_entry(tmp_path / "toy")


def test_rdf_without_content_type_is_read_as_asked(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """QLever sends N-Triples without a Content-Type; asked for N-Triples alone, it is read so."""
    endpoint.content_type = ""
    manifest = _export(endpoint, tmp_path)
    assert source_complete(manifest)
    piece = _by_graph(manifest)[EX + "g1"]["pieces"][0]
    assert piece["file"].endswith(".nt.gz") and piece["triples"] == 18


def test_refused_default_check_with_empty_default_graph(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """When the default-graph comparison is refused, an empty default graph needs none."""
    endpoint.store.remove(
        pyoxigraph.Quad(
            pyoxigraph.NamedNode(EX + "d"),
            pyoxigraph.NamedNode(EX + "p"),
            pyoxigraph.Literal("default"),
            pyoxigraph.DefaultGraph(),
        )
    )
    endpoint.refuse = ("NOT EXISTS",)
    manifest = _export(endpoint, tmp_path)
    assert manifest["default_graph"]["role"] is None
    assert source_complete(manifest)


def test_counted_but_unreadable_graph_does_not_block(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """A graph that the endpoint counts but serves no triple of is unreadable, not failed."""
    endpoint.hide = f"<{EX}g2>"
    manifest = _export(endpoint, tmp_path)
    g2 = _by_graph(manifest)[EX + "g2"]
    assert g2["outcome"] == "unreadable" and g2["count"] == 4
    assert source_complete(manifest)


def test_busy_host_is_retried_in_a_later_round(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """Graphs that failed on HTTP 502 are exported again after the wait."""
    endpoint.busy = 40
    manifest = _export(endpoint, tmp_path, retry_wait=0.0, retry_rounds=3)
    assert source_complete(manifest), [g["reason"] for g in manifest["graphs"]]


def test_a_line_that_is_no_statement_fails_the_graph_only(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """A response with a line that is not a statement fails its graph; the others are kept."""
    endpoint.garbage = (
        b"<http://example.org/x> what is this\n<http://e/a> <http://e/p> <http://e/o> .\n"
    )
    manifest = _export(endpoint, tmp_path)
    graphs = _by_graph(manifest)
    assert graphs[EX + "g1"]["outcome"] == "failed"
    assert "1 lines are not statements" in graphs[EX + "g1"]["reason"]
    assert graphs[EX + "g2"]["outcome"] == "retrieved/verified"
    assert manifest["outcome"] == "partial"


_LINE = f'<{EX}g1/s0> <{EX}p> "0" .\n'.encode()


def test_literal_subject_is_left_out_recorded_and_accounted(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """A statement with a literal subject, which the endpoint counts, is left out of the file
    and recorded with its text; the files and it add up to the COUNT, so the graph is
    verified and the export complete, with the gap stated."""
    from rdfsolve.graph_export import exported_entry

    endpoint.substitute = (_LINE, f'"0" <{EX}p> <{EX}g1/s0> .\n'.encode())
    manifest = _export(endpoint, tmp_path)
    g1 = _by_graph(manifest)[EX + "g1"]
    assert g1["outcome"] == "retrieved/verified" and g1["method"] == "whole"
    assert (g1["count"], g1["triples"], g1["left_out"]) == (18, 17, {"literal-subject": 1})
    piece = g1["pieces"][0]
    record = json.loads((tmp_path / "toy" / piece["statements_file"]).read_text())
    assert record["lines"][0]["kind"] == "literal-subject"
    assert record["lines"][0]["original"] == f'"0" <{EX}p> <{EX}g1/s0> .'
    with gzip.open(tmp_path / "toy" / piece["file"], "rt") as stream:
        assert len(stream.read().splitlines()) == 17
    assert manifest["statements"]["left_out"] == {"literal-subject": 1}
    assert source_complete(manifest)
    fields = exported_entry(tmp_path / "toy")
    assert fields["endpoint_export"]["left_out_statements"] == {"literal-subject": 1}
    assert fields["endpoint_export"]["verified"]


def test_left_out_statements_of_merged_pages_are_counted_once(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """In pages merged into one file, a left-out statement still makes up the count."""
    endpoint.substitute = (_LINE, f'"0" <{EX}p> <{EX}g1/s0> .\n'.encode())
    endpoint.row_cap = 5
    manifest = _export(endpoint, tmp_path)
    g1 = _by_graph(manifest)[EX + "g1"]
    assert g1["outcome"] == "retrieved/verified", g1["reason"]
    assert g1["left_out"] == {"literal-subject": 1} and g1["triples"] == 17
    merged = [p for p in g1["pieces"] if p["error"] is None and p["pages"] > 1]
    assert merged and merged[0]["left_out"] == {"literal-subject": 1}
    on_disk = {p.name for p in (tmp_path / "toy").glob("*.json")} - {"manifest.json"}
    assert on_disk == {p["statements_file"] for p in merged}


def test_term_written_invalidly_is_repaired(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """An IRI that an engine writes with a raw < and > is percent-encoded, as the index input
    repair does; the triple is kept and counted."""
    endpoint.substitute = (f"<{EX}o>".encode(), f"<{EX}o<1>>".encode())
    manifest = _export(endpoint, tmp_path)
    g1 = _by_graph(manifest)[EX + "g1"]
    assert g1["outcome"] == "retrieved/verified"
    assert (g1["triples"], g1["repaired"], g1["left_out"]) == (18, 6, {})
    piece = g1["pieces"][0]
    with gzip.open(tmp_path / "toy" / piece["file"], "rt") as stream:
        assert f"<{EX}o%3C1%3E>" in stream.read()
    record = json.loads((tmp_path / "toy" / piece["statements_file"]).read_text())
    assert {r["kind"] for r in record["lines"]} == {"repaired"}


@pytest.mark.parametrize("trailer", [b"request timeout\n", b""])
def test_cut_body_is_asked_again_in_parts(
    endpoint: type[_Endpoint], tmp_path: Path, trailer: bytes
) -> None:
    """A 200 body that ends with a text after the last triple or mid-statement
    was cut: it is not kept, and the graph is fetched by predicate."""
    endpoint.cut_on = f"GRAPH <{EX}g1> {{ ?s ?p ?o }} }} LIMIT"
    endpoint.trailer = trailer
    manifest = _export(endpoint, tmp_path, retry_rounds=0)
    g1 = _by_graph(manifest)[EX + "g1"]
    assert g1["outcome"] == "retrieved/verified" and g1["method"] == "by-predicate"
    whole = g1["pieces"][0]
    assert whole["error"].startswith("cut: the body ends with") and not whole["file"]
    assert source_complete(manifest)


def test_cut_body() -> None:
    """Whole bodies end with a statement's dot (or a closing tag); others were cut."""
    from rdfsolve.graph_export import cut_body

    assert cut_body(b"<a> <b> <c> .\n", "N_TRIPLES") is None
    assert cut_body(b"", "N_TRIPLES") is None
    assert cut_body(b"<a> <b> <c> .\nrequest timeout\n", "N_TRIPLES")
    assert cut_body(b"<a> <b> <c> .\n<http://data", "N_TRIPLES")
    assert cut_body(b"@prefix ex: <http://e/> .\nex:a ex:b ex:c .\n", "TURTLE") is None
    assert cut_body(b"PREFIX ex: <http://e/>\n", "TURTLE") is None
    assert cut_body(b"ex:a ex:b ex:c ;\n", "TURTLE")
    assert cut_body(b"<rdf:RDF>\n</rdf:RDF>\n", "RDF_XML") is None
    assert cut_body(b"<rdf:RDF>\n<rdf:Descr", "RDF_XML")


def test_refused_ntriples_accept_falls_back(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """An endpoint that refuses N-Triples alone (HTTP 406) is asked with the full Accept list."""
    endpoint.refuse_accept = "application/n-triples"
    manifest = _export(endpoint, tmp_path)
    assert source_complete(manifest)
    assert isinstance(manifest["source_versions"], list)


def test_error_trailer_rejects_the_body(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """A 200 body that QLever ends with its error trailer is not a retrieved graph."""
    endpoint.garbage = (
        b"<http://example.org/x> <http://ex\n!!!!>># An error has occurred while exporting\n"
    )
    manifest = _export(endpoint, tmp_path, retry_rounds=0)
    g1 = _by_graph(manifest)[EX + "g1"]
    assert g1["outcome"] == "failed"
    assert any("error trailer" in (p["error"] or "") for p in g1["pieces"])


def test_default_remainder_beside_named_graphs_is_mapped(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """The default-graph remainder becomes a graph named after the endpoint; a refused COUNT
    of it is tried another way, and the method is recorded."""
    from rdfsolve.graph_export import exported_entry

    endpoint.refuse = ("(COUNT(*) AS ?n) WHERE { ?s ?p ?o FILTER NOT EXISTS",)
    manifest = _export(endpoint, tmp_path)
    remainder = _by_graph(manifest)[None]
    # Oxigraph's default graph is not the union, so neither other count fits: not verified.
    assert remainder["outcome"] == "retrieved/unverified"
    assert remainder["count_method"].startswith("not verified")
    fields = exported_entry(tmp_path / "toy")
    graph = endpoint.url.rstrip("/") + "#default"  # type: ignore[attr-defined]
    assert graph in fields["graph_sources"] and graph in fields["graph_uris"]
    assert fields["endpoint_export"]["default_graph_remainder_as"] == graph


def test_unreadable_graphs_are_listed(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """A graph the endpoint counts but never serves is listed in the manifest."""
    endpoint.hide = f"<{EX}g2>"
    manifest = _export(endpoint, tmp_path)
    assert manifest["unreadable_graphs"] == [{"graph": EX + "g2", "count": 4}]
    assert manifest["lenient_parses"] == []


def test_strict_failures_counts_and_samples_invalid_lines(tmp_path: Path) -> None:
    """Lines a strict N-Triples parser refuses (an IRI with a space) are counted and sampled."""
    from rdfsolve.graph_export import strict_failures

    path = tmp_path / "x.nt.gz"
    with gzip.open(path, "wt") as stream:
        stream.write("<http://e/a> <http://e/p> <http://e/o> .\n")
        stream.write("<http://e/a b> <http://e/p> <http://e/o> .\n")
    assert count_file_triples(path, "N_TRIPLES")[0] == 2
    count, samples = strict_failures(path)
    assert count == 1 and "a b" in samples[0]


def test_null_context_becomes_the_endpoint_default_graph(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """RDF4J's null context, exported as a graph, is named after the endpoint in the registry."""
    from rdfsolve.graph_export import exported_entry

    nil = pyoxigraph.NamedNode("http://www.openrdf.org/schema/sesame#nil")
    endpoint.store.add(
        pyoxigraph.Quad(
            pyoxigraph.NamedNode(EX + "n"),
            pyoxigraph.NamedNode(EX + "p"),
            pyoxigraph.Literal("null"),
            nil,
        )
    )
    entry = {"name": "toy", "endpoint": endpoint.url, "graph_uris": [EX + "g1", nil.value]}  # type: ignore[attr-defined]
    manifest = GraphExporter(entry, tmp_path / "toy", delay=0.0).run()
    assert source_complete(manifest)
    fields = exported_entry(tmp_path / "toy")
    graph = endpoint.url.rstrip("/") + "#default"  # type: ignore[attr-defined]
    assert fields["graph_uris"] == [EX + "g1", graph]


def test_statement_over_the_parser_buffer_is_read(tmp_path: Path) -> None:
    """pyoxigraph raises MemoryError past its 16 MiB buffer (a long literal); the line reader reads the statement, which counts and is kept as it is."""
    from rdfsolve.graph_export import OXIGRAPH_BUFFER, read_statements

    path = tmp_path / "x.nt.gz"
    huge = "1 2, " * (OXIGRAPH_BUFFER // 4 + 10)
    with gzip.open(path, "wt") as stream:
        stream.write('<http://e/a> <http://e/p> "ok" .\n')
        stream.write(f'_:b <http://e/p> "LINESTRING ({huge})\\n"^^<http://e/wkt> .\n')
    before = path.read_bytes()
    found = read_statements(path, "N_TRIPLES", rewrite=True)
    assert (found.triples, found.long_statements, found.unparsed) == (2, 1, 0)
    assert found.blank_nodes == 1 and found.parse_error.startswith("MemoryError")
    assert path.read_bytes() == before
    assert count_file_triples(path, "N_TRIPLES")[0::3] == (2, 0)


def test_long_statement_with_an_invalid_escape_is_repaired(tmp_path: Path) -> None:
    """The line reader checks the long literal's escapes as N-Triples allows them; a bare
    backslash is escaped as the index input repair does."""
    from rdfsolve.graph_export import OXIGRAPH_BUFFER, read_statements

    path = tmp_path / "x.nt.gz"
    with gzip.open(path, "wt") as stream:
        stream.write(f'<http://e/a> <http://e/p> "{"x" * OXIGRAPH_BUFFER}\\q" .\n')
    found = read_statements(path, "N_TRIPLES", rewrite=True)
    # Written with the backslash escaped (the index input repair), then read as long.
    assert (found.triples, found.repaired, found.long_statements) == (1, 1, 1)
    with gzip.open(path, "rb") as stream:
        assert stream.read().endswith(b'\\\\q" .\n')


def test_null_context_is_exported_when_the_default_check_is_refused(tmp_path: Path) -> None:
    """A store refuses the default-graph check; a null context that holds triples is then
    exported as a graph, as when the check answers."""
    from rdfsolve.sparql_helper import SparqlHelperError

    exporter = GraphExporter({"name": "x", "endpoint": "http://127.0.0.1:9/sparql"}, tmp_path)

    def ask(query: str, purpose: str, seconds: float | None = None) -> bool:
        raise SparqlHelperError("HTTP 503: Query evaluation took too long")

    def count(where: str, prologue: str = "") -> tuple[int | None, str | None]:
        return (5, None) if "sesame#nil" in where else (100, None)

    exporter._ask = ask  # type: ignore[method-assign]
    exporter._count = count  # type: ignore[method-assign]
    role, why = exporter._default_graph_role([EX + "g1", EX + "g2"], {EX + "g1": 60, EX + "g2": 60})
    assert role == "null-context" and "holds 5 triples" in why


def test_undetermined_default_graph_is_not_exported_by_a_retry_round() -> None:
    """A default graph whose role was not decided is not exported whole by a retry round."""
    from rdfsolve.graph_export import _transient

    item = {"graph": None, "method": "undetermined", "reason": "HTTP 503", "pieces": []}
    assert not _transient(item)
    assert _transient({**item, "method": ""})


def test_complete_export_is_not_rewritten(endpoint: type[_Endpoint], tmp_path: Path) -> None:
    """A complete export may be pinned by its manifest's SHA-256: a later run keeps it as is."""
    import hashlib

    from rdfsolve.graph_export import export_source

    entry = {"name": "toy", "endpoint": endpoint.url}  # type: ignore[attr-defined]
    export_source(entry, tmp_path, "s", delay=0.0, min_free_bytes=0)
    path = tmp_path / "toy" / "s" / "manifest.json"
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    endpoint.queries.clear()
    export_source(entry, tmp_path, "s", delay=0.0, min_free_bytes=0)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert not endpoint.queries


def test_files_of_a_failed_graph_are_removed_when_it_is_exported_again(
    endpoint: type[_Endpoint], tmp_path: Path
) -> None:
    """A later run exports a failed graph again; the files of the failed attempt go."""
    endpoint.garbage = (
        b"<http://example.org/x> what is this\n<http://e/a> <http://e/p> <http://e/o> .\n"
    )
    _export(endpoint, tmp_path)
    assert (tmp_path / "toy" / "g0001.whole.nt.gz").is_file()
    endpoint.garbage = b""
    # Asked whole again, the graph is cut; its earlier whole file must not stay.
    endpoint.cut_on = f"GRAPH <{EX}g1> {{ ?s ?p ?o }} }} LIMIT"
    manifest = _export(endpoint, tmp_path)
    assert source_complete(manifest)
    on_disk = {p.name for p in (tmp_path / "toy").glob("*.gz")}
    listed = {p["file"] for g in manifest["graphs"] for p in g["pieces"] if p["file"]}
    assert on_disk == listed
