"""rdfsolve.mining.scan export: rows read from an endpoint as text and ids together, in blocks;
an export that stopped resumes; literal text is cut except for names and definitions; a
predicate counted in parts gives the counts of one pass."""

import hashlib
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyoxigraph as ox
import pytest

from rdfsolve.mining import scan

DATA = b"""
<urn:a1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:A> .
<urn:a2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:A> .
<urn:b1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:B> .
<urn:a1> <urn:link> <urn:b1> .
<urn:a2> <urn:link> <urn:b1> .
<urn:a2> <urn:link> <urn:u1> .
<urn:a1> <urn:note> "a long note that goes on and on, much longer than sixty-four characters in all"@en .
<urn:a1> <http://www.w3.org/2000/01/rdf-schema#label> "a label that is also longer than sixty-four characters, kept in full here" .
<urn:a2> <urn:size> "7"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""


class Endpoint:
    """A local SPARQL endpoint on pyoxigraph answering as QLever does: TSV, or one 64-bit id per
    cell (a hash of the cell's text); it can fail after a number of row queries."""

    def __init__(self) -> None:
        self.store = ox.Store()
        self.store.load(DATA, format=ox.RdfFormat.N_TRIPLES)
        self.fail_after: int | None = None
        # The rows of urn:link are refused as QLever does at its time limit (429, "timed out")
        # when a query would read more than this many of them.
        self.timeout_rows: int | None = None
        # With cut_after_200, the time limit is reported as QLever does when it is reached while
        # the result is sent: HTTP 200, one row, then the error trailer.
        self.cut_after_200 = False
        # The predicates whose row queries the server refuses (400, as a query it cannot parse).
        self.refuse: set[str] = set()
        self.queries: list[str] = []
        # The form fields of each request (the per-query timeout of a planning query).
        self.forms: list[dict[str, list[str]]] = []
        # With quads, the data is in named graphs (the default graph is their union, as in
        # QLever), and the planning queries over all graphs (counts or DISTINCT with GRAPH ?g)
        # time out, as on PubChem (job 115703).
        self.quads = False
        self.row_queries = 0
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"])).decode()
                form = urllib.parse.parse_qs(body)
                query = form["query"][0]
                endpoint.queries.append(query)
                endpoint.forms.append(form)
                planning = "COUNT" in query or "DISTINCT" in query
                if endpoint.quads and "GRAPH ?g" in query and planning:
                    self.send_response(429)
                    self.end_headers()
                    self.wfile.write(b'{"exception": "Operation timed out. Last operation: Sort"}')
                    return
                if any(f"<{p}>" in query and "COUNT" not in query for p in endpoint.refuse):
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"Invalid SPARQL query")
                    return
                if "DATATYPE" in query:
                    endpoint.row_queries += 1
                    if (
                        endpoint.fail_after is not None
                        and endpoint.row_queries > endpoint.fail_after
                    ):
                        self.send_response(500)
                        self.end_headers()
                        return
                if (
                    endpoint.timeout_rows is not None
                    and "<urn:link>" in query
                    and "COUNT" not in query
                    and "GRAPH" not in query
                    and len(list(endpoint.store.query(query))) > endpoint.timeout_rows
                ):
                    if (
                        endpoint.cut_after_200
                        and self.headers["Accept"] != "application/octet-stream"
                    ):
                        self.send_response(200)
                        self.end_headers()
                        self.wfile.write(
                            b"?s\t?o\t?d\n<urn:a1>\t<urn:b1>\t\n"
                            + scan.QLEVER_ERROR_TRAILER
                            + b" while exporting the query result. Unfortunately due to limitations"
                            + b" in the HTTP 1.1 protocol, there is no better way to report this"
                            + b" than to append it to the incomplete result. The error message"
                            + b" was:\n"
                            + b" " * 300
                            + b"Operation timed out.\n"
                        )
                        return
                    if endpoint.cut_after_200:
                        text = endpoint.store.query(query).serialize(
                            format=ox.QueryResultsFormat.TSV
                        )
                        cells = [line.split(b"\t") for line in text.split(b"\n")[1:] if line]
                        self.send_response(200)
                        self.end_headers()
                        self.wfile.write(
                            b"".join(
                                hashlib.blake2b(c, digest_size=8).digest()
                                for row in cells
                                for c in row
                            )
                        )
                        return
                    self.send_response(429)
                    self.end_headers()
                    self.wfile.write(b'{"exception": "Operation timed out. Last operation: Scan"}')
                    return
                if "GRAPH ?g" in query and not endpoint.quads:
                    text = b"?g\n"
                else:
                    # QLever's default graph is the union of the graphs; its triples outside
                    # every named graph (FROM QLEVER_DEFAULT_GRAPH) are none here.
                    union = endpoint.quads and scan.QLEVER_DEFAULT_GRAPH not in query
                    text = endpoint.store.query(query, use_default_graph_as_union=union).serialize(
                        format=ox.QueryResultsFormat.TSV
                    )
                if self.headers["Accept"] == "application/octet-stream":
                    cells = [line.split(b"\t") for line in text.split(b"\n")[1:] if line]
                    text = b"".join(
                        hashlib.blake2b(c, digest_size=8).digest() for row in cells for c in row
                    )
                self.send_response(200)
                self.end_headers()
                self.wfile.write(text)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"


@pytest.fixture
def endpoint():
    server = Endpoint()
    yield server
    server.server.shutdown()


def test_rows_are_read_in_blocks_with_their_ids(endpoint, tmp_path, monkeypatch):
    monkeypatch.setattr(scan, "BLOCK_BYTES", 40)
    store = scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"}, workers=2)
    rows = store.rows("urn:link").collect()
    assert rows.height == 3 and rows["sid"].n_unique() == 2 and rows["oid"].n_unique() == 2
    ids = {s: sid for s, sid in rows.select("s", "sid").iter_rows()}
    assert ids["<urn:a1>"] == int.from_bytes(
        hashlib.blake2b(b"<urn:a1>", digest_size=8).digest(), "little"
    )
    note = store.rows("urn:note").collect()["o"][0]
    assert note.endswith('…"@en') and len(note) < 80, "Other literals are cut"
    label = store.rows("http://www.w3.org/2000/01/rdf-schema#label").collect()["o"][0]
    assert "kept in full here" in label, "Names and definitions keep their text"
    assert store.manifest["triples"] == 9


def test_an_export_that_stopped_reads_only_the_predicates_not_yet_written(endpoint, tmp_path):
    endpoint.fail_after = 2  # two of the five predicates (their text; the ids have no DATATYPE)
    with pytest.raises(Exception):
        scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"}, workers=1)
    assert (tmp_path / "store" / "progress.jsonl").read_text().count("\n") == 2
    endpoint.fail_after, endpoint.row_queries = None, 0
    store = scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"}, workers=1)
    assert endpoint.row_queries == 3, "The three predicates left (the text query of each)"
    assert store.manifest["resumed_predicates"] == 2
    assert sum(store.manifest["rows"].values()) == 9


def test_a_predicate_counted_in_parts_gives_the_counts_of_one_pass(endpoint, tmp_path, monkeypatch):
    store = scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"})

    def counts():
        return {
            (p.subject_class, p.property_uri, p.object_class, p.datatype): (
                p.count,
                p.distinct_subjects,
                p.distinct_objects,
            )
            for p in scan.count_patterns(store)
        }

    whole = counts()
    monkeypatch.setattr(scan, "PARTITION_ROWS", 1)
    monkeypatch.setattr(scan, "BATCH_ROWS", 1)
    assert counts() == whole
    assert whole[("urn:A", "urn:link", "urn:B", None)] == (2, 2, 1)


def test_a_predicate_the_server_refuses_is_a_gap(endpoint, tmp_path):
    endpoint.refuse = {"urn:note"}
    store = scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"}, workers=2)
    assert "urn:note" not in store.predicates and "urn:link" in store.predicates
    assert list(store.manifest["gaps"]) == ["urn:note"]
    assert store.rows("urn:link").collect().height == 3


def test_a_term_that_is_not_an_rdf_iri_is_sent_with_iri(endpoint):
    with scan._post(endpoint.url, "SELECT ?s ?o WHERE { ?s <urn:statistic  n> ?o }", "text/plain"):
        pass
    assert 'IRI("urn:statistic  n")' in endpoint.queries[-1]
    assert "<urn:statistic  n>" not in endpoint.queries[-1]


def test_a_resumed_export_reads_again_the_files_that_are_not_whole(endpoint, tmp_path):
    """rdfportal.oma (job 115716) resumed a store an earlier job left when it was killed. A
    file that is zero-filled or short, or a progress line that was cut, is not trusted: its
    predicate is read again, and the store is whole."""
    endpoint.fail_after = 2
    with pytest.raises(Exception):
        scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"}, workers=1)
    store_dir = tmp_path / "store"
    progress = store_dir / "progress.jsonl"
    written = [line for line in progress.read_text().splitlines() if line]
    assert len(written) == 2
    files = [store_dir / "rows" / scan.json.loads(line)["file"] for line in written]
    files[0].write_bytes(b"\x00" * files[0].stat().st_size)  # zero-filled, same size
    files[1].write_bytes(files[1].read_bytes()[:20])  # short
    with progress.open("a") as log:
        log.write('{"predicate": "urn:cut", "fi')  # a line the kill cut
    endpoint.fail_after, endpoint.row_queries = None, 0
    store = scan.export_index(endpoint.url, store_dir, index={"name": "t"}, workers=1)
    assert endpoint.row_queries == 5, "The two damaged predicates and the three left"
    assert store.manifest["resumed_predicates"] == 0
    for predicate in store.predicates:
        assert scan.store_file_problem(store.predicates[predicate]) is None
    assert sum(store.rows(p).collect().height for p in store.predicates) == 9


def test_a_store_file_with_nul_terms_is_not_reused(tmp_path):
    import polars as pl

    good = tmp_path / "good.parquet"
    pl.DataFrame({"s": ["<urn:a>"], "o": ["<urn:b>"]}).write_parquet(good)
    assert scan.store_file_problem(good, 1) is None
    assert "rows" in scan.store_file_problem(good, 2)
    nul = tmp_path / "nul.parquet"
    pl.DataFrame({"s": ["\x00" * 8], "o": ["<urn:b>"]}).write_parquet(nul)
    assert "NUL" in scan.store_file_problem(nul)
    (tmp_path / "zero.parquet").write_bytes(b"\x00" * 100)
    assert "open" in scan.store_file_problem(tmp_path / "zero.parquet")


def test_a_graph_split_that_times_out_does_not_fail_the_source(endpoint, tmp_path):
    """PubChem (job 115703): the count by graph and predicate and then the listing of the graphs
    (DISTINCT ?g, which reads every triple) timed out, and the source failed. The count by
    graph is a planning query with a time limit; when the graph split cannot be counted, the
    rows are read without their graphs, the store says so, and a graph scope mines them all."""
    endpoint.store = ox.Store()
    endpoint.store.load(DATA.replace(b" .\n", b" <urn:graph:one> .\n"), format=ox.RdfFormat.N_QUADS)
    endpoint.quads = True
    store = scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"}, workers=1)
    assert store.graphs is None and store.manifest["graph_split"]["state"] == "missing"
    assert not any("DISTINCT ?g" in q for q in endpoint.queries), "The graphs are not listed"
    planned = [f for f in endpoint.forms if "GROUP BY ?g ?p" in f["query"][0]]
    assert planned and planned[0]["timeout"] == [f"{scan.PLAN_SECONDS:g}s"]
    assert sum(store.rows(p).collect().height for p in store.predicates) == 9
    assert store.view(["urn:graph:one"]) is store, "The scope is not applied"


def test_known_graphs_are_not_listed_or_grouped(endpoint, tmp_path):
    """The graphs of a graph-mapped entry are given: each is counted alone, nothing is
    grouped by graph over the whole index."""
    endpoint.store = ox.Store()
    endpoint.store.load(DATA.replace(b" .\n", b" <urn:graph:one> .\n"), format=ox.RdfFormat.N_QUADS)
    endpoint.quads = True
    store = scan.export_index(
        endpoint.url,
        tmp_path / "store",
        index={"name": "t"},
        workers=1,
        named_graphs=["urn:graph:one"],
    )
    assert store.manifest["graph_split"] == {"state": "by_graph", "graphs_from": "registry"}
    assert set(store.graphs) == {"urn:graph:one"}
    assert not any("GROUP BY ?g" in q or "DISTINCT ?g" in q for q in endpoint.queries)
