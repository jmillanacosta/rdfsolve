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
        self.row_queries = 0
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"])).decode()
                query = urllib.parse.parse_qs(body)["query"][0]
                if "DATATYPE" in query:
                    endpoint.row_queries += 1
                    if (
                        endpoint.fail_after is not None
                        and endpoint.row_queries > endpoint.fail_after
                    ):
                        self.send_response(500)
                        self.end_headers()
                        return
                if "GRAPH ?g" in query:
                    text = b"?g\n"
                else:
                    text = endpoint.store.query(query).serialize(format=ox.QueryResultsFormat.TSV)
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
    endpoint.fail_after = 4  # two of the five predicates (text and ids of each)
    with pytest.raises(Exception):
        scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"}, workers=1)
    assert (tmp_path / "store" / "progress.jsonl").read_text().count("\n") == 2
    endpoint.fail_after, endpoint.row_queries = None, 0
    store = scan.export_index(endpoint.url, tmp_path / "store", index={"name": "t"}, workers=1)
    assert endpoint.row_queries == 6, "The three predicates left (text and ids of each)"
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
