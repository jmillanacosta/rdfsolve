"""Export the graphs of a SPARQL endpoint to files, one CONSTRUCT at a time per host.

For a registry entry, the exporter lists the graphs that hold the source's data, counts the
triples of each where the endpoint answers the count, and fetches each graph with
``CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <g> { ?s ?p ?o } } LIMIT <cap>``, streamed to a gzip
file on disk. A graph counts as retrieved when its files parse and hold as many triples as the
endpoint counts (verified, with the statements left out below), or, when the endpoint refuses the count, when the transfer ended
cleanly with fewer triples than the cap and than a known server row cap (unverified). A graph
that is larger than the cap, or whose CONSTRUCT is refused or cut, is fetched by predicate,
and a predicate that is still too large in pages of a stable order (ORDER BY ?s ?o).

A body that does not end with a whole statement was cut by the server (a server may end the
body of a query past its time limit mid-statement, or write a message after the last whole
triple): it is not kept, and the graph is asked for in smaller parts. In an N-Triples body, a
line that the parser does not read is read again alone: a statement longer than the parser's
buffer is read by a line reader, a term that can be written validly is repaired as the index
input repair does (rdfsolve.qlever.repair), and a statement that RDF cannot hold (a literal as
subject or predicate) is left out of the file. Each repaired or left-out line is recorded with
its original text beside the file (<piece>.statements.json), and the graph is verified when the
triples in the files and the statements left out add up to the endpoint's count.

Which graphs: the entry's graph_uris when it names them; otherwise every named graph of the
endpoint except the engine's own (SUGGESTED_SERVICE_GRAPHS), listed with one GROUP BY (which
also counts), else an unordered DISTINCT, else the paged listing, else the graph names of the
service description. The default graph is exported when the endpoint has no other named
graph; when it has, the default-graph triples that no named graph holds are exported if any
exist.

Requests go one at a time per host (rdfsolve._host_gate), with the host spacing and
server cooldowns of the SPARQL helper. Every outcome is written to manifest.json in the output
directory after each graph, so a stopped export is seen and resumed.

Prior art: sparql-dump (Java, RDF4J) pages a graph with LIMIT/OFFSET without ORDER BY, which
no engine promises to keep stable, and does not check counts; Virtuoso's RDF_DUMP_NQUADS and
dump_one_graph run inside the server and need its administrator.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, cast
from urllib.parse import urlsplit

from rdflib import URIRef

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_CAP",
    "EXPORT_FORMAT",
    "ExportBudgetError",
    "GraphExporter",
    "GraphOutcome",
    "Piece",
    "count_file_triples",
    "entries_on_host",
    "export_source",
    "exported_entry",
    "long_statement",
    "prepare_workdir",
    "read_statements",
    "source_complete",
]

EXPORT_FORMAT = "rdfsolve-graph-export/1"
DEFAULT_CAP = 50_000_000
MANIFEST = "manifest.json"
# Formats asked for, best first; N-Triples streams and parses fastest.
# N-Triples alone first: given a list with q-values, QLever answers Turtle, which it writes
# many times slower than N-Triples.
ACCEPT = "application/n-triples"
# Asked for when an endpoint refuses ACCEPT with HTTP 406, then FALLBACK_ACCEPT.
ACCEPT_ANY = (
    "application/n-triples, text/plain;q=0.9, text/turtle;q=0.8, application/x-turtle;q=0.8, "
    "application/rdf+xml;q=0.5"
)
FALLBACK_ACCEPT = "text/turtle"
# Media type of the response: (file suffix, pyoxigraph format name).
_FORMATS: dict[str, tuple[str, str]] = {
    "application/n-triples": ("nt", "N_TRIPLES"),
    "text/plain": ("nt", "N_TRIPLES"),
    "text/turtle": ("ttl", "TURTLE"),
    "application/x-turtle": ("ttl", "TURTLE"),
    "text/n3": ("ttl", "TURTLE"),
    "application/rdf+xml": ("rdf", "RDF_XML"),
}
# Graphs that hold a W3C vocabulary as the engine ships it, not a source's data: Virtuoso loads
# the OWL vocabulary into <http://www.w3.org/2002/07/owl#>.
VOCABULARY_GRAPHS = ("http://www.w3.org/2002/07/owl#",)
# Texts with which a server ends an HTTP 200 body that it could not finish: QLever writes
# "!!!!>># An error has occurred while exporting the query result. ... Operation timed out."
# after a half-written triple.
ERROR_TRAILERS = (b"!!!!>># An error has occurred",)
_TAIL_BYTES = 8192
# Engines whose default graph, without a dataset clause, is the union of the named graphs: an
# endpoint that does not answer whether its default graph holds other triples is taken to hold
# none there (Virtuoso, Blazegraph in quad mode).
UNION_DEFAULT_ENGINES = ("virtuoso", "blazegraph")
# The smallest page asked for before a predicate is given up.
MIN_PAGE = 10_000
# Lines parsed together before a batch that fails is read line by line.
_BATCH_LINES = 10_000
# pyoxigraph refuses a statement longer than its parser buffer (MemoryError).
OXIGRAPH_BUFFER = 16 * 1024 * 1024
# The record of the lines of a piece that were repaired or left out, beside its file.
STATEMENTS_SUFFIX = ".statements.json"
# What the kinds of left-out statements and of other recorded lines mean.
STATEMENT_KINDS = {
    "literal-subject": "a literal as subject: not RDF, left out",
    "literal-predicate": "a literal as predicate: not RDF, left out",
    "blank-predicate": "a blank node as predicate: not RDF, left out",
    "invalid-term": "a statement with a term that no parser reads and that cannot be "
    "written validly, left out",
    "not-a-statement": "a line that is not a statement: not counted, the graph is not verified",
    "repaired": "terms written validly (rdfsolve.qlever.repair): an IRI percent-encoded, a "
    "backslash escaped; the same triple",
    "long": "a statement longer than the parser buffer, read by the line reader; kept as is",
}
# Kinds of left-out lines that are statements, counted by the endpoint's COUNT.
LEFT_OUT_KINDS = ("literal-subject", "literal-predicate", "blank-predicate", "invalid-term")
_RETRYABLE = (429, 502, 503, 504, 520, 522, 524)


class ExportBudgetError(RuntimeError):
    """The export of a source would exceed its byte, request or time budget."""


class _FetchError(RuntimeError):
    """One CONSTRUCT was refused, cut or returned something that is not RDF.

    received is the number of lines the body held before it was cut (None: not cut).
    """

    def __init__(self, message: str, received: int | None = None) -> None:
        """Keep the message and the lines received before a cut."""
        super().__init__(message)
        self.received = received


@dataclass
class Piece:
    """One CONSTRUCT response on disk."""

    label: str
    query: str
    file: str = ""
    content_type: str = ""
    bytes: int = 0
    uncompressed_bytes: int = 0
    sha256: str = ""
    triples: int | None = None
    blank_nodes: int = 0
    parse_error: str | None = None
    unparsed: int = 0
    # Statements that a strict parser refuses but a lenient one reads (N-Triples only).
    invalid_count: int | None = None
    invalid_samples: list[str] = field(default_factory=list)
    # Lines written validly, statements left out by kind, statements longer than the parser
    # buffer, and the record of those lines (STATEMENTS_SUFFIX) with its SHA-256.
    repaired: int = 0
    left_out: dict[str, int] = field(default_factory=dict)
    long_statements: int = 0
    statements_file: str = ""
    statements_sha256: str = ""
    seconds: float = 0.0
    error: str | None = None
    pages: int = 1


@dataclass
class GraphOutcome:
    """The export of one graph: its count, its pieces and how it ended."""

    graph: str | None
    role: str
    count: int | None = None
    count_error: str | None = None
    count_method: str = "COUNT"
    method: str = ""
    triples: int = 0
    # Statements repaired, and left out by kind, in the kept files (see Piece).
    repaired: int = 0
    left_out: dict[str, int] = field(default_factory=dict)
    pieces: list[Piece] = field(default_factory=list)
    outcome: str = "pending"
    reason: str = ""
    started: str = ""
    finished: str = ""
    seconds: float = 0.0

    @property
    def retrieved(self) -> bool:
        """Return whether the graph is on disk whole (verified or unverified)."""
        return self.outcome.startswith("retrieved")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def _chain(first: bytes, rest: Iterator[bytes]) -> Iterator[bytes]:
    yield first
    yield from rest


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _media(content_type: str) -> str:
    return (content_type.split(";", 1)[0].split() or [""])[0].lower()


@dataclass
class StatementRead:
    """What reading an RDF file found: see read_statements."""

    triples: int = 0
    blank_nodes: int = 0
    parse_error: str | None = None
    unparsed: int = 0
    repaired: int = 0
    long_statements: int = 0
    left_out: dict[str, int] = field(default_factory=dict)
    records: list[dict[str, Any]] = field(default_factory=list)


def accounted(piece: Piece) -> int:
    """Return the statements of a piece: the triples in its file and those left out."""
    return (piece.triples or 0) + sum(piece.left_out.values())


# A literal, unrolled (fast on a statement of many megabytes).
_LONG_LITERAL = re.compile(rb'"[^"\\]*(?:\\.[^"\\]*)*"')
# A backslash and the escape it starts (group 1), or a bare one; a raw line break.
_LITERAL_ESCAPE = re.compile(rb"\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|[tbnrf\"'\\])?|[\r\n]")


def long_statement(line: bytes, rdf_format: Any) -> Any:
    """Read a statement longer than pyoxigraph's buffer; return its quad, or None.

    The longest literal (whose text is what makes a statement this long) is checked alone:
    valid UTF-8 and only the escapes N-Triples allows; the statement with that literal
    written empty is then parsed as any other line.
    """
    import pyoxigraph

    literals = list(_LONG_LITERAL.finditer(line))
    if not literals:
        return None
    longest = max(literals, key=lambda m: m.end() - m.start())
    text = longest.group()[1:-1]
    try:
        text.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if any(m.group(1) is None for m in _LITERAL_ESCAPE.finditer(text)):
        return None
    short = line[: longest.start()] + b'""' + line[longest.end() :]
    if len(short) >= OXIGRAPH_BUFFER:
        return None
    try:
        quads = list(pyoxigraph.parse(short, rdf_format, lenient=True))
    except (SyntaxError, MemoryError):
        return None
    return quads[0] if len(quads) == 1 else None


def _statement_kind(line: str) -> str:
    """Name what a line that no parser reads is (a key of STATEMENT_KINDS)."""
    from rdfsolve.qlever.repair import _BLANK, _IRI_END, _SPACE

    text = line.strip()
    if not text.endswith(".") or not text.startswith(("<", "_:", '"')):
        return "not-a-statement"
    if text.startswith('"'):
        return "literal-subject"
    if text.startswith("<"):
        end = _IRI_END.search(text, 1)
        position = end.end() if end else -1
    else:
        match = _BLANK.match(text)
        position = match.end() if match else -1
    if position < 0:
        return "not-a-statement"
    position = _SPACE.match(text, position).end()  # type: ignore[union-attr]
    if text.startswith('"', position):
        return "literal-predicate"
    if text.startswith("_:", position):
        return "blank-predicate"
    return "invalid-term" if text.startswith("<", position) else "not-a-statement"


def read_statements(path: Path, fmt: str, rewrite: bool = False) -> StatementRead:
    """Parse a gzip RDF file as a stream; return its triples and what could not be read.

    The file is parsed strictly, else leniently, as a lenient loader would (an IRI with a
    space). When even the lenient parser stops, an N-Triples file is read in batches of
    lines, and a batch that does not parse line by line:

    - a statement longer than pyoxigraph's 16 MiB buffer (a long literal) is read by long_statement and counted as a triple;
    - a line that rdfsolve.qlever.repair writes so that it parses (an IRI with <, > or ", a
      backslash that starts no escape) is a repaired triple;
    - a statement RDF cannot hold (a literal as subject or predicate) is left out, by kind;
    - a line that is not a statement is counted in unparsed.

    With rewrite, a file with a repaired or left-out line is written again with the repairs
    and without the left-out lines; records lists each such line with its original text (a
    long statement by its line and length). In a Turtle or RDF/XML file the statements after
    the error are not known, and unparsed is -1.
    """
    import pyoxigraph

    rdf_format = getattr(pyoxigraph.RdfFormat, fmt)

    def blank(quad: Any) -> bool:
        """Return whether the triple has a blank node as subject or object."""
        return isinstance(quad.subject, pyoxigraph.BlankNode) or isinstance(
            quad.object, pyoxigraph.BlankNode
        )

    def scan(lenient: bool) -> tuple[int, int]:
        """Count the triples and those with a blank node, in one pass."""
        triples = blanks = 0
        with gzip.open(path, "rb") as stream:
            for quad in pyoxigraph.parse(cast(IO[bytes], stream), rdf_format, lenient=lenient):
                triples += 1
                blanks += blank(quad)
        return triples, blanks

    # pyoxigraph raises MemoryError for a statement longer than its 16 MiB buffer.
    try:
        triples, blanks = scan(False)
        return StatementRead(triples, blanks)
    except (SyntaxError, MemoryError) as error:
        message = f"{type(error).__name__}: {error}"[:500]
    try:
        triples, blanks = scan(True)
        return StatementRead(triples, blanks, message)
    except (SyntaxError, MemoryError):
        pass
    if fmt != "N_TRIPLES":
        return StatementRead(0, 0, message, -1)
    return _read_lines(path, rdf_format, message, rewrite, blank)


def _read_lines(
    path: Path, rdf_format: Any, message: str, rewrite: bool, blank: Callable[[Any], bool]
) -> StatementRead:
    """Read an N-Triples file in batches of lines; see read_statements."""
    import pyoxigraph

    from rdfsolve.qlever.repair import repair_line

    found = StatementRead(parse_error=message)
    partial = path.with_name(f".{path.name}.rewrite")
    changed = False

    def parse(line: bytes) -> tuple[list[Any], bool] | None:
        """Parse one line alone: its quads and whether it was long; None if it does not parse."""
        try:
            return list(pyoxigraph.parse(line, rdf_format, lenient=True)), False
        except MemoryError:
            quad = long_statement(line, rdf_format)
            return None if quad is None else ([quad], True)
        except SyntaxError:
            return None

    def take(number: int, line: bytes, quads: list[Any], long: bool) -> None:
        """Count the triples of a line that was read."""
        found.triples += len(quads)
        found.blank_nodes += sum(blank(q) for q in quads)
        if long:
            found.long_statements += 1
            found.records.append({"line": number, "kind": "long", "bytes": len(line)})

    def one(number: int, line: bytes) -> bytes:
        """Read one line alone; return what is written for it (b"" to leave it out)."""
        nonlocal changed
        read = parse(line)
        if read is not None:
            take(number, line, *read)
            return line
        text = line.decode("utf-8", "replace")
        written, kinds = repair_line(text)
        read = parse(written.encode()) if kinds else None
        if read is not None and len(read[0]) == 1:
            take(number, written.encode(), *read)
            found.repaired += 1
            found.records.append(
                {
                    "line": number,
                    "kind": "repaired",
                    "changes": sorted(set(kinds)),
                    "original": text.rstrip("\r\n"),
                    "written": written.rstrip("\n"),
                }
            )
            changed = True
            return written.encode()
        kind = _statement_kind(text)
        if kind == "not-a-statement":
            found.unparsed += 1
        else:
            found.left_out[kind] = found.left_out.get(kind, 0) + 1
        found.records.append({"line": number, "kind": kind, "original": text.rstrip("\r\n")})
        changed = True
        return b""

    def flush(first: int, batch: list[bytes], out: gzip.GzipFile | None) -> None:
        """Parse a batch at once; when it fails, read its lines one by one."""
        try:
            quads = list(pyoxigraph.parse(b"".join(batch), rdf_format, lenient=True))
        except (SyntaxError, MemoryError):
            written = [one(first + k, line) for k, line in enumerate(batch)]
        else:
            found.triples += len(quads)
            found.blank_nodes += sum(blank(q) for q in quads)
            written = batch
        if out is not None:
            out.writelines(written)

    with (
        gzip.open(path, "rb") as stream,
        gzip.open(partial, "wb", compresslevel=3) if rewrite else _Discard() as sink,
    ):
        out = sink if rewrite else None
        batch: list[bytes] = []
        first = 1
        for number, line in enumerate(stream, 1):
            if not batch:
                first = number
            if not line.endswith(b"\n"):
                line += b"\n"
            batch.append(line)
            if len(batch) >= _BATCH_LINES:
                flush(first, batch, out)
                batch = []
        if batch:
            flush(first, batch, out)
    if rewrite and changed:
        partial.replace(path)
    else:
        partial.unlink(missing_ok=True)
    return found


class _Discard:
    """A context that stands in for no output file."""

    def __enter__(self) -> None:
        """Give no file."""

    def __exit__(self, *args: object) -> None:
        """Nothing to close."""


def cut_body(tail: bytes, fmt: str) -> str | None:
    """Return how a response body whose last bytes are TAIL was cut, or None if it ends whole.

    A whole N-Triples or Turtle body ends with the dot of a statement (or with a comment or,
    in Turtle, a prefix line); an RDF/XML body with a closing tag. A server may end the body
    of a query past its time limit mid-statement, with HTTP 200, or write a message after
    the last whole triple.
    """
    text = tail.rstrip()
    if not text:
        return None
    last = text.rsplit(b"\n", 1)[-1].strip()
    if fmt == "RDF_XML":
        whole = text.endswith(b">")
    else:
        heads = (b"#",) if fmt == "N_TRIPLES" else (b"#", b"@prefix", b"@base", b"prefix", b"base")
        whole = text.endswith(b".") or last.lower().startswith(heads)
    if whole:
        return None
    return f"the body ends with {last[-80:].decode('utf-8', 'replace')!r}, not a whole statement"


def count_file_triples(path: Path, fmt: str) -> tuple[int, int, str | None, int]:
    """Parse a gzip RDF file as a stream, without changing it.

    Return (triples, triples with a blank node, first syntax error, statements that are not
    read as triples): the triples include the lines read_statements would repair and the
    long statements; the last number counts the statements it would leave out and the lines
    that are not statements (-1: not known, in a Turtle or RDF/XML file).
    """
    found = read_statements(path, fmt)
    missing = (
        found.unparsed if found.unparsed < 0 else found.unparsed + sum(found.left_out.values())
    )
    return found.triples, found.blank_nodes, found.parse_error, missing


def strict_failures(path: Path, samples: int = 20) -> tuple[int, list[str]]:
    """Return how many lines of a gzip N-Triples file a strict parser refuses, and samples."""
    import pyoxigraph

    count = 0
    found: list[str] = []
    with gzip.open(path, "rb") as stream:
        for line in stream:
            if not line.strip() or line.lstrip().startswith(b"#"):
                continue
            try:
                list(pyoxigraph.parse(line, pyoxigraph.RdfFormat.N_TRIPLES))
            except (SyntaxError, MemoryError):
                count += 1
                if len(found) < samples:
                    found.append(line.decode("utf-8", "replace").rstrip()[:300])
    return count, found


class _UnparsableError(Exception):
    """A response with statements that do not parse: the graph cannot be verified."""

    def __init__(self, piece: Piece) -> None:
        """Keep the piece whose file does not parse."""
        super().__init__(piece.parse_error)
        self.piece = piece


class GraphExporter:
    """Export the graphs of one registry entry into *output*.

    cap is the LIMIT of each CONSTRUCT; max_bytes bounds the compressed bytes of the source;
    max_requests and max_seconds bound the CONSTRUCTs and the wall-clock time of the source.
    count_seconds is the budget of each COUNT and listing; read_timeout the longest silence of
    a CONSTRUCT response and request_seconds its longest duration.
    """

    def __init__(
        self,
        entry: dict[str, Any],
        output: Path,
        *,
        cap: int = DEFAULT_CAP,
        max_bytes: int = 300 * 10**9,
        max_requests: int = 5000,
        max_seconds: float = 72 * 3600.0,
        count_seconds: float = 900.0,
        read_timeout: float = 1800.0,
        request_seconds: float = 24 * 3600.0,
        delay: float = 1.0,
        retries: int = 4,
        retry_rounds: int = 2,
        retry_wait: float = 900.0,
    ) -> None:
        """Prepare the export; nothing is sent until run()."""
        from rdfsolve.sparql_helper import SparqlHelper

        if cap < 1 or max_bytes < 1 or max_requests < 1:
            raise ValueError("cap, max_bytes and max_requests must be positive")
        self.entry = entry
        self.name = str(entry["name"])
        self.output = Path(output)
        self.cap = cap
        self.max_bytes = max_bytes
        self.max_requests = max_requests
        self.max_seconds = max_seconds
        self.count_seconds = count_seconds
        self.read_timeout = read_timeout
        self.request_seconds = request_seconds
        self.delay = max(float(entry.get("delay") or 0.0), delay)
        self.retries = retries
        self.retry_rounds = retry_rounds
        self.retry_wait = retry_wait
        self.engine = str(entry.get("sparql_engine") or "").lower()
        self.helper = SparqlHelper.from_source_entry(entry, timeout=60.0, max_retries=2)
        self.helper.inter_request_delay = self.delay
        self.requests = 0
        self.bytes = 0
        self._started = time.monotonic()
        self._session: Any = None
        self.manifest: dict[str, Any] = {}
        self._previous: dict[str, Any] = {}
        # The engine's own graphs (left out of the default graph) and the row count at which
        # the server was seen to cut a CONSTRUCT (the page size of a paged predicate).
        self._engine_graphs: list[str] = []
        self._observed_cap: int | None = None
        # The COUNT of each named graph, for counting a default-graph remainder by difference.
        self._named_counts: dict[str, int | None] = {}
        # The lines a cut body of the current graph held: larger parts are asked in pages.
        self._cut_rows: int | None = None

    # Small queries go through SparqlHelper: host gate, spacing, busy-host waits.

    def _select(
        self, query: str, purpose: str, seconds: float | None = None
    ) -> list[dict[str, Any]]:
        with self.helper.budget(seconds or self.count_seconds, retries=2):
            rows = self.helper.select(query, purpose=purpose)
        bindings: list[dict[str, Any]] = rows.get("results", {}).get("bindings", [])
        return bindings

    def _ask(self, query: str, purpose: str, seconds: float | None = None) -> bool:
        with self.helper.budget(seconds or self.count_seconds, retries=2):
            answer = self.helper.select(query, purpose=purpose).get("boolean")
        if not isinstance(answer, bool):
            raise ValueError(f"No boolean answer to {purpose}")
        return answer

    def _count(self, where: str, prologue: str = "") -> tuple[int | None, str | None]:
        from rdfsolve.sparql_helper import SparqlHelperError

        query = f"{prologue}SELECT (COUNT(*) AS ?n) WHERE {{ {where} }}"
        try:
            rows = self._select(query, "export/count")
            return int(rows[0]["n"]["value"]), None
        except (SparqlHelperError, ValueError, KeyError, IndexError) as error:
            return None, str(error)[:300]

    # Graph listing.

    def list_graphs(self) -> tuple[list[str], dict[str, int], dict[str, Any]]:
        """Return the data graphs, the counts the listing gave, and how they were listed.

        The entry's graph_uris are taken as they are. Otherwise the endpoint's named graphs
        without the engine's own: one GROUP BY with counts, else an unordered DISTINCT (an
        ORDER BY reads every quad first: STRING, 18 s quiet and cut at 60 s by its gateway
        when busy, while the unordered DISTINCT answers in 0.25 s), else the paged listing,
        else the service description's graph names.
        """
        from rdfsolve.mining.graph_selection import described_graph_names
        from rdfsolve.schema_models._constants import SUGGESTED_SERVICE_GRAPHS
        from rdfsolve.sparql_helper import SparqlHelper, SparqlHelperError
        from rdfsolve.void_retrieval import discover_graph_names

        named = [str(g) for g in self.entry.get("graph_uris") or []]
        if named:
            return named, {}, {"method": "registry graph_uris", "excluded": []}
        record: dict[str, Any] = {"attempts": []}
        listed: list[str] | None = None
        counts: dict[str, int] = {}
        attempts: list[tuple[str, Callable[[], list[str]]]] = []

        def grouped() -> list[str]:
            """List the graphs with their counts in one GROUP BY."""
            rows = self._select(
                "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
                "export/graph-counts",
            )
            if SparqlHelper.row_cap_suspected(len(rows)):
                raise ValueError(f"{len(rows)} rows: a server row cap may have cut the listing")
            for row in rows:
                counts[row["g"]["value"]] = int(row["n"]["value"])
            return [row["g"]["value"] for row in rows]

        def distinct() -> list[str]:
            """List the graphs with an unordered DISTINCT."""
            rows = self._select(
                "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }", "export/graph-list"
            )
            if SparqlHelper.row_cap_suspected(len(rows)):
                raise ValueError(f"{len(rows)} rows: a server row cap may have cut the listing")
            return [row["g"]["value"] for row in rows]

        def paged() -> list[str]:
            """List the graphs in ordered pages."""
            with self.helper.budget(self.count_seconds, retries=2):
                return discover_graph_names(self.helper, batch_size=1000, max_pages=1000)

        attempts = [("group-by", grouped), ("distinct", distinct), ("paged", paged)]
        for method, call in attempts:
            try:
                listed = call()
                record["method"] = method
                break
            except (SparqlHelperError, ValueError, KeyError) as error:
                record["attempts"].append({"method": method, "error": str(error)[:300]})
                counts.clear()
        if listed is None:
            listed = described_graph_names(self.helper, seconds=self.count_seconds)
            if not listed:
                raise RuntimeError(f"Graphs not listed: {record['attempts']}")
            record["method"] = "service description (sd:name)"
        prefixes = (*SUGGESTED_SERVICE_GRAPHS, *VOCABULARY_GRAPHS)
        excluded = sorted(g for g in listed if g.startswith(prefixes))
        record["excluded"] = excluded
        record["listed"] = len(listed)
        graphs = sorted(g for g in listed if not g.startswith(prefixes))
        return graphs, {g: counts[g] for g in graphs if g in counts}, record

    # CONSTRUCT streaming.

    def _session_get(self) -> Any:
        if self._session is None:
            import requests

            from rdfsolve.sparql_helper import _KeepaliveAdapter

            self._session = requests.Session()
            for scheme in ("http://", "https://"):
                self._session.mount(scheme, _KeepaliveAdapter())
            if urlsplit(self.helper.endpoint_url).hostname in ("localhost", "127.0.0.1", "::1"):
                self._session.trust_env = False
        return self._session

    def _check_budget(self) -> None:
        if self.bytes > self.max_bytes:
            raise ExportBudgetError(f"source exceeds {self.max_bytes} bytes on disk")
        if self.requests >= self.max_requests:
            raise ExportBudgetError(f"source needs more than {self.max_requests} CONSTRUCTs")
        if time.monotonic() - self._started > self.max_seconds:
            raise ExportBudgetError(f"source export exceeds {self.max_seconds:.0f} s")

    def fetch(self, query: str, label: str) -> Piece:
        """Stream one CONSTRUCT to a gzip file and count its triples; raise _FetchError."""
        from rdfsolve._host_gate import HostBusyError, host_request
        from rdfsolve._http_policy import defer_host, retry_after_seconds
        from rdfsolve.sparql_helper import SparqlHelper, _Deadline

        self._check_budget()
        self.requests += 1
        piece = Piece(label=label, query=query)
        host = urlsplit(self.helper.endpoint_url).hostname or self.helper.endpoint_url
        partial = self.output / f".{label}.part"
        method = "POST" if self.helper._requires_post else "GET"
        started = time.monotonic()
        tries = 0
        accept = ACCEPT
        while True:
            tries += 1
            try:
                with (
                    host_request(host, timeout=3600.0, interval=self.delay, cooldown_wait=3600.0),
                    _Deadline(self.request_seconds) as deadline,
                ):
                    status, headers, body = self._stream(method, query, partial, piece, accept)
                    if deadline.expired:
                        raise _FetchError(f"no complete response in {self.request_seconds:.0f} s")
            except HostBusyError as error:
                raise _FetchError(f"host busy: {error}") from error
            except _FetchError as error:
                partial.unlink(missing_ok=True)
                logger.warning("%s: %s", label, str(error)[:300])
                raise
            except ExportBudgetError:
                partial.unlink(missing_ok=True)
                raise
            except Exception as error:  # a connection cut or reset mid-transfer
                partial.unlink(missing_ok=True)
                raise _FetchError(
                    f"transfer failed: {type(error).__name__}: {error}"[:500]
                ) from error
            if status == 406 and accept != FALLBACK_ACCEPT:
                accept = ACCEPT_ANY if accept == ACCEPT else FALLBACK_ACCEPT
                continue
            if status in (405, 414) and method == "GET":
                method = "POST"
                continue
            busy = status in (429, 503) or (
                status in _RETRYABLE
                and (
                    headers.get("Retry-After") is not None
                    or any(word in body.lower() for word in SparqlHelper.OVERLOAD_PATTERNS)
                )
            )
            # A gateway that cuts the query (502/504/524 without a busy sign) is not waited
            # for: the same CONSTRUCT would be cut again, so the graph is split at once.
            if busy and tries <= self.retries:
                wait = retry_after_seconds(headers.get("Retry-After"))
                wait = wait if wait is not None else SparqlHelper.overload_backoff(tries)
                logger.warning("%s: HTTP %s, %s waited out %.0f s", label, status, host, wait)
                defer_host(host, wait)
                continue
            if status >= 400:
                raise _FetchError(f"HTTP {status}: {body[:300]}")
            break
        piece.seconds = round(time.monotonic() - started, 1)
        media = _media(piece.content_type)
        suffix, fmt = _FORMATS[media]
        final = self.output / f"{label}.{suffix}.gz"
        partial.replace(final)
        piece.file = final.name
        self._read(piece, final, fmt)
        self.bytes += piece.bytes
        if piece.unparsed:
            # The file is kept, but its triples cannot be checked against the count.
            raise _UnparsableError(piece)
        if piece.parse_error and fmt == "N_TRIPLES":
            piece.invalid_count, piece.invalid_samples = strict_failures(final)
        logger.info(
            "%s: %s triples, %s bytes in %.0f s", label, piece.triples, piece.bytes, piece.seconds
        )
        return piece

    def _read(
        self, piece: Piece, path: Path, fmt: str, records: list[dict[str, Any]] | None = None
    ) -> None:
        """Read the piece's file (rewritten with its repairs), fill its counts and digests.

        The repaired and left-out lines (and RECORDS, those of merged pages) are written to
        the piece's statements record.
        """
        found = read_statements(path, fmt, rewrite=True)
        piece.bytes = path.stat().st_size
        piece.sha256 = _sha256(path)
        piece.triples, piece.blank_nodes = found.triples, found.blank_nodes
        piece.parse_error, piece.unparsed = found.parse_error, found.unparsed
        piece.long_statements = found.long_statements
        kept = list(records or [])
        if found.records:
            kept += [{"file": path.name, **record} for record in found.records]
        piece.repaired = found.repaired + sum(1 for r in records or [] if r["kind"] == "repaired")
        left_out: dict[str, int] = dict(found.left_out)
        for record in records or []:
            if record["kind"] in LEFT_OUT_KINDS:
                left_out[record["kind"]] = left_out.get(record["kind"], 0) + 1
        piece.left_out = left_out
        if kept:
            target = self.output / f"{piece.label}{STATEMENTS_SUFFIX}"
            body = {"kinds": STATEMENT_KINDS, "file": path.name, "lines": kept}
            target.write_text(json.dumps(body, indent=1, ensure_ascii=False) + "\n", "utf-8")
            piece.statements_file = target.name
            piece.statements_sha256 = _sha256(target)

    def _discard(self, piece: Piece) -> None:
        """Delete a piece's file and its statements record; take its bytes off the budget."""
        if piece.file:
            (self.output / piece.file).unlink(missing_ok=True)
            self.bytes -= piece.bytes
        if piece.statements_file:
            (self.output / piece.statements_file).unlink(missing_ok=True)
        piece.file = piece.statements_file = ""

    def _stream(
        self, method: str, query: str, partial: Path, piece: Piece, accept: str = ""
    ) -> tuple[int, dict[str, str], str]:
        """Send the request; write a 200 body to *partial*; return status, headers, error body."""
        session = self._session_get()
        headers = {"Accept": accept or ACCEPT, "User-Agent": self.helper.user_agent}
        kwargs: dict[str, Any] = {
            "headers": headers,
            "timeout": (60.0, self.read_timeout),
            "stream": True,
        }
        if method == "GET":
            kwargs["params"] = {"query": query}
        else:
            kwargs["data"] = {"query": query}
        with session.request(method, self.helper.endpoint_url, **kwargs) as response:
            status = int(response.status_code)
            answer = dict(response.headers.items())
            if status != 200:
                text = response.raw.read(65536, decode_content=True) or b""
                return status, answer, text.decode("utf-8", "replace")
            piece.content_type = answer.get("Content-Type", "")
            chunks = response.iter_content(chunk_size=1 << 20)
            first = next(chunks, b"")
            if _media(piece.content_type) not in _FORMATS:
                start = first.lstrip()[:2048]
                # QLever can send N-Triples without a Content-Type: a body
                # that starts as Turtle or N-Triples does (or is empty) is read as Turtle, of
                # which N-Triples is a subset.
                if piece.content_type or not (
                    not start or start.startswith((b"<", b"_:", b"@prefix", b"PREFIX", b"@base"))
                ):
                    raise _FetchError(
                        f"not RDF: {piece.content_type!r}: {start.decode('utf-8', 'replace')[:200]}"
                    )
                # Asked for N-Triples alone, a body of IRIs and blank nodes is N-Triples.
                asked = (
                    ACCEPT
                    if accept == ACCEPT and not start.startswith((b"@", b"PREFIX"))
                    else (FALLBACK_ACCEPT)
                )
                piece.content_type = f"{asked} (no Content-Type sent)"
            if answer.get("X-SQL-State") == "S1TAT":
                raise _FetchError("Virtuoso ANYTIME partial result (X-SQL-State S1TAT)")
            size = lines = 0
            tail = b""
            with gzip.open(partial, "wb", compresslevel=3) as sink:
                for chunk in _chain(first, chunks):
                    sink.write(chunk)
                    size += len(chunk)
                    lines += chunk.count(b"\n")
                    tail = (tail + chunk)[-_TAIL_BYTES:]
                    if any(marker in tail for marker in ERROR_TRAILERS):
                        raise _FetchError(
                            "the server ended the body with an error trailer", received=lines
                        )
                    if size // 4 > self.max_bytes - self.bytes:
                        raise ExportBudgetError("source would exceed its byte budget")
            piece.uncompressed_bytes = size
            cut = cut_body(tail, _FORMATS[_media(piece.content_type)][1])
            if cut:
                raise _FetchError(f"cut: {cut} after {lines} lines", received=lines)
            if size and size < 4096:
                with gzip.open(partial, "rb") as check:
                    start = check.read(512).lstrip().lower()
                if start.startswith((b"<!doctype", b"<html")):
                    raise _FetchError("HTML page instead of RDF")
            return status, answer, ""

    # One graph.

    def _prologue(self) -> str:
        """Return the Virtuoso pragmas that leave the engine's graphs out of the default graph."""
        from rdfsolve.sparql_helper import graph_exclusion_prologue

        if not self._engine_graphs or not self.engine.startswith("virtuoso"):
            return ""
        return graph_exclusion_prologue(self._engine_graphs)

    def _graph_terms(self, graph: str | None, role: str) -> tuple[str, str]:
        """Return the WHERE body (over ?s ?p ?o) and the prologue of a graph."""
        if graph is not None:
            return f"GRAPH {URIRef(graph).n3()} {{ ?s ?p ?o }}", ""
        if role == "default-extra":
            return "?s ?p ?o FILTER NOT EXISTS { GRAPH ?g { ?s ?p ?o } }", self._prologue()
        return "?s ?p ?o", self._prologue()

    def export_graph(self, outcome: GraphOutcome, label: str) -> None:
        """Fill *outcome*: whole graph, else by predicate, else predicate pages."""
        where, prologue = self._graph_terms(outcome.graph, outcome.role)
        outcome.started = _now()
        began = time.monotonic()
        self._cut_rows = None
        if outcome.count is None and outcome.count_error is None:
            outcome.count, outcome.count_error = self._count(where, prologue)
        try:
            if outcome.count == 0:
                outcome.method, outcome.outcome = "empty", "retrieved/verified"
                return
            whole = self._try_whole(outcome, where, prologue, label)
            if not whole and self._unreadable(outcome, where, prologue):
                return
            if not whole:
                self._by_predicate(outcome, where, prologue, label)
        except _UnparsableError as error:
            piece = error.piece
            what = "statements do not parse" if piece.unparsed < 0 else "lines are not statements"
            piece.error = f"{piece.unparsed} {what}"
            outcome.pieces.append(piece)
            outcome.outcome = "failed"
            outcome.reason = (
                f"{piece.file}: {piece.unparsed} {what} (first: {piece.parse_error})"
            )[:500]
        except ExportBudgetError as error:
            outcome.outcome, outcome.reason = "failed", f"budget: {error}"
            raise
        finally:
            outcome.finished = _now()
            outcome.seconds = round(time.monotonic() - began, 1)
            kept = [p for p in outcome.pieces if p.error is None]
            outcome.triples = sum(p.triples or 0 for p in kept)
            outcome.repaired = sum(p.repaired for p in kept)
            outcome.left_out = {}
            for piece in kept:
                for kind, number in piece.left_out.items():
                    outcome.left_out[kind] = outcome.left_out.get(kind, 0) + number
        if outcome.outcome == "retrieved/unverified" and outcome.role == "default-extra":
            self._verify_remainder(outcome)

    def _verify_remainder(self, outcome: GraphOutcome) -> None:
        """Verify a default-graph remainder whose COUNT was refused by counting it another way."""
        count, method = self._remainder_count()
        if count is not None and count == outcome.triples:
            outcome.count, outcome.count_method = count, method
            outcome.outcome = "retrieved/verified"
        else:
            outcome.count_method = f"not verified: {method} gives {count}"

    # RDF4J and GraphDB keep the triples loaded without a graph in this context; GRAPH ?g does
    # not match it, but it can be named.
    NULL_CONTEXT = "http://www.openrdf.org/schema/sesame#nil"

    def _remainder_count(self) -> tuple[int | None, str]:
        """Count the default-graph triples that no named graph holds, another way.

        When the endpoint refuses COUNT over FILTER NOT EXISTS (ENPKG's GraphDB), the null
        context of RDF4J/GraphDB is counted by name; else, when the endpoint counts its default
        graph and every named graph, the remainder is their difference, which is exact only when
        the default graph is their union and the named graphs share no triple. The caller
        accepts a count only when the exported triples equal it, and records the method.
        """
        count, _ = self._count(f"GRAPH <{self.NULL_CONTEXT}> {{ ?s ?p ?o }}")
        if count:
            return count, f"COUNT of the null context <{self.NULL_CONTEXT}>"
        total, _ = self._count("?s ?p ?o", self._prologue())
        named = self._named_counts
        if total is not None and named and all(c is not None for c in named.values()):
            return total - sum(c for c in named.values() if c is not None), (
                "COUNT of the default graph minus the COUNTs of the named graphs"
            )
        return None, "not counted: the endpoint refused every count of the remainder"

    def _unreadable(self, outcome: GraphOutcome, where: str, prologue: str) -> bool:
        """Mark a graph that the endpoint counts but whose triples no query reads.

        An endpoint can count triples in graphs (vocabulary graphs it ships, private graphs)
        while CONSTRUCT and SELECT of them answer nothing: nothing can be exported or mined from
        them.
        """
        from rdfsolve.sparql_helper import SparqlHelperError

        empty = [p for p in outcome.pieces if p.error and p.error.startswith("incomplete: 0 ")]
        if not empty or not outcome.count:
            return False
        try:
            rows = self._select(f"{prologue}SELECT ?s WHERE {{ {where} }} LIMIT 1", "export/read")
        except (SparqlHelperError, ValueError):
            return False
        if rows:
            return False
        outcome.method = "none"
        outcome.outcome = "unreadable"
        outcome.reason = (
            f"the endpoint counts {outcome.count} triples, but CONSTRUCT and SELECT read none"
        )
        return True

    def _verdict(self, got: int, expected: int | None) -> str | None:
        """Return 'verified'/'unverified' for a complete result, or None for an incomplete one."""
        from rdfsolve.sparql_helper import SparqlHelper

        if expected is not None:
            return "verified" if got == expected else None
        cut = SparqlHelper.row_cap_suspected(got) or SparqlHelper.row_cap_suspected(got - 1)
        if got < self.cap and not cut:
            return "unverified"
        return None

    def _try_whole(self, outcome: GraphOutcome, where: str, prologue: str, label: str) -> bool:
        if outcome.count is not None and outcome.count > self.cap:
            outcome.reason = f"count {outcome.count} exceeds the cap {self.cap}"
            return False
        query = f"{prologue}CONSTRUCT {{ ?s ?p ?o }} WHERE {{ {where} }} LIMIT {self.cap}"
        try:
            piece = self.fetch(query, f"{label}.whole")
        except _FetchError as error:
            outcome.pieces.append(Piece(label=f"{label}.whole", query=query, error=str(error)))
            outcome.reason = f"whole graph: {error}"[:300]
            self._cut_rows = error.received or self._cut_rows
            return False
        verdict = self._verdict(accounted(piece), outcome.count)
        if verdict is None:
            # Kept for the record, but not as a piece of the graph.
            self._discard(piece)
            piece.error = f"incomplete: {accounted(piece)} statements, count {outcome.count}"
            outcome.pieces.append(piece)
            outcome.reason = f"whole graph incomplete ({piece.triples} of {outcome.count})"
            self._observed_cap = piece.triples or None
            return False
        outcome.pieces.append(piece)
        outcome.method = "whole"
        outcome.outcome = f"retrieved/{verdict}"
        return True

    def _predicates(self, where: str, prologue: str) -> list[tuple[str, int | None]]:
        from rdfsolve.sparql_helper import SparqlHelper, SparqlHelperError

        body = where
        try:
            rows = self._select(
                f"{prologue}SELECT ?p (COUNT(*) AS ?n) WHERE {{ {body} }} GROUP BY ?p",
                "export/predicate-counts",
            )
            if not SparqlHelper.row_cap_suspected(len(rows)):
                return [(r["p"]["value"], int(r["n"]["value"])) for r in rows]
        except (SparqlHelperError, ValueError, KeyError):
            pass
        rows = self._select(
            f"{prologue}SELECT DISTINCT ?p WHERE {{ {body} }}", "export/predicate-list"
        )
        if SparqlHelper.row_cap_suspected(len(rows)):
            raise ValueError(f"{len(rows)} predicates: a server row cap may have cut the list")
        return [(r["p"]["value"], None) for r in rows]

    def _by_predicate(self, outcome: GraphOutcome, where: str, prologue: str, label: str) -> None:
        from rdfsolve.sparql_helper import SparqlHelperError

        try:
            predicates = self._predicates(where, prologue)
        except (SparqlHelperError, ValueError, KeyError) as error:
            outcome.outcome = "failed"
            outcome.reason = f"{outcome.reason}; predicates not listed: {error}"[:500]
            return
        outcome.method = "by-predicate"
        verdicts: list[str] = []
        for index, (predicate, expected) in enumerate(predicates, 1):
            p = URIRef(predicate).n3()
            # Every occurrence: the default-graph remainder repeats the pattern in its filter.
            sub = where.replace("?s ?p ?o", f"?s {p} ?o")
            if expected is None:
                expected, _ = self._count(sub, prologue)
            stem = f"{label}.p{index:04d}"
            verdict: str | None = None
            # A predicate larger than a cut body goes to pages at once.
            ceiling = min(self.cap, self._observed_cap or self.cap, self._cut_rows or self.cap)
            if expected is None or expected <= ceiling:
                query = f"{prologue}CONSTRUCT {{ ?s {p} ?o }} WHERE {{ {sub} }} LIMIT {self.cap}"
                try:
                    piece = self.fetch(query, stem)
                    verdict = self._verdict(accounted(piece), expected)
                    if verdict is None:
                        self._discard(piece)
                        piece.error = f"incomplete: {accounted(piece)} statements, count {expected}"
                        self._observed_cap = piece.triples or self._observed_cap
                    outcome.pieces.append(piece)
                except _FetchError as error:
                    outcome.pieces.append(Piece(label=stem, query=query, error=str(error)))
                    self._cut_rows = error.received or self._cut_rows
            if verdict is None:
                verdict = self._paged(outcome, sub, p, prologue, expected, stem)
            if verdict is None:
                outcome.outcome = "failed"
                outcome.reason = f"predicate {predicate} not retrieved whole"[:500]
                return
            verdicts.append(verdict)
        total = sum(accounted(p) for p in outcome.pieces if p.error is None)
        if outcome.count is not None and total != outcome.count:
            outcome.outcome = "failed"
            outcome.reason = (
                f"predicate pieces hold {total} statements, graph count {outcome.count}"
            )
            return
        exact = outcome.count is not None and "unverified" not in verdicts
        outcome.outcome = "retrieved/verified" if exact else "retrieved/unverified"
        outcome.reason = ""

    def _page_size(self) -> int:
        """Return the page size: the server's row cap seen on a cut result, else the cap.

        Virtuoso can answer ResultSetMaxRows + 1 rows, so the size is taken
        down to the known cap below it.
        """
        from rdfsolve.sparql_helper import SparqlHelper

        seen = self._observed_cap
        if not seen or seen >= self.cap:
            return self.cap
        known = [c for c in SparqlHelper.SUSPECTED_ROW_CAPS if c <= seen]
        return max(known) if known else seen

    def _paged(
        self,
        outcome: GraphOutcome,
        sub: str,
        p: str,
        prologue: str,
        expected: int | None,
        stem: str,
    ) -> str | None:
        """Fetch one predicate in LIMIT/OFFSET pages; verify the distinct triples by its count.

        Pages are first asked without ORDER BY: an engine that sorts only a bounded number of
        rows refuses ORDER BY with a deep OFFSET (Virtuoso: "SR353: Sorted TOP clause specifies
        more then 10001 rows to sort", NanoSafety). Unordered pages may overlap or skip, so they
        are merged into one file of distinct N-Triples lines, which must hold as many triples as
        the endpoint counts: overlap or a skip leaves fewer. When they do not, pages in ORDER BY
        ?s ?o are tried.
        """
        if expected is None:
            outcome.reason = f"{outcome.reason}; {p}: no count, pages cannot be verified"
            return None
        size = self._page_size()
        if self._cut_rows:
            # Half of what came before a cut, so that a page ends in time.
            size = min(size, max(MIN_PAGE, self._cut_rows // 2))
        for ordered in (False, True):
            pages: list[Piece] = []
            offset = page = 0
            failed = False
            while offset < expected:
                page += 1
                select = f"SELECT ?s ?o WHERE {{ {sub} }}"
                if ordered:
                    select += " ORDER BY ?s ?o"
                query = (
                    f"{prologue}CONSTRUCT {{ ?s {p} ?o }} WHERE {{ {{ {select} "
                    f"LIMIT {size} OFFSET {offset} }} }}"
                )
                label = f"{stem}.{'o' if ordered else 'u'}{page:05d}"
                try:
                    piece = self.fetch(query, label)
                except _FetchError as error:
                    outcome.pieces.append(Piece(label=label, query=query, error=str(error)))
                    if size > MIN_PAGE:
                        # A refused or cut page is asked again, smaller: Virtuoso refuses a
                        # CONSTRUCT whose triples overflow its hash dictionary ; a
                        # page cut after some lines is asked at half of them.
                        got = error.received or 0
                        size = max(MIN_PAGE, got // 2 if 0 < got < size else size // 4)
                        self._cut_rows = got or self._cut_rows
                        page -= 1
                        continue
                    outcome.reason = f"{outcome.reason}; {p} page {page}: {error}"[:500]
                    failed = True
                    break
                pages.append(piece)
                got = accounted(piece)
                offset += got
                if got < size:
                    break
            merged = (
                None
                if failed or not pages
                else self._merge(pages, f"{stem}.{'o' if ordered else 'u'}")
            )
            for piece in pages:
                self._discard(piece)
            if merged is not None:
                outcome.pieces.append(merged)
                if accounted(merged) == expected:
                    return "verified"
                merged.error = (
                    f"{accounted(merged)} distinct statements in {merged.pages} pages, "
                    f"count {expected}"
                )
                self._discard(merged)
                outcome.reason = f"{outcome.reason}; {p}: {merged.error}"[:500]
        return None

    def _merge(self, pages: list[Piece], stem: str) -> Piece:
        """Merge page files into one gzip N-Triples file of distinct lines (LC_ALL=C sort -u)."""
        import subprocess
        import threading

        import pyoxigraph

        target = self.output / f"{stem}.nt.gz"
        env = {**os.environ, "LC_ALL": "C"}
        sort = subprocess.Popen(
            ["sort", "-u", "-S", "1G", "-T", str(self.output)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=env,
        )
        if sort.stdin is None or sort.stdout is None:
            raise RuntimeError("sort has no pipes")

        def feed(sink: IO[bytes]) -> None:
            """Write the N-Triples lines of every page to sort, then close its input."""
            try:
                for piece in pages:
                    path = self.output / piece.file
                    if piece.file.endswith(".nt.gz"):
                        with gzip.open(path, "rb") as stream:
                            shutil.copyfileobj(stream, sink, 1 << 20)
                        continue
                    fmt = getattr(pyoxigraph.RdfFormat, _FORMATS[_media(piece.content_type)][1])
                    with gzip.open(path, "rb") as stream:
                        quads = pyoxigraph.parse(cast(IO[bytes], stream), fmt, lenient=True)
                        pyoxigraph.serialize(quads, sink, pyoxigraph.RdfFormat.N_TRIPLES)
            finally:
                sink.close()

        writer = threading.Thread(target=feed, args=(sort.stdin,), daemon=True)
        writer.start()
        size = 0
        with gzip.open(target, "wb", compresslevel=3) as out:
            for line in sort.stdout:
                if line.strip():
                    out.write(line)
                    size += len(line)
        writer.join()
        if sort.wait() != 0:
            raise _FetchError(f"sort -u of the pages of {stem} failed")
        merged = Piece(
            label=stem,
            query=pages[0].query,
            file=target.name,
            content_type="application/n-triples",
            uncompressed_bytes=size,
            seconds=round(sum(p.seconds for p in pages), 1),
            pages=len(pages),
        )
        self._read(merged, target, "N_TRIPLES", self._page_records(pages))
        self.bytes += merged.bytes
        return merged

    def _page_records(self, pages: list[Piece]) -> list[dict[str, Any]]:
        """Return the repaired and left-out lines of pages, each distinct statement once.

        Unordered pages may overlap: a statement in two pages is one statement of the merged
        file, as sort -u keeps one of its lines. Long statements are found again in the
        merged file.
        """
        seen: set[tuple[str, str]] = set()
        records: list[dict[str, Any]] = []
        for piece in pages:
            if not piece.statements_file:
                continue
            body = json.loads((self.output / piece.statements_file).read_text("utf-8"))
            for record in body["lines"]:
                key = (record["kind"], str(record.get("written") or record.get("original")))
                if record["kind"] != "long" and key not in seen:
                    seen.add(key)
                    records.append({**record, "page": piece.label})
        return records

    # The source.

    def _default_graph_role(
        self, graphs: list[str], counts: dict[str, int]
    ) -> tuple[str | None, str]:
        """Decide whether the default graph is exported: (role or None, why)."""
        from rdfsolve.sparql_helper import SparqlHelperError

        if self.entry.get("graph_uris"):
            return None, "the entry names its graphs"
        if not graphs:
            return "default", "no named data graph"
        if self.engine.startswith("virtuoso"):
            # Every Virtuoso quad has a graph: its default graph is the union of the named ones.
            # The comparison reads every quad and is cut on large endpoints (STRING: HTTP 502).
            return None, "virtuoso: the default graph is the union of the named graphs"
        prologue = self._prologue()
        query = f"{prologue}ASK {{ ?s ?p ?o FILTER NOT EXISTS {{ GRAPH ?g {{ ?s ?p ?o }} }} }}"
        try:
            extra = self._ask(query, "export/default-extra")
        except (SparqlHelperError, ValueError) as error:
            if self.engine.startswith(UNION_DEFAULT_ENGINES):
                return None, f"not answered ({str(error)[:120]}); {self.engine}: union default"
            # A default graph without triples needs no comparison (Fuseki keeps it apart).
            count, _ = self._count("?s ?p ?o", prologue)
            if count == 0:
                return None, "the default graph holds no triple"
            if count is not None and len(counts) == len(graphs) and count == sum(counts.values()):
                # Fuseki with a union default graph.
                return None, f"the default graph counts {count}, as many as the named graphs"
            # RDF4J and GraphDB: the default graph is the named graphs and the null context; a
            # null context that holds triples is exported as a graph (also when the store refuses
            # the default-graph check as too long).
            null, _ = self._count(f"GRAPH <{self.NULL_CONTEXT}> {{ ?s ?p ?o }}")
            if null:
                return (
                    "null-context",
                    f"check not answered; the null context <{self.NULL_CONTEXT}> holds {null} triples",
                )
            return "undetermined", f"not answered: {str(error)[:200]}; default count {count}"
        if extra:
            # RDF4J and GraphDB keep the triples loaded without a graph in a null context that
            # GRAPH ?g does not match but that can be named: exported whole, it is countable (a
            # count of its triples outside every named graph can be refused, and some of them
            # are also in named graphs).
            null, _ = self._count(f"GRAPH <{self.NULL_CONTEXT}> {{ ?s ?p ?o }}")
            if null:
                return (
                    "null-context",
                    f"the null context <{self.NULL_CONTEXT}> holds {null} triples",
                )
            return "default-extra", "the default graph holds triples that no named graph holds"
        return None, "every default-graph triple is in a named graph"

    def _write_manifest(self) -> None:
        self.manifest["bytes"] = self.bytes
        self.manifest["requests"] = self.requests
        path = self.output / MANIFEST
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(self.manifest, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)

    def _resumable(self, graph: str | None, role: str) -> GraphOutcome | None:
        """Return a retrieved outcome of an earlier run whose files are still on disk."""
        for item in self._previous.get("graphs", []):
            if item.get("graph") != graph or item.get("role") != role:
                continue
            if not str(item.get("outcome", "")).startswith("retrieved"):
                # The files of a graph that was not retrieved (a cut body kept for the record)
                # are not part of the new export.
                for piece in item.get("pieces", []):
                    for name in (piece.get("file"), piece.get("statements_file")):
                        if name:
                            (self.output / name).unlink(missing_ok=True)
                return None
            pieces = [Piece(**p) for p in item.get("pieces", [])]
            if not all(not p.file or (self.output / p.file).is_file() for p in pieces):
                return None
            data = {k: v for k, v in item.items() if k != "pieces"}
            outcome = GraphOutcome(pieces=pieces, **data)
            if outcome.outcome == "retrieved/unverified":
                self._recount(outcome)
            return outcome
        return None

    def _recount(self, outcome: GraphOutcome) -> None:
        """Ask again for the count of a graph retrieved unverified; verify it if it equals.

        A count that differs sends the graph to be exported again.
        """
        where, prologue = self._graph_terms(outcome.graph, outcome.role)
        count, error = self._count(where, prologue)
        if count is None and outcome.role == "default-extra":
            self._verify_remainder(outcome)
            return
        if count is None:
            outcome.count_error = error
            return
        outcome.count = count
        if count == outcome.triples + sum(outcome.left_out.values()):
            outcome.outcome = "retrieved/verified"
        else:
            outcome.outcome = "failed"
            outcome.reason = f"recounted {count}, the files hold {outcome.triples}"

    def run(self) -> dict[str, Any]:
        """Export every graph of the source; return the manifest (also written to disk)."""
        self.output.mkdir(parents=True, exist_ok=True)
        previous = self.output / MANIFEST
        if previous.is_file():
            self._previous = json.loads(previous.read_text(encoding="utf-8"))
        self.manifest = {
            "format": EXPORT_FORMAT,
            "source": self.name,
            "endpoint": self.helper.endpoint_url,
            "sparql_engine": self.engine,
            "cap": self.cap,
            "started": _now(),
            "finished": None,
            "graphs": [],
            "outcome": "running",
            # CONSTRUCTs sent by earlier runs of this export (resumed graphs are not sent again).
            "requests_before": int(self._previous.get("requests_before") or 0)
            + int(self._previous.get("requests") or 0),
        }
        self._write_manifest()
        try:
            self._run()
        except ExportBudgetError as error:
            self.manifest["outcome"] = "stopped"
            self.manifest["reason"] = str(error)
        except Exception as error:
            self.manifest["outcome"] = "failed"
            self.manifest["reason"] = f"{type(error).__name__}: {error}"[:1000]
            logger.exception("Export of %s failed", self.name)
        else:
            done = _settled(self.manifest["graphs"])
            self.manifest["outcome"] = "complete" if done else "partial"
        finally:
            self.manifest["finished"] = _now()
            self.manifest["seconds"] = round(time.monotonic() - self._started, 1)
            if self.helper.redirected_from:
                self.manifest["redirected_from"] = self.helper.redirected_from
            self._write_manifest()
            for stray in self.output.glob(".*.part"):
                stray.unlink(missing_ok=True)
        return self.manifest

    VERSION_QUERY = (
        "SELECT DISTINCT ?d ?p ?v WHERE { VALUES ?p { "
        "<http://purl.org/pav/version> <http://purl.org/dc/terms/modified> "
        "<http://purl.org/dc/terms/issued> <http://www.w3.org/2002/07/owl#versionInfo> "
        "<http://schema.org/version> } ?d ?p ?v . "
        "{ ?d a <http://rdfs.org/ns/void#Dataset> } UNION { ?d a <http://www.w3.org/ns/dcat#Dataset> }"
        " UNION { ?d a <http://schema.org/Dataset> } } LIMIT 100"
    )

    def source_versions(self) -> list[dict[str, str]] | str:
        """Return the versions and dates that the endpoint states of its datasets, or why not.

        An endpoint may serve another release than its description elsewhere; the export
        records what
        the endpoint itself says when it was exported.
        """
        from rdfsolve.sparql_helper import SparqlHelperError

        try:
            rows = self._select(self.VERSION_QUERY, "export/versions", 60.0)
        except (SparqlHelperError, ValueError) as error:
            return f"not answered: {str(error)[:200]}"
        return [
            {"dataset": r["d"]["value"], "property": r["p"]["value"], "value": r["v"]["value"]}
            for r in rows
            if {"d", "p", "v"} <= set(r)
        ]

    def _run(self) -> None:
        self.manifest["source_versions"] = self.source_versions()
        graphs, counts, listing = self.list_graphs()
        self._named_counts = {g: counts.get(g) for g in graphs}
        self._engine_graphs = list(listing.get("excluded", []))
        self.manifest["listing"] = listing
        role, why = self._default_graph_role(graphs, counts)
        self.manifest["default_graph"] = {"role": role, "why": why}
        todo: list[tuple[str | None, str]] = [(g, "named") for g in graphs]
        if role == "undetermined":
            # Not exported by the retry rounds (_transient): a later run decides it again.
            self.manifest["graphs"].append(
                asdict(
                    GraphOutcome(
                        None, "default", method="undetermined", outcome="failed", reason=why
                    )
                )
            )
        elif role == "null-context":
            todo.append((self.NULL_CONTEXT, "named"))
        elif role is not None:
            todo.append((None, role))
        self._write_manifest()
        for index, (graph, kind) in enumerate(todo, 1):
            outcome = self._resumable(graph, kind)
            if outcome is not None and outcome.outcome == "failed":
                for piece in outcome.pieces:
                    self._discard(piece)
                outcome = None
            if outcome is None:
                outcome = GraphOutcome(graph, kind, count=counts.get(graph or ""))
                try:
                    self.export_graph(outcome, f"g{index:04d}")
                finally:
                    self.manifest["graphs"].append(asdict(outcome))
                    self._write_manifest()
            else:
                self.bytes += sum(p.bytes for p in outcome.pieces if p.file)
                self.manifest["graphs"].append(asdict(outcome))
                self._write_manifest()
            if graph is not None:
                self._named_counts[graph] = outcome.count
        self._retry_transient(todo)
        self._record_quality()

    def _record_quality(self) -> None:
        """List in the manifest the graphs the endpoint counts but never serves, and the files
        that parse only leniently (invalid IRIs and the like), with samples.
        """
        graphs = self.manifest["graphs"]
        self.manifest["unreadable_graphs"] = [
            {"graph": g["graph"], "count": g["count"]}
            for g in graphs
            if g["outcome"] == "unreadable"
        ]
        self.manifest["lenient_parses"] = [
            {
                "graph": g["graph"],
                "file": p["file"],
                "first_error": p["parse_error"],
                "invalid_statements": p.get("invalid_count"),
                "samples": p.get("invalid_samples") or [],
            }
            for g in graphs
            for p in g["pieces"]
            if p.get("file") and p.get("parse_error") and not p.get("error")
        ]
        # Lines repaired or left out, and long statements, by file (records beside each file).
        self.manifest["statements"] = statement_summary(graphs)
        self._write_manifest()

    def _retry_transient(self, todo: list[tuple[str | None, str]]) -> None:
        """Export again, after a wait, the graphs that failed on a busy or cut connection.

        A busy host (STRING answers HTTP 502 while it is loaded) is not a verdict on the
        graph: up to retry_rounds rounds, retry_wait seconds apart, are made in this run, and
        a later run resumes whatever is still not retrieved.
        """
        for round_ in range(1, self.retry_rounds + 1):
            failed = [
                (i, g)
                for i, g in enumerate(self.manifest["graphs"])
                if g["outcome"] == "failed" and _transient(g)
            ]
            if not failed:
                return
            logger.warning(
                "%d graphs failed on a busy or cut connection; again in %.0f s (round %d of %d)",
                len(failed),
                self.retry_wait,
                round_,
                self.retry_rounds,
            )
            time.sleep(self.retry_wait)
            for i, item in failed:
                self._check_budget()
                for piece in item["pieces"]:
                    for name in (piece.get("file"), piece.get("statements_file")):
                        if name:
                            (self.output / name).unlink(missing_ok=True)
                outcome = GraphOutcome(item["graph"], item["role"], count=item.get("count"))
                index = 1 + next(
                    (
                        k
                        for k, (g, r) in enumerate(todo)
                        if g == item["graph"] and r == item["role"]
                    ),
                    i,
                )
                try:
                    self.export_graph(outcome, f"g{index:04d}r{round_}")
                finally:
                    self.manifest["graphs"][i] = asdict(outcome)
                    self._write_manifest()


# Errors of a busy host or a cut connection, not of the query.
_TRANSIENT = (
    "http 429",
    "http 502",
    "http 503",
    "http 504",
    "http 52",
    "transfer failed",
    "host busy",
)


def statement_summary(graphs: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Return the repaired, left-out and long statements of the kept files of an export.

    left_out counts by kind the statements that the endpoint counts but that RDF cannot hold
    (STATEMENT_KINDS); they are the gap between the files and the endpoint.
    """
    files = []
    left_out: dict[str, int] = {}
    repaired = long = 0
    for graph in graphs:
        for piece in graph.get("pieces", []):
            if not piece.get("file") or piece.get("error"):
                continue
            kinds = piece.get("left_out") or {}
            if not (kinds or piece.get("repaired") or piece.get("long_statements")):
                continue
            files.append(
                {
                    "graph": graph["graph"],
                    "file": piece["file"],
                    "repaired": piece.get("repaired", 0),
                    "left_out": kinds,
                    "long_statements": piece.get("long_statements", 0),
                    "record": piece.get("statements_file", ""),
                }
            )
            repaired += int(piece.get("repaired") or 0)
            long += int(piece.get("long_statements") or 0)
            for kind, number in kinds.items():
                left_out[kind] = left_out.get(kind, 0) + int(number)
    return {"repaired": repaired, "left_out": left_out, "long": long, "files": files}


def _transient(graph: dict[str, Any]) -> bool:
    """Return whether a failed graph failed on a busy host or a cut connection.

    A default graph whose role was not decided is not: exporting it whole would export the
    union of the named graphs where the default graph is their union.
    """
    if graph.get("method") == "undetermined":
        return False
    texts = [str(graph.get("reason") or "")] + [str(p.get("error") or "") for p in graph["pieces"]]
    return any(word in text.lower() for text in texts for word in _TRANSIENT)


def _settled(graphs: Sequence[dict[str, Any]]) -> bool:
    """Every graph retrieved, or unreadable at the endpoint; at least one retrieved."""
    outcomes = [str(g.get("outcome", "")) for g in graphs]
    return any(o.startswith("retrieved") for o in outcomes) and all(
        o.startswith("retrieved") or o == "unreadable" for o in outcomes
    )


def source_complete(manifest: dict[str, Any]) -> bool:
    """Return whether every graph of an export manifest was retrieved (or is unreadable)."""
    graphs = manifest.get("graphs") or []
    return (
        manifest.get("format") == EXPORT_FORMAT
        and manifest.get("outcome") == "complete"
        and _settled(graphs)
    )


def export_source(entry: dict[str, Any], root: Path, stamp: str, **options: Any) -> dict[str, Any]:
    """Export one entry into ROOT/<name>/<stamp>/ and return its manifest."""
    output = Path(root) / str(entry["name"]) / stamp
    done = output / MANIFEST
    if done.is_file() and not options.pop("force", False):
        manifest: dict[str, Any] = json.loads(done.read_text(encoding="utf-8"))
        if source_complete(manifest):
            # A complete export may be pinned by the registry (manifest_sha256): not rewritten.
            logger.info("%s: export complete at %s, kept", entry["name"], output)
            return manifest
    options.pop("force", None)
    Path(root).mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(root).free
    floor = int(options.pop("min_free_bytes", 500 * 10**9))
    if free < floor:
        raise ExportBudgetError(f"only {free} bytes free under {root}")
    return GraphExporter(entry, output, **options).run()


def entries_on_host(entries: Sequence[dict[str, Any]], host: str) -> Iterator[dict[str, Any]]:
    """Yield the entries whose endpoint is on *host*."""
    for entry in entries:
        if (urlsplit(str(entry.get("endpoint") or "")).hostname or "").lower() == host.lower():
            yield entry


_ABSOLUTE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
# Download field of the registry for each file suffix of an export.
_FIELDS = {"nt": "download_nt", "ttl": "download_ttl", "rdf": "download_rdfxml"}


def _kept_files(graph: dict[str, Any]) -> list[dict[str, Any]]:
    return [p for p in graph.get("pieces", []) if p.get("file") and p.get("error") is None]


def _field(piece: dict[str, Any]) -> str:
    suffix = str(piece["file"]).removesuffix(".gz").rsplit(".", 1)[-1]
    return _FIELDS[suffix]


def exported_entry(export_dir: Path) -> dict[str, Any]:
    """Return the registry fields that make a complete export the inputs of a local source.

    The named graphs become graph_uris and graph_sources (one file URL per piece; a graph
    without triples is left out); an export of the default graph alone becomes download_*
    fields. endpoint_export pins the export: the endpoint, when it ended, and the manifest
    with its SHA-256, which holds the SHA-256 of each file. A default-graph remainder beside
    named graphs has no place in graph_sources and is refused.
    """
    export_dir = Path(export_dir).resolve()
    path = export_dir / MANIFEST
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not source_complete(manifest):
        raise ValueError(f"Export not complete: {path}")
    graphs = manifest["graphs"]
    named = [g for g in graphs if g["graph"] is not None and _kept_files(g)]
    # A graph named by a relative IRI (such as one that holds the endpoint's VoID description)
    # cannot be a registry graph; its files stay in the export and are listed.
    left_out = [g["graph"] for g in named if not _ABSOLUTE.match(g["graph"])]
    named = [g for g in named if _ABSOLUTE.match(g["graph"])]
    default = [g for g in graphs if g["graph"] is None and _kept_files(g)]
    remainder: str | None = None
    null = [g for g in named if g["graph"] == GraphExporter.NULL_CONTEXT]
    if null:
        remainder = str(manifest["endpoint"]).rstrip("/") + "#default"
        named = [g for g in named if g["graph"] != GraphExporter.NULL_CONTEXT]
        named.append({**null[0], "graph": remainder})
    if named and default:
        # The default-graph triples that no named graph holds get a graph of their own, named
        # after the endpoint.
        remainder = str(manifest["endpoint"]).rstrip("/") + "#default"
        default[0] = {**default[0], "graph": remainder}
        named = [*named, default[0]]
        default = []
    if not named and not default:
        raise ValueError("No graph with triples that a registry entry can name")
    fields: dict[str, Any] = {}
    if named:
        fields["graph_uris"] = [g["graph"] for g in named]
        sources: dict[str, dict[str, list[str]]] = {}
        for graph in named:
            mapping = sources.setdefault(graph["graph"], {})
            for piece in _kept_files(graph):
                url = (export_dir / piece["file"]).as_uri()
                mapping.setdefault(_field(piece), []).append(url)
        fields["graph_sources"] = sources
    else:
        for piece in _kept_files(default[0]) if default else []:
            fields.setdefault(_field(piece), []).append((export_dir / piece["file"]).as_uri())
    fields["endpoint_export"] = {
        "endpoint": manifest["endpoint"],
        "finished": manifest["finished"],
        "manifest": str(path),
        "manifest_sha256": _sha256(path),
        "graphs": len(graphs),
        "triples": sum(int(g.get("triples") or 0) for g in graphs),
        "verified": all(g["outcome"] == "retrieved/verified" for g in graphs),
    }
    if left_out:
        fields["endpoint_export"]["left_out_relative_graphs"] = left_out
    statements = statement_summary(graphs)
    if statements["left_out"]:
        # The statements the endpoint counts that are not in the files (not RDF), by kind.
        fields["endpoint_export"]["left_out_statements"] = statements["left_out"]
    if statements["repaired"]:
        fields["endpoint_export"]["repaired_statements"] = statements["repaired"]
    if remainder:
        fields["endpoint_export"]["default_graph_remainder_as"] = remainder
    return fields


def prepare_workdir(export_dir: Path, workdir: Path) -> dict[str, Any]:
    """Place the files of a complete export in a QLever work folder as downloaded inputs.

    Each file is hard-linked (or, across file systems, symlinked) where the local pipeline
    keeps the download of its URL, and downloads.json records the URLs with each file's size
    and SHA-256, so that the pipeline indexes them and downloads nothing. Return the fields
    of exported_entry.
    """
    from rdfsolve.qlever.downloads import write_record
    from rdfsolve.qlever.inputs import graph_input_directory
    from rdfsolve.qlever.utils import graph_download_name

    fields = exported_entry(export_dir)
    workdir = Path(workdir)
    if any(workdir.glob("*.index.pso")):
        # Built already: its inputs.json pins what it was built from; the folder is kept.
        return fields
    placed: dict[str, Path] = {}
    if "graph_sources" in fields:
        for graph, mapping in fields["graph_sources"].items():
            folder = graph_input_directory(workdir, graph) / "rdf"
            for key, urls in mapping.items():
                for url in urls:
                    placed[url] = folder / graph_download_name(url, key)
    else:
        for key, urls in fields.items():
            if key.startswith("download_"):
                for url in urls:
                    placed[url] = workdir / "rdf" / url.rsplit("/", 1)[-1]
    sizes: dict[str, dict[str, str | None]] = {}
    manifest = json.loads(Path(fields["endpoint_export"]["manifest"]).read_text("utf-8"))
    digests = {p["file"]: p["sha256"] for g in manifest["graphs"] for p in _kept_files(g)}
    for url, target in placed.items():
        source = Path(url.removeprefix("file://"))
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            try:
                os.link(source, target)
            except OSError:
                target.symlink_to(source)
        sizes[url] = {
            "last_modified": None,
            "content_length": str(source.stat().st_size),
            "sha256": digests.get(source.name),
        }
    # The record lists the URLs that the local pipeline downloads for the entry, those of its
    # graphs for an entry with graph_sources (downloads.entry_download_urls). The files of the
    # export are pinned in export_inputs.json beside it.
    write_record(workdir, list(placed), head=lambda url: sizes[url])
    pins = {
        "endpoint_export": fields["endpoint_export"],
        "files": {url: {"path": str(target), **sizes[url]} for url, target in placed.items()},
    }
    (workdir / "export_inputs.json").write_text(json.dumps(pins, indent=1), encoding="utf-8")
    return fields
