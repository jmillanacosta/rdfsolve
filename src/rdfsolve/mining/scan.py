"""Scan mining: QLever only reads its index; every count is made by Polars on the rows it reads.

The grouped SPARQL queries of the other strategies join and group inside the server, within
its memory limit, and QLever does not spill to disk. Here the server is asked only for the
rows of one predicate at a time, ``SELECT ?s ?o WHERE { ?s <p> ?o }``, a scan of one range of a sorted
permutation that QLever streams without holding it, and for the members of the classes. The
rows are saved as Parquet (the row store); patterns, their counts and the rest are computed
from the store, one predicate at a time, so memory is bound by the largest predicate.

Each value is saved twice: as the term QLever writes in TSV, and as QLever's 64-bit id of the
value (the same query with ``Accept: application/octet-stream``). The ids count distinct
values exactly: the TSV writes doubles with 13 significant digits, so values that differ further
print the same.

The patterns follow the definitions of the count queries of the two-phase strategy
(query_builders: typed objects, untyped IRIs, blank nodes, literals), so that the two
strategies give the same schema of the same index.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, Self

from rdfsolve.mining.query_builders import MEMBERSHIP
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS, _URI_SCHEMES, UNTYPED_SUBJECT
from rdfsolve.schema_models.pattern import PatternType, SchemaPattern

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

__all__ = [
    "QLEVER_DEFAULT_GRAPH",
    "RowStore",
    "ScanStrategy",
    "StoreView",
    "count_patterns",
    "export_index",
    "name_class_expressions",
    "store_from_graph",
]

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
# The subject class of the rows of untyped IRI subjects while they are counted (not a term).
UNTYPED = "\x00untyped"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
RDF_LANG = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
STORE_VERSION = 3
# The graph of the triples that QLever keeps outside every named graph (input without a graph).
QLEVER_DEFAULT_GRAPH = "http://qlever.cs.uni-freiburg.de/builtin-functions/default-graph"


class CountedStore(Protocol):
    """What count_patterns reads of a store: a RowStore, a StoreView, or a store whose type
    table is replaced (scan_terms.RetypedStore).
    """

    @property
    def manifest(self) -> dict[str, Any]:
        """Return the manifest of the store (store.json)."""

    @property
    def predicates(self) -> dict[str, Path]:
        """Return the file of each predicate in scope."""

    @property
    def base(self) -> RowStore:
        """Return the whole store."""

    def graph_rows(self, predicate: str) -> pl.LazyFrame:
        """Return the rows of *predicate* with the graph of each row."""

    def rows(self, predicate: str) -> pl.LazyFrame:
        """Return the rows of *predicate*, each triple once."""

    def type_ids(self) -> pl.LazyFrame:
        """Return the type table by id (sid, c)."""

    def graph_types(self) -> pl.LazyFrame:
        """Return the type rows as read (s, sid, c), not made distinct."""

    def typed_ids(self) -> pl.LazyFrame:
        """Return the ids of the nodes with a type in scope (sid), whatever the type table."""

    def data_types(self) -> pl.LazyFrame:
        """Return the types in the data graphs."""


class RowStore:
    """The rows of one index, saved by predicate.

    ``types.parquet`` has the members of each class (s, c: the rows of the membership
    properties); ``rows/NNNNN.parquet`` the rows of one predicate (s, o, d, oid);
    ``store.json`` names the file of each predicate, the membership properties, and the index
    the rows were read from. Terms are written as in SPARQL TSV: <iri>, _:label, or a literal.

    An index with named graphs gives each row its graph: columns g (the graph IRI as a term)
    and gid (QLever's id of it), in the rows and in ``types.parquet``. A triple held by k
    graphs is k rows. The triples that QLever keeps outside every named graph (input without
    a graph) have the graph QLEVER_DEFAULT_GRAPH, which ``GRAPH ?g`` does not return.
    ``store.json`` then lists the rows of each predicate in each graph (graphs), the rows of
    each predicate (rows) and its triples in the default graph (triples_by_predicate).

    The store itself is the default graph of the index, as QLever answers a query without FROM
    or GRAPH: the union of all graphs, each triple once. view() selects a graph scope.
    """

    graph_uris: list[str] | None = None
    context: tuple[str, ...] = ()

    def __init__(self, path: Path) -> None:
        """Open the store in *path*."""
        self.path = Path(path)
        self.manifest: dict[str, Any] = json.loads((self.path / "store.json").read_text())

    @property
    def base(self) -> RowStore:
        """The store of the whole index (a view returns the store it selects from)."""
        return self

    @property
    def graphs(self) -> dict[str, dict[str, int]] | None:
        """The rows of each predicate in each graph, or None for an index without named graphs."""
        graphs: dict[str, dict[str, int]] | None = self.manifest.get("graphs")
        return graphs

    @property
    def predicates(self) -> dict[str, Path]:
        """The file of the rows of each predicate that can be a pattern's property.

        A predicate that cannot (_pattern_term, such as a string of NUL bytes) is left out of
        every scan phase; left_out_predicates names it.
        """
        return {
            p: self.path / "rows" / f
            for p, f in self.manifest["predicates"].items()
            if _pattern_term(p)
        }

    @property
    def left_out_predicates(self) -> dict[str, int]:
        """The predicates that cannot be a pattern's property, with their rows."""
        rows = self.manifest.get("rows") or {}
        return {
            p: int(rows.get(p) or 0) for p in self.manifest["predicates"] if not _pattern_term(p)
        }

    def _duplicated(self, predicate: str) -> bool:
        """Whether some triple of *predicate* is in more than one graph (more rows than triples)."""
        rows = (self.manifest.get("rows") or {}).get(predicate)
        triples = (self.manifest.get("triples_by_predicate") or {}).get(predicate)
        return self.graphs is not None and (rows is None or triples is None or rows > triples)

    def graph_rows(self, predicate: str) -> pl.LazyFrame:
        """Return every row of *predicate* (with its graph g and gid when the index has graphs)."""
        import polars as pl

        o = pl.col("o")
        kind = (
            pl.when(o.str.starts_with("<"))
            .then(pl.lit("iri"))
            .when(o.str.starts_with("_:"))
            .then(pl.lit("bnode"))
            .otherwise(pl.lit("literal"))
        )
        return (
            pl.scan_parquet(self.predicates[predicate])
            .with_columns(kind=kind)
            .with_columns(
                d=pl.when(pl.col("kind") == "literal").then(
                    _bare(pl.col("d")).fill_null(XSD_STRING)
                )
            )
        )

    def rows(self, predicate: str) -> pl.LazyFrame:
        """Return the triples of *predicate* in the default graph: s, o, d (datatype IRI of a
        literal), oid, kind; a triple held by several graphs once.
        """
        rows = self.graph_rows(predicate)
        if self.graphs is None:
            return rows
        if self._duplicated(predicate):
            rows = rows.unique(subset=["s", "oid"])
        return rows.drop("g", "gid")

    def _type_rows(self) -> pl.LazyFrame:
        """Return every membership row as read, whatever its type value."""
        import polars as pl

        named = self.path / "types-named.parquet"
        return pl.scan_parquet(named if named.is_file() else self.path / "types.parquet")

    def graph_types(self) -> pl.LazyFrame:
        """Return the membership rows (s, c, and g when the index has graphs).

        name_class_expressions writes types-named.parquet, where a type value that is a blank
        node (an OWL class expression) is replaced by the IRI of its expression. A type value
        that is neither an IRI nor a blank node (a literal) names no class and is left out, so
        that a node typed only so is an untyped subject (literal_type_values reports them).
        """
        import polars as pl

        types = self._type_rows().filter(_class_term(pl.col("c")))
        # An index without any type: the empty table is held in memory,
        # since Polars 2.0 panics on a join with unique() of an empty Parquet scan ("min > max").
        if not types.select(pl.len()).collect().item():
            return pl.DataFrame(schema=types.collect_schema()).lazy()
        return types

    @property
    def distinct_types(self) -> bool:
        """Whether the membership rows (graph_types) are already one per node and class.

        An index without named graphs gives each membership triple once: an export query reads
        distinct rows, and the rows of several membership properties and the named class
        expressions (types-named.parquet) are made distinct when written. Such a type table
        need not be made distinct again, which on a large index holds every row in memory.
        """
        return self.graphs is None

    def literal_type_values(self, limit: int = 20) -> dict[str, Any] | None:
        """Return the type values that are not classes (literals), with their membership rows.

        The form is that of the report's literal_type_values (count, samples), with the rows of
        each value; None when there is none.
        """
        import polars as pl

        found = (
            self._type_rows()
            .filter(~_class_term(pl.col("c")))
            .group_by("c")
            .agg(pl.len().alias("rows"))
            .sort("rows", "c", descending=[True, False])
            .collect()
        )
        if not found.height:
            return None
        return {
            "count": int(found["rows"].sum()),
            "values": found.height,
            "samples": [
                {"value": value, "rows": int(rows)} for value, rows in found.head(limit).iter_rows()
            ],
        }

    def types(self) -> pl.LazyFrame:
        """Return the members of each class (s, c), anonymous classes by their expression's IRI."""
        return self.graph_types().select("s", "c").unique()

    def type_ids(self) -> pl.LazyFrame:
        """Return the members of each class by id (sid, c): 16 bytes a row, for large indexes."""
        return self.graph_types().select("sid", "c").unique()

    def typed_ids(self) -> pl.LazyFrame:
        """Return the ids of the nodes with a membership row (sid): the typed nodes.

        A store whose type table is rewritten (scan_terms.RetypedStore: class expressions left
        out, terms grouped) reads this from the store it wraps, so that a node typed only by a
        left-out class is still typed.
        """
        return self.graph_types().select("sid").unique()

    def data_types(self) -> pl.LazyFrame:
        """Return the members read from the data graphs alone (all graphs here)."""
        return self.types()

    def data_type_ids(self) -> pl.LazyFrame:
        """Return data_types by id (sid, c)."""
        return self.type_ids()

    def members(self) -> pl.LazyFrame:
        """Return the members of each class as the miner counts them (_subject_type_pattern)."""
        return self.types()

    def view(
        self, graph_uris: Sequence[str] | None, type_context_graph_uris: Sequence[str] | None = None
    ) -> RowStore | StoreView:
        """Select the graph scope of a mining run (rdfsolve.mining.query_builders._graph_scope).

        Without data graphs the scope is the default graph (this store; type context graphs add
        nothing to it, since it is the union of all graphs).
        """
        if not graph_uris:
            return self
        if self.graphs is None:
            if (self.manifest.get("graph_split") or {}).get("state") == "missing":
                # The rows were read without their graphs (export_index): the scope is not
                # applied, and every row is mined.
                logger.warning(
                    "Scan: the graph split of the store is missing; the graph scope is not "
                    "applied and the whole index is mined"
                )
                return self
            raise ValueError("The index has no named graphs: a graph scope selects no triple of it")
        return StoreView(self, graph_uris, type_context_graph_uris or ())

    @property
    def class_expressions(self) -> dict[str, dict[str, Any]]:
        """The anonymous classes: IRI -> Manchester syntax (CURIEs, and full IRIs), members."""
        path = self.path / "class-expressions.json"
        return json.loads(path.read_text()) if path.is_file() else {}


class StoreView:
    """The rows of a row store in a graph scope, as the miner's queries read them.

    The scope of a mining run (query_builders._graph_scope, _type_pattern, _context_pattern):

    - the data graphs (graph_uris) hold the edges: ``GRAPH ?_g { ?s ?p ?o }`` with ?_g one of
      them; graph_rows() keeps the graph of each row, rows() is their RDF merge (``FROM`` each
      data graph: a triple held by two of them once);
    - types are read from the data graphs and the type context graphs (types(): DISTINCT
      (s, c) over them);
    - a class's members (_subject_type_pattern) are its typed nodes; with type context graphs,
      only those with an edge in the data graphs (members());
    - data_types(): the types in the data graphs alone, as a query without type context reads
      them (the predicates of blank nodes; property usage evidence).
    """

    # A node typed in several graphs of the scope has a membership row in each.
    distinct_types = False

    def __init__(
        self, store: RowStore, graph_uris: Sequence[str], context: Sequence[str] = ()
    ) -> None:
        """Select *graph_uris* of *store*, with types also from *context*."""
        self.base = store
        self.path = store.path
        self.manifest = store.manifest
        self.graphs = store.graphs
        self.graph_uris = list(dict.fromkeys(graph_uris))
        self.context = tuple(dict.fromkeys(context))
        self.type_graph_uris = list(dict.fromkeys([*self.graph_uris, *self.context]))

    @staticmethod
    def _terms(graphs: Iterable[str]) -> list[str]:
        return [f"<{g}>" for g in graphs]

    @property
    def predicates(self) -> dict[str, Path]:
        """The files of the predicates that have rows in the data graphs."""
        held = {p for g in self.graph_uris for p, n in (self.graphs or {}).get(g, {}).items() if n}
        return {p: f for p, f in self.base.predicates.items() if p in held}

    @property
    def class_expressions(self) -> dict[str, dict[str, Any]]:
        """The anonymous classes of the store."""
        return self.base.class_expressions

    def graph_rows(self, predicate: str) -> pl.LazyFrame:
        """Return the rows of *predicate* in the data graphs, each with its graph."""
        import polars as pl

        return self.base.graph_rows(predicate).filter(
            pl.col("g").is_in(self._terms(self.graph_uris))
        )

    def rows(self, predicate: str) -> pl.LazyFrame:
        """Return the triples of *predicate* in the RDF merge of the data graphs."""
        rows = self.graph_rows(predicate)
        if len(self.graph_uris) > 1 and self.base._duplicated(predicate):
            rows = rows.unique(subset=["s", "oid"])
        return rows.drop("g", "gid")

    def graph_types(self) -> pl.LazyFrame:
        """Return the membership rows of the data and type context graphs."""
        import polars as pl

        return self.base.graph_types().filter(pl.col("g").is_in(self._terms(self.type_graph_uris)))

    def types(self) -> pl.LazyFrame:
        """Return (s, c) read from the data and type context graphs."""
        return self.graph_types().select("s", "c").unique()

    def type_ids(self) -> pl.LazyFrame:
        """Return (sid, c) read from the data and type context graphs."""
        return self.graph_types().select("sid", "c").unique()

    def typed_ids(self) -> pl.LazyFrame:
        """Return the ids of the nodes typed in the data and type context graphs (sid)."""
        return self.graph_types().select("sid").unique()

    def data_types(self) -> pl.LazyFrame:
        """Return (s, c) read from the data graphs alone."""
        import polars as pl

        return (
            self.base.graph_types()
            .filter(pl.col("g").is_in(self._terms(self.graph_uris)))
            .select("s", "c")
            .unique()
        )

    def data_type_ids(self) -> pl.LazyFrame:
        """Return data_types by id (sid, c)."""
        import polars as pl

        return (
            self.base.graph_types()
            .filter(pl.col("g").is_in(self._terms(self.graph_uris)))
            .select("sid", "c")
            .unique()
        )

    def members(self) -> pl.LazyFrame:
        """Return the members of each class: with type context graphs, those with an edge in
        the data graphs (_subject_type_pattern's FILTER EXISTS).
        """
        import polars as pl

        if not self.context:
            return self.types()
        subjects = pl.concat(
            [self.graph_rows(p).select("s") for p in self.predicates], how="vertical_relaxed"
        ).unique()
        return self.types().join(subjects, on="s", how="semi")

    def for_graph(self, graph: str) -> StoreView:
        """Return the view of one data graph with the type graphs of this scope."""
        return StoreView(self.base, [graph], [g for g in self.type_graph_uris if g != graph])

    def view(
        self, graph_uris: Sequence[str] | None, type_context_graph_uris: Sequence[str] | None = None
    ) -> RowStore | StoreView:
        """Select another scope of the same store."""
        return self.base.view(graph_uris, type_context_graph_uris)


def _class_term(expr: pl.Expr) -> pl.Expr:
    """Return whether a type value, as SPARQL TSV writes it, can be a class.

    An IRI or a blank node (an OWL class expression) can; a literal cannot.
    """
    return expr.str.starts_with("<") | expr.str.starts_with("_:")


def _durable(partial: Path, path: Path) -> None:
    """Flush *partial* to disk and rename it to *path*, so that *path* is whole or absent.

    A job killed (OOM, time limit) or a node that fails while a store file is written leaves a
    file named ``.partial`` at most, never a short or zero-filled file under the final name that
    a resumed export would reuse.
    """
    with partial.open("rb") as stream:
        os.fsync(stream.fileno())
    partial.replace(path)


def _write_text(path: Path, text: str) -> None:
    """Write a store's JSON file whole: to a partial file, flushed, then renamed."""
    partial = path.with_name(path.name + ".partial")
    partial.write_text(text)
    _durable(partial, path)


def _bare(expr: pl.Expr) -> pl.Expr:
    """Strip the angle brackets of an IRI term."""
    return expr.str.strip_prefix("<").str.strip_suffix(">")


# Building a store


class QueryTimeoutError(RuntimeError):
    """The server stopped the query at its time limit (QLever answers 429 with "timed out")."""


class HalvingStoppedError(QueryTimeoutError):
    """A half of a read timed out like the read it was cut from (_read_query)."""


# The queries this process has open on a local server: at most two per export stream
# (2 * export_workers()), below the slots the server is started with
# (qlever.lifecycle.simultaneous_queries: 2 * export_workers() + 2), so that the client never
# sends more than the server can take. BUSY_WAIT: how long a query waits on a busy server
# (429) after the last of this process's queries ended.
BUSY_WAIT = 600.0
_OPEN: dict[str, Any] = {}
_OPEN_LOCK = threading.Lock()
_PAIR_LOCK = threading.Lock()


def _query_slots() -> threading.BoundedSemaphore:
    """Return the semaphore of the queries this process has open (sized when first used)."""
    with _OPEN_LOCK:
        slots = 2 * export_workers()
        if _OPEN.get("size") != slots:
            _OPEN.update(size=slots, semaphore=threading.BoundedSemaphore(slots))
        _OPEN.setdefault("ended", time.monotonic())
        semaphore: threading.BoundedSemaphore = _OPEN["semaphore"]
        return semaphore


class _OpenQuery:
    """A response that holds one query slot until it is closed."""

    def __init__(self, response: Any, semaphore: threading.BoundedSemaphore) -> None:
        """Keep the open *response* and the semaphore whose slot it holds."""
        self._response = response
        self._semaphore: threading.BoundedSemaphore | None = semaphore

    def read(self, *args: Any) -> bytes:
        """Read from the response."""
        data: bytes = self._response.read(*args)
        return data

    def readline(self, *args: Any) -> bytes:
        """Read a line from the response."""
        data: bytes = self._response.readline(*args)
        return data

    def close(self) -> None:
        """Close the response and give its slot back (once)."""
        self._response.close()
        if self._semaphore is not None:
            self._semaphore.release()
            self._semaphore = None
            _OPEN["ended"] = time.monotonic()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


@contextlib.contextmanager
def _post_pair(endpoint: str, query: str, id_query: str) -> Iterator[tuple[Any, Any]]:
    """Open the TSV result of *query* and the ids of *id_query* together. The two slots are
    taken under one lock, so that streams never each hold one slot waiting for a second.
    """
    with contextlib.ExitStack() as stack:
        with _PAIR_LOCK:
            text = stack.enter_context(_post(endpoint, query, "text/tab-separated-values"))
            binary = stack.enter_context(_post(endpoint, id_query, "application/octet-stream"))
        yield text, binary


def _post(endpoint: str, query: str, accept: str, *, timeout: float | None = None) -> Any:
    """Send *query* to a local endpoint; return the open response (no proxy, no time limit).

    The process has at most 2 * export_workers() queries open at once (_query_slots), fewer
    than the server's slots. A server whose query slots are all taken (queries of other
    clients, or ones it is still ending) answers 429; the query is sent again after a wait
    (1, 2, 4 … 60 s) for as long as this process's other queries go on ending, and up to
    BUSY_WAIT seconds after the last one ended. QLever also answers 429 to a query it stopped
    at its time limit, with "timed out" in the body: that raises QueryTimeoutError at once
    (the caller reads it in slices; sending it again would time out again). With *timeout*,
    QLever stops the query after that many seconds (its per-query ``timeout`` parameter; a
    time below the server's default needs no access token): a planning query that a large
    index cannot answer soon is given up early.
    """
    import urllib.error

    from rdfsolve.sparql_terms import writable_query

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    # A term that is not an RDF IRI (one with a space) is written with IRI("...").
    form = {"query": writable_query(query)}
    if timeout is not None:
        form["timeout"] = f"{timeout:g}s"
    data = urllib.parse.urlencode(form).encode()
    wait = 1.0
    started = time.monotonic()
    semaphore = _query_slots()
    semaphore.acquire()
    try:
        while True:
            request = urllib.request.Request(endpoint, data, {"Accept": accept})  # noqa: S310
            try:
                return _OpenQuery(opener.open(request, timeout=None), semaphore)
            except urllib.error.HTTPError as error:
                if error.code != 429:
                    raise
                body = error.read(4096).decode(errors="replace")
                if "timed out" in body.lower():
                    raise QueryTimeoutError(
                        f"The server timed out on the query: {query}"
                    ) from error
                if time.monotonic() > max(started, _OPEN["ended"]) + BUSY_WAIT:
                    raise
                logger.info(
                    "Scan: the server is busy (429); sending the query again in %.0f s", wait
                )
                time.sleep(wait)
                wait = min(wait * 2, 60.0)
    except BaseException:
        semaphore.release()
        raise


# QLever ends a result that failed while it was being sent with this line, after HTTP 200.
QLEVER_ERROR_TRAILER = b"!!!!>># An error has occurred"


def _check_trailer(data: bytes, query: str, rest: Callable[[], bytes] | None = None) -> None:
    """Raise if *data* holds QLever's error trailer: the result was cut while being sent.

    The trailer explains itself before it gives the error, so the rest of the response is read
    (*rest*) to find it. A query that timed out raises QueryTimeoutError (read in slices).
    """
    if QLEVER_ERROR_TRAILER in data:
        if rest is not None:
            data += rest()
        at = data.index(QLEVER_ERROR_TRAILER)
        message = data[at : at + 2000].decode(errors="replace").replace("\n", " ")
        error = QueryTimeoutError if "timed out" in message.lower() else RuntimeError
        raise error(f"QLever cut the result while sending it ({message}): {query}")


def _tsv_to_parquet(endpoint: str, query: str, path: Path, *, timeout: float | None = None) -> int:
    """Stream the TSV result of *query* to *path* as Parquet; return its rows.

    QLever's TSV escapes tabs and newlines inside terms, so one line is one row. *timeout*
    limits the query (_post).
    """
    import polars as pl

    tsv = path.with_suffix(".tsv")
    with (
        _post(endpoint, query, "text/tab-separated-values", timeout=timeout) as response,
        tsv.open("wb") as out,
    ):
        tail = b""
        while chunk := response.read(1 << 24):
            _check_trailer(tail + chunk, query, lambda: response.read(4096))
            tail = chunk[-len(QLEVER_ERROR_TRAILER) :]
            out.write(chunk)
    # A term that is not UTF-8 (a predicate or graph name) is read with U+FFFD for its bytes.
    frame = pl.scan_csv(
        tsv,
        separator="\t",
        quote_char=None,
        has_header=True,
        infer_schema=False,
        encoding="utf8-lossy",
    )
    partial = path.with_name(path.name + ".partial")
    frame.rename(lambda c: c.lstrip("?")).sink_parquet(partial)
    _durable(partial, path)
    tsv.unlink()
    return int(pl.scan_parquet(path).select(pl.len()).collect().item())


# Literal text kept in full: the name and definition predicates (labels, Manchester names,
# enrichment). Other literals keep their datatype, language and id; their text is cut to
# LITERAL_CHARS characters (examples stay readable; counts use the ids).
LITERAL_CHARS = 64
BLOCK_BYTES = 1 << 26


def _text_predicates() -> frozenset[str]:
    from rdfsolve.schema_models.enrichment import DEFINITION_PREDICATES, NAME_PREDICATES

    return frozenset([*DEFINITION_PREDICATES, *NAME_PREDICATES, *LABEL_PROPERTIES])


# QLever's datatype of an id, in its top four bits, for the values it keeps in the id (written
# bare in TSV): 1 bool, 2 int, 3 double, 7 a date. DATATYPE(?o) gives xsd:boolean, xsd:int,
# xsd:double, and xsd:date, xsd:dateTime, xsd:gYearMonth or xsd:gYear by the lexical form.
_XSD = "http://www.w3.org/2001/XMLSchema#"
_ID_DATATYPES = {1: "boolean", 2: "int", 3: "double"}
_DATE_ID = 7
# A WKT point (geo:wktLiteral "POINT(x y)") is kept in the id too (code 8), written bare in TSV;
# other WKT shapes stay in the vocabulary, with their datatype written.
_GEO_POINT_ID = 8
_WKT_LITERAL = "<http://www.opengis.net/ont/geosparql#wktLiteral>"
_ZONE = r"(Z|[+-][0-9]{2}:[0-9]{2})?"


def _literal_datatype(term: str = "o", ids: str = "oid") -> pl.Expr:
    """Return the datatype IRI of each literal of *term* (``<...>``, as DATATYPE writes it), from
    its TSV form and QLever's id, without asking the server; None for an IRI or blank node.

    A quoted literal names its datatype (``"x"^^<dt>``), its language (rdf:langString) or none
    (xsd:string); a bare value is one that QLever keeps in its id, whose top bits say which
    (_ID_DATATYPES). DATATYPE(?o) in the export query made QLever read the text of every
    literal of the predicate before LIMIT and OFFSET, so a slice cost as much as the whole
    predicate.
    """
    import polars as pl

    o = pl.col(term)
    bits = pl.col(ids) // (2**60)
    typed = o.str.extract(r'"\^\^(<[^>]*>)$', 1)
    date = pl.lit(None, pl.String)
    for pattern, name in (
        (r"^-?[0-9]{4,}-[0-9]{2}-[0-9]{2}T", "dateTime"),
        (rf"^-?[0-9]{{4,}}-[0-9]{{2}}-[0-9]{{2}}{_ZONE}$", "date"),
        (rf"^-?[0-9]{{4,}}-[0-9]{{2}}{_ZONE}$", "gYearMonth"),
        (rf"^-?[0-9]{{4,}}{_ZONE}$", "gYear"),
    ):
        date = (
            pl.when(date.is_null() & o.str.contains(pattern))
            .then(pl.lit(f"<{_XSD}{name}>"))
            .otherwise(date)
        )
    value = pl.lit(None, pl.String)
    for code, name in _ID_DATATYPES.items():
        value = pl.when(bits == code).then(pl.lit(f"<{_XSD}{name}>")).otherwise(value)
    return (
        pl.when(o.str.starts_with("<") | o.str.starts_with("_:") | o.is_null())
        .then(pl.lit(None, pl.String))
        .when(o.str.starts_with('"') & typed.is_not_null())
        .then(typed)
        .when(o.str.starts_with('"') & o.str.contains(r'"@[A-Za-z0-9-]+$'))
        .then(pl.lit("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>"))
        .when(o.str.starts_with('"'))
        .then(pl.lit(f"<{_XSD}string>"))
        .when(bits == _DATE_ID)
        .then(date)
        .when(bits == _GEO_POINT_ID)
        .then(pl.lit(_WKT_LITERAL))
        .otherwise(value)
        .alias("d")
    )


def _trim_literals(column: str) -> pl.Expr:
    """Cut the lexical form of the literals of *column* to LITERAL_CHARS characters."""
    import polars as pl

    o = pl.col(column)
    parts = r'^"(.*)"((?:@[A-Za-z0-9-]+)|(?:\^\^<[^>]*>))?$'
    value = o.str.extract(parts, 1)
    suffix = o.str.extract(parts, 2).fill_null("")
    cut = value.str.slice(0, LITERAL_CHARS).str.strip_suffix("\\")
    return (
        pl.when(o.str.starts_with('"') & (value.str.len_chars() > LITERAL_CHARS))
        .then(pl.lit('"') + cut + pl.lit('…"') + suffix)
        .otherwise(o)
        .alias(column)
    )


# Rows whose bytes are not valid UTF-8, by the file they are written to (the name of the whole
# file, not of a slice): the export moves them into progress.jsonl and the manifest
# (invalid_utf8). Polars refuses a block with one such row, which would fail the source.
_INVALID_UTF8: dict[str, dict[str, Any]] = {}
# The rows of the type table that were not UTF-8, kept beside it for a resumed export.
TYPES_INVALID_UTF8 = "types.invalid_utf8.json"
_INVALID_LOCK = threading.Lock()
INVALID_UTF8_SAMPLES = 5


def _store_file(path: Path) -> str:
    """Return the name of the store file that *path* (a slice or a partial of it) is part of."""
    return path.name.split(".slice-")[0].split(".partial")[0]


def _valid_utf8(lines: bytes, path: Path) -> bytes:
    """Return *lines* (TSV rows) with each byte sequence that is not UTF-8 replaced (U+FFFD).

    The rows changed are counted for the store file of *path*, with a few of them as escaped
    bytes; valid input is returned as it is (one decode of the block).
    """
    try:
        lines.decode("utf-8")
        return lines
    except UnicodeDecodeError:
        pass
    written = []
    changed = 0
    samples: list[str] = []
    for line in lines.split(b"\n"):
        try:
            line.decode("utf-8")
            written.append(line)
        except UnicodeDecodeError:
            changed += 1
            if len(samples) < INVALID_UTF8_SAMPLES:
                samples.append(repr(line[:300])[2:-1])
            written.append(line.decode("utf-8", errors="replace").encode("utf-8"))
    with _INVALID_LOCK:
        found = _INVALID_UTF8.setdefault(_store_file(path), {"rows": 0, "samples": []})
        found["rows"] += changed
        found["samples"] = (found["samples"] + samples)[:INVALID_UTF8_SAMPLES]
    return b"\n".join(written)


def _take_invalid_utf8(*paths: Path) -> dict[str, Any] | None:
    """Return and forget the rows that were not UTF-8 in the store files *paths*."""
    taken: dict[str, Any] = {"rows": 0, "samples": []}
    with _INVALID_LOCK:
        for path in paths:
            found = _INVALID_UTF8.pop(_store_file(path), None)
            if found:
                taken["rows"] += found["rows"]
                taken["samples"] = (taken["samples"] + found["samples"])[:INVALID_UTF8_SAMPLES]
    return taken if taken["rows"] else None


def _stream_rows(
    endpoint: str,
    query: str,
    path: Path,
    ids: dict[str, int],
    *,
    keep_text: bool = True,
    extra: dict[str, Any] | None = None,
    id_query: str | None = None,
) -> int:
    """Write the rows of *query* to *path* as Parquet, with QLever's ids of some columns.

    The TSV result (the terms) and the octet-stream result (one 64-bit id per cell, row by row,
    in the same order) of the same query are read together, one block at a time, so memory is
    bound by the block, not by the result, and nothing is written to disk but the Parquet file.
    *ids* names the id columns and the position of their variable; *extra* adds constant
    columns. Without *keep_text*, literal text in column o is cut (_trim_literals).

    *id_query* is the query whose ids are read, when it is not *query*: the same scan without
    the columns that need no id (``DATATYPE(?o)``, which QLever computes for every row, takes
    most of the time of the octet-stream result). It must give the rows of *query* in the same
    order, with the id columns at the same positions. A read that fails leaves no file.
    """
    import io

    import numpy as np
    import polars as pl
    import pyarrow.parquet as pq

    partial = path.with_name(path.name + ".partial")
    rows = 0
    writer = None
    try:
        with _post_pair(endpoint, query, id_query or query) as (text, binary):
            header = text.readline().decode().rstrip("\n").split("\t")
            names = [h.lstrip("?") for h in header]
            width = _width(id_query) if id_query else len(names)
            rest = b""
            while True:
                block = text.read(BLOCK_BYTES)
                data = rest + block
                _check_trailer(data, query, lambda: text.read(4096))
                if not block:
                    lines, rest = data, b""
                    if lines and not lines.endswith(b"\n"):
                        lines += b"\n"
                else:
                    cut = data.rfind(b"\n") + 1
                    lines, rest = data[:cut], data[cut:]
                count = lines.count(b"\n")
                if count:
                    # Bytes that are not UTF-8 are replaced, and the rows counted (_valid_utf8).
                    lines = _valid_utf8(lines, path)
                    raw = binary.read(8 * width * count)
                    if len(raw) != 8 * width * count:
                        raise RuntimeError(
                            f"{len(raw) // 8} ids for {count} rows of {width}: {query}"
                        )
                    cells = np.frombuffer(raw, dtype="<u8")
                    frame = pl.read_csv(
                        io.BytesIO(lines),
                        has_header=False,
                        new_columns=names,
                        separator="\t",
                        quote_char=None,
                        infer_schema=False,
                    ).with_columns(
                        *(
                            pl.Series(name, cells[at::width], dtype=pl.UInt64)
                            for name, at in ids.items()
                        ),
                        *(pl.lit(v).alias(k) for k, v in (extra or {}).items()),
                    )
                    if "o" in frame.columns and "oid" in frame.columns and "d" not in names:
                        # The datatype of each literal, from its form and id (_literal_datatype).
                        frame = frame.with_columns(_literal_datatype())
                    if not keep_text and "o" in frame.columns:
                        frame = frame.with_columns(_trim_literals("o"))
                    table = frame.to_arrow()
                    if writer is None:
                        writer = pq.ParquetWriter(partial, table.schema, compression="zstd")
                    writer.write_table(table)
                    rows += count
                if not block:
                    break
            if binary.read(1):
                raise RuntimeError(f"More ids than rows: {query}")
    except BaseException:
        if writer is not None:
            writer.close()
        partial.unlink(missing_ok=True)
        raise
    if writer is None:
        derived = ["d"] if "o" in names and "oid" in ids and "d" not in names else []
        pl.DataFrame(
            {n: [] for n in [*names, *ids, *(extra or {}), *derived]},
            schema={
                **dict.fromkeys([*names, *derived], pl.String),
                **dict.fromkeys(ids, pl.UInt64),
            },
        ).write_parquet(partial)
    else:
        writer.close()
    _durable(partial, path)
    return rows


def _width(query: str) -> int:
    """Return the columns of ``SELECT ?a ?b ... [FROM <g>] WHERE``, a query of variables only."""
    head = query.split(" WHERE ", 1)[0].split(" FROM ", 1)[0]
    if "(" in head:
        raise ValueError(f"The query of the ids selects variables only: {query}")
    return head.count("?")


def _ids(
    endpoint: str, query: str, width: int, rows: int, columns: dict[str, int]
) -> list[pl.Series]:
    """Return QLever's 64-bit ids of some columns of *query* (application/octet-stream): one id
    per cell, row by row, in the order of the TSV result of the same query.
    """
    import numpy as np
    import polars as pl

    with _post(endpoint, query, "application/octet-stream") as response:
        ids = np.frombuffer(response.read(), dtype="<u8")
    if len(ids) != width * rows:
        raise RuntimeError(f"{len(ids)} ids for {rows} rows of {width} columns: {query}")
    return [pl.Series(name, ids[c::width], dtype=pl.UInt64) for name, c in columns.items()]


# The rows of a predicate; without *datatype*, the same scan without ?d (the query of the ids).
def _row_query(predicate: str, *, datatype: bool = True) -> str:
    d = " (DATATYPE(?o) AS ?d)" if datatype else ""
    return f"SELECT ?s ?o{d} WHERE {{ ?s <{predicate}> ?o }}"


def _graph_row_query(predicate: str, *, datatype: bool = True) -> str:
    d = " (DATATYPE(?o) AS ?d)" if datatype else ""
    return f"SELECT ?g ?s ?o{d} WHERE {{ GRAPH ?g {{ ?s <{predicate}> ?o }} }}"


def _unnamed_row_query(predicate: str, *, datatype: bool = True) -> str:
    """Read the triples of *predicate* outside every named graph (QLever's own default graph)."""
    d = " (DATATYPE(?o) AS ?d)" if datatype else ""
    return (
        f"SELECT ?s ?o{d} FROM <{QLEVER_DEFAULT_GRAPH}> "  # noqa: S608 (SPARQL)
        f"WHERE {{ ?s <{predicate}> ?o }}"
    )


# A query with more rows than SLICE_ROWS is read in slices (LIMIT and OFFSET), several at once:
# QLever streams one query from one thread, so several slices read faster than one query.
# Skipping rows is not free in every index, so a query has at most MAX_SLICES slices, of
# SLICE_ROWS rows or more.
SLICE_ROWS = 10_000_000
MAX_SLICES = 64


def export_workers() -> int:
    """Return the queries an export reads at once: RDFSOLVE_SCAN_WORKERS, else the CPUs of
    this process (at least 4). Each stream keeps about one core of the server busy.
    """
    import os

    value = os.environ.get("RDFSOLVE_SCAN_WORKERS")
    if value:
        return max(1, int(value))
    return max(4, len(os.sched_getaffinity(0)))


def _slices(rows: int) -> list[tuple[int, int | None]]:
    """Cut *rows* rows into (offset, limit) slices of SLICE_ROWS rows or more, at most
    MAX_SLICES; the last one is open (no LIMIT), so that rows beyond the count are read too and
    found by the row check.
    """
    size = max(SLICE_ROWS, -(-rows // MAX_SLICES))
    starts = range(0, max(rows, 1), size)
    return [(start, size if start + size < rows else None) for start in starts]


def _sliced(query: str, offset: int, limit: int | None) -> str:
    return query + (f" LIMIT {limit}" if limit is not None else "") + f" OFFSET {offset}"


def _join_parts(parts: Sequence[Path], path: Path) -> None:
    """Write the Parquet files *parts* to *path*, one after another, row group by row group."""
    import pyarrow.parquet as pq

    files = [pq.ParquetFile(part) for part in parts]
    full = [f for f in files if f.metadata.num_rows]
    if not full:
        Path(parts[0]).replace(path)
    else:
        partial = path.with_name(path.name + ".partial")
        writer = pq.ParquetWriter(partial, full[0].schema_arrow, compression="zstd")
        for file in full:
            for group in range(file.num_row_groups):
                writer.write_table(file.read_row_group(group))
        writer.close()
        _durable(partial, path)
    for file in files:
        file.close()
    for part in parts:
        Path(part).unlink(missing_ok=True)


def store_file_problem(path: Path, rows: int | None = None) -> str | None:
    """Return why a Parquet file of a row store cannot be reused, or None when it can.

    It must open, hold *rows* rows when they are known, and its term columns (s, o, c) must
    hold no empty term and no NUL byte in its first and last row groups: a file that a
    killed job or a failed node left short or zero-filled is read again, not trusted. The check reads two row groups, not the whole file.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    try:
        file = pq.ParquetFile(path)
    except Exception as error:  # any unreadable file is read again
        return f"does not open: {error}"
    try:
        found = file.metadata.num_rows
        if rows is not None and found != rows:
            return f"{found} rows, {rows} recorded"
        columns = [c for c in ("s", "o", "c") if c in file.schema_arrow.names]
        for group in sorted({0, file.num_row_groups - 1}):
            if group < 0 or not columns:
                continue
            table = file.read_row_group(group, columns=columns)
            for name in columns:
                column = table.column(name)
                bad = pc.or_(
                    pc.equal(pc.utf8_length(column), 0),
                    pc.match_substring(column, "\x00"),
                )
                if pc.any(bad).as_py():
                    return f"an empty term or one with NUL bytes in column {name}"
    except Exception as error:
        return f"does not read: {error}"
    finally:
        file.close()
    return None


def _read_query(
    pool: ThreadPoolExecutor,
    endpoint: str,
    query: str,
    path: Path,
    ids: dict[str, int],
    rows: int,
    *,
    id_query: str | None = None,
    keep_text: bool = True,
    extra: dict[str, Any] | None = None,
) -> int:
    """Write the rows of *query* (about *rows* of them) to *path* with _stream_rows, in
    slices read at once in *pool* when it has more than SLICE_ROWS rows; return the rows.

    The slices are joined in order, so the file has the rows in the order of the query (QLever's
    scan order). A slice already written (an export resumed) is not read again.
    """
    import pyarrow.parquet as pq

    def read(
        part: Path,
        offset: int | None,
        limit: int | None,
        size: int,
        *,
        parent: float | None = None,
    ) -> int:
        """Write the whole result (*offset* None), or one slice of it, to *part*; return its
        rows. A read the server times out on is read again in two halves, joined in order.

        A half that times out after as long as the read it was cut from (*parent*: the
        seconds that read ran; HALVING_RATIO of them, when they were HALVING_MIN_SECONDS or
        more) costs as much as its parent: the cost is not in the rows read, and halving goes
        no further (HalvingStoppedError).
        """
        started = time.monotonic()
        try:
            if offset is None:
                return _stream_rows(
                    endpoint, query, part, ids, keep_text=keep_text, extra=extra, id_query=id_query
                )
            return _stream_rows(
                endpoint,
                _sliced(query, offset, limit),
                part,
                ids,
                keep_text=keep_text,
                extra=extra,
                id_query=_sliced(id_query, offset, limit) if id_query else None,
            )
        except QueryTimeoutError as error:
            ran = time.monotonic() - started
            if (
                parent is not None
                and parent >= HALVING_MIN_SECONDS
                and ran >= HALVING_RATIO * parent
            ):
                raise HalvingStoppedError(
                    f"a half of {size} rows timed out like the read it was cut from: {error}"
                ) from error
            if size <= max(SPLIT_ROWS, 1):
                raise
        offset = offset or 0
        cut = size // 2
        logger.info("Scan: the server timed out on %d rows; reading them in halves", size)
        halves = [part.with_name(part.name + ".a"), part.with_name(part.name + ".b")]
        count = read(halves[0], offset, cut, cut, parent=ran)
        count += read(
            halves[1],
            offset + cut,
            None if limit is None else limit - cut,
            size - cut,
            parent=ran,
        )
        _join_parts(halves, part)
        return count

    def stream(part: Path, offset: int | None = None, limit: int | None = None) -> int:
        """Write the whole result, or one slice of it, to *part*; return its rows."""
        if offset is None:
            return read(part, None, None, rows)
        if part.is_file() and store_file_problem(part) is None:
            return int(pq.ParquetFile(part).metadata.num_rows)
        return read(part, offset, limit, limit if limit is not None else rows - offset)

    if rows <= SLICE_ROWS:
        return pool.submit(stream, path).result()
    slices = _slices(rows)
    parts = [
        path.with_name(f"{path.name}.slice-{offset}-{limit if limit else 'end'}")
        for offset, limit in slices
    ]
    futures = [
        pool.submit(stream, part, offset, limit)
        for part, (offset, limit) in zip(parts, slices, strict=True)
    ]
    count = sum(future.result() for future in futures)
    _join_parts(parts, path)
    return count


# Rows of a predicate whose literal text is read when the text of all of them cannot be: their
# datatype and language stand for the predicate's literals (_read_long_literals).
LONG_TEXT_SAMPLE = 2000


def _read_ids(endpoint: str, query: str, width: int) -> Any:
    """Return the ids of the rows of *query* (application/octet-stream), *width* a row."""
    import numpy as np

    chunks = []
    with _post(endpoint, query, "application/octet-stream") as response:
        while chunk := response.read(1 << 24):
            chunks.append(chunk)
    data = b"".join(chunks)
    if len(data) % (8 * width):
        raise RuntimeError(f"{len(data) // 8} ids for rows of {width}: {query}")
    return np.frombuffer(data, dtype="<u8").reshape(-1, width)


def _read_long_literals(
    endpoint: str,
    path: Path,
    where: str,
    *,
    graph: bool,
    keep_text: bool,
    extra: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    """Write the rows of one predicate (*where*: its triple pattern) whose literal text cannot
    be read in time; return the rows and how its literals were read.

    QLever can write the text of long literals far more slowly than their ids, while
    ``isLiteral`` is answered from the ids. So the rows whose object is not a literal
    are read as usual, and the literal rows as ids (exact triples, subjects and distinct
    objects) with the subjects' text and the text of LONG_TEXT_SAMPLE of them. The most
    frequent datatype and language of the sample stand for the other literals of QLever's
    vocabulary, which are written ``"…"`` with that suffix; a value that QLever keeps in its id
    (a number, a boolean) has the datatype of its id (_ID_DATATYPES). The profile is
    sample-based, and recorded (long_text).
    """
    import polars as pl

    work = path.with_name(path.name + ".long")
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir()
    g = "?g " if graph else ""
    other = f"SELECT {g}?s ?o WHERE {{ {where} FILTER(!isLiteral(?o)) }}"
    literal = f"SELECT {g}?s ?o WHERE {{ {where} FILTER(isLiteral(?o)) }}"
    columns = {"gid": 0, "sid": 1, "oid": 2} if graph else {"sid": 0, "oid": 1}
    try:
        rows = _stream_rows(
            endpoint,
            other,
            work / "other.parquet",
            columns,
            keep_text=keep_text,
            extra=extra,
            id_query=other,
        )
        cells = _read_ids(endpoint, literal, len(columns))
        ids = pl.DataFrame(
            [pl.Series(name, cells[:, at], dtype=pl.UInt64) for name, at in columns.items()]
        )
        subjects = f"SELECT DISTINCT ?s WHERE {{ {where} FILTER(isLiteral(?o)) }}"
        _stream_rows(endpoint, subjects, work / "s.parquet", {"sid": 0}, id_query=subjects)
        names = pl.read_parquet(work / "s.parquet").select("s", "sid")
        sampled = literal + f" LIMIT {LONG_TEXT_SAMPLE}"
        _stream_rows(
            endpoint,
            sampled,
            work / "sample.parquet",
            columns,
            keep_text=keep_text,
            id_query=sampled,
        )
        sample = pl.read_parquet(work / "sample.parquet")
        vocabulary = sample.filter(pl.col("oid") // (2**60) == 4)
        profile = vocabulary.group_by("d").agg(n=pl.len()).sort("n", "d", descending=[True, False])
        languages = (
            vocabulary.select(lang=pl.col("o").str.extract(r'"@([A-Za-z0-9-]+)$', 1))
            .drop_nulls()
            .group_by("lang")
            .agg(n=pl.len())
            .sort("n", "lang", descending=[True, False])
        )
        string = f"<{_XSD}string>"
        datatype = str(profile["d"][0]) if profile.height else string
        language = str(languages["lang"][0]) if languages.height else None
        if datatype.endswith("#langString>") and language:
            suffix = f"@{language}"
        elif datatype == string:
            suffix = ""
        else:
            suffix = f"^^{datatype}"
        bits = pl.col("oid") // (2**60)
        d = pl.lit(datatype)
        for code, name in _ID_DATATYPES.items():
            d = pl.when(bits == code).then(pl.lit(f"<{_XSD}{name}>")).otherwise(d)
        d = pl.when(bits == _GEO_POINT_ID).then(pl.lit(_WKT_LITERAL)).otherwise(d)
        known = sample.select("oid", known_o="o", known_d="d").unique("oid")
        rest = (
            ids.join(names, on="sid", how="left")
            .join(known, on="oid", how="left")
            .with_columns(
                o=pl.coalesce("known_o", pl.lit('"…"' + suffix)), d=pl.coalesce("known_d", d)
            )
            .drop("known_o", "known_d")
        )
        if graph:
            graphs = f"SELECT DISTINCT ?g WHERE {{ {where} FILTER(isLiteral(?o)) }}"
            _stream_rows(endpoint, graphs, work / "g.parquet", {"gid": 0}, id_query=graphs)
            rest = rest.join(
                pl.read_parquet(work / "g.parquet").select("g", "gid"), on="gid", how="left"
            )
        for key, value in (extra or {}).items():
            rest = rest.with_columns(pl.lit(value).alias(key))
        written = pl.read_parquet(work / "other.parquet")
        joined = path.with_name(path.name + ".partial")
        pl.concat([written, rest.select(written.columns)], how="vertical_relaxed").write_parquet(
            joined
        )
        _durable(joined, path)
        rows += ids.height
        share = float(profile["n"][0]) / vocabulary.height if profile.height else 1.0
        info = {
            "literal_rows": ids.height,
            "sampled_rows": sample.height,
            "datatype": datatype[1:-1],
            "language": language,
            # The share of the sample's literals with that datatype; the others of the
            # predicate are given it too.
            "sample_share": round(share, 4),
            "datatypes_in_sample": sorted(str(v)[1:-1] for v in profile["d"].to_list()),
        }
        return rows, info
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _count(value: str) -> int:
    """Read a count as QLever's TSV writes it (3, or "3"^^<...#int>)."""
    return int(value.strip('"').split('"')[0])


def _counts(
    endpoint: str, query: str, path: Path, *, timeout: float | None = None
) -> list[tuple[tuple[str, ...], int]]:
    """Return the rows of a grouped count query: the grouping terms, and the count as an int."""
    import polars as pl

    _tsv_to_parquet(endpoint, query, path, timeout=timeout)
    rows = [(tuple(row[:-1]), _count(row[-1])) for row in pl.read_parquet(path).iter_rows()]
    path.unlink()
    return rows


# The time a planning query of the export (the count by graph and predicate) is given before
# the export counts another way: on a very large index it can run to QLever's time limit.
PLAN_SECONDS = 120.0


def _graph_counts(
    endpoint: str,
    path: Path,
    sizes: dict[str, int],
    unnamed: dict[str, int],
    known: Sequence[str] | None = None,
) -> dict[str, dict[str, int]]:
    """Return the rows of each predicate in each named graph.

    When the graphs of the index are *known* (a graph-mapped registry entry names the graph of
    each input file), they are not listed: one graph that holds every triple has the counts of
    GROUP BY ?p (*sizes*), and several are counted graph by graph (GROUP BY ?p in one graph,
    which reads that graph alone). Otherwise one grouped query (GROUP BY ?g ?p), given
    PLAN_SECONDS, answers it; on a large index QLever sorts every triple by graph and predicate,
    and then each
    predicate is counted by graph with the predicate bound, which also lists the graphs. The
    graphs are not listed with DISTINCT ?g, which reads every triple. A count that still times
    out raises
    QueryTimeoutError: the caller reads the rows without their graphs.
    """
    graphs: dict[str, dict[str, int]] = {}
    if known:
        names = list(dict.fromkeys(known))
        if len(names) == 1 and not unnamed:
            return {names[0]: dict(sizes)}
        for name in names:
            query = (
                f"SELECT ?p (COUNT(?s) AS ?n) WHERE {{ GRAPH <{name}> {{ ?s ?p ?o }} }} GROUP BY ?p"
            )
            counts = {p[1:-1]: n for (p,), n in _counts(endpoint, query, path)}
            if counts:
                graphs[name] = counts
        return graphs
    try:
        for (g, p), n in _counts(
            endpoint,
            "SELECT ?g ?p (COUNT(?s) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ?p",
            path,
            timeout=PLAN_SECONDS,
        ):
            graphs.setdefault(g[1:-1], {})[p[1:-1]] = n
        return graphs
    except QueryTimeoutError:
        logger.info("Scan: the count by graph and predicate timed out; counting by predicate")
    path.unlink(missing_ok=True)
    path.with_suffix(".tsv").unlink(missing_ok=True)
    for predicate, size in sizes.items():
        if size - unnamed.get(predicate, 0) <= 0:
            continue  # no row in a named graph
        for g, n in _predicate_graph_counts(endpoint, path, predicate, 0, None, size).items():
            graphs.setdefault(g[1:-1], {})[predicate] = n
    return graphs


# A read that the server stops at its time limit is read again in two halves (LIMIT, OFFSET),
# down to slices of SPLIT_ROWS rows.
SPLIT_ROWS = 100_000
# A half of a read that timed out after HALVING_RATIO of the time the read ran, when it ran
# HALVING_MIN_SECONDS or more, is not halved again (_read_query): its cost does not shrink.
HALVING_RATIO = 0.9
HALVING_MIN_SECONDS = 60.0


def _predicate_graph_counts(
    endpoint: str, path: Path, predicate: str, offset: int, limit: int | None, size: int
) -> dict[str, int]:
    """Count the rows of *predicate* in each named graph, rows *offset* to *offset* + *limit*
    of its scan (all from *offset* when *limit* is None; about *size* rows); a count the server
    times out on is made in two halves.
    """
    scan_rows = f"SELECT ?g ?s WHERE {{ GRAPH ?g {{ ?s <{predicate}> ?o }} }}"
    if offset == 0 and limit is None:
        query = f"SELECT ?g (COUNT(?s) AS ?n) WHERE {{ GRAPH ?g {{ ?s <{predicate}> ?o }} }} "
    else:
        query = (
            f"SELECT ?g (COUNT(?s) AS ?n) WHERE {{ {{ {_sliced(scan_rows, offset, limit)} }} }} "
        )
    try:
        return {g: n for (g,), n in _counts(endpoint, query + "GROUP BY ?g", path)}
    except QueryTimeoutError:
        if size <= max(SPLIT_ROWS, 1):
            raise
    path.unlink(missing_ok=True)
    path.with_suffix(".tsv").unlink(missing_ok=True)
    half = size // 2
    logger.info(
        "Scan: counting <%s> by graph timed out; counting %d rows in halves", predicate, size
    )
    counts = _predicate_graph_counts(endpoint, path, predicate, offset, half, half)
    rest = None if limit is None else limit - half
    for g, n in _predicate_graph_counts(
        endpoint, path, predicate, offset + half, rest, size - half
    ).items():
        counts[g] = counts.get(g, 0) + n
    return counts


def _has_graph_column(path: Path) -> bool:
    """Whether a row file was read with the graph of each row (column g)."""
    import pyarrow.parquet as pq

    try:
        return "g" in pq.ParquetFile(path).schema_arrow.names
    except Exception:  # an unreadable file is read again anyway
        return False


def _has_named_graphs(endpoint: str, path: Path) -> bool:
    """Whether the index holds a named graph (QLever's GRAPH ?g returns only named graphs)."""
    import polars as pl

    probe = path / "graphs.parquet"
    _tsv_to_parquet(endpoint, "SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 1", probe)
    found = pl.read_parquet(probe).height > 0
    probe.unlink()
    return found


def export_index(
    endpoint: str,
    path: Path,
    *,
    index: dict[str, Any] | None = None,
    workers: int | None = None,
    named_graphs: Sequence[str] | None = None,
) -> RowStore:
    """Read every predicate of a local QLever endpoint into a row store at *path*.

    *index* describes the index (its metadata: build and triple count); a store of the same
    index, read with the same membership properties, is reused. An export that stopped is
    resumed: plan.json records what is being read, progress.jsonl each predicate written, and
    only the predicates not written yet are read again (and the slices of a large predicate
    already written are kept). *workers* queries are read at once (export_workers() by
    default): a predicate with more than SLICE_ROWS rows is read in slices, joined in order.
    The ids are read with the same scan without ``DATATYPE(?o)`` (_stream_rows).

    Each row has the terms (s, o, the datatype d) and QLever's ids of the subject and object
    (sid, oid); literal text is kept in full only for name and definition predicates.

    When the index has named graphs, each row is read with its graph (``GRAPH ?g``), and the
    triples outside every named graph, which ``GRAPH ?g`` does not return, are read with
    ``FROM <QLEVER_DEFAULT_GRAPH>`` and given that graph. *named_graphs* are the named graphs of the
    index when they are known (the graphs of a graph-mapped entry), which are then not listed
    (_graph_counts). When the rows of each graph cannot be counted in time, the rows are read
    without their graphs, and the manifest says that the graph split is missing (graph_split):
    the source is mined over all its graphs, not failed.
    """
    import threading

    import polars as pl

    path = Path(path)
    manifest = path / "store.json"
    membership = list(MEMBERSHIP.get())
    plan = {"version": STORE_VERSION, "index": index, "membership": membership}
    if manifest.is_file() and index is not None:
        old = json.loads(manifest.read_text())
        same = (old.get("index"), old.get("version"), old.get("membership"))
        if same == (index, STORE_VERSION, membership):
            logger.info("Scan: reusing the row store %s", path)
            return RowStore(path)
    plan_file, progress = path / "plan.json", path / "progress.jsonl"
    resumable = (
        index is not None
        and plan_file.is_file()
        and json.loads(plan_file.read_text()) == plan
        and not manifest.is_file()
    )
    if not resumable and path.exists():
        shutil.rmtree(path)
    (path / "rows").mkdir(parents=True, exist_ok=True)
    _write_text(plan_file, json.dumps(plan) + "\n")
    done: dict[str, tuple[str, int]] = {}
    invalid_utf8: dict[str, dict[str, Any]] = {}
    if resumable and progress.is_file():
        rejected: list[str] = []
        for line in progress.read_text(errors="replace").splitlines():
            try:
                entry = json.loads(line)
                predicate, name, rows = entry["predicate"], entry["file"], int(entry["rows"])
            except (ValueError, KeyError, TypeError):
                continue  # a line cut when the job was killed
            problem = store_file_problem(path / "rows" / name, rows)
            if problem is None:
                done[predicate] = (name, rows)
                if entry.get("invalid_utf8"):
                    invalid_utf8[predicate] = entry["invalid_utf8"]
            else:
                rejected.append(f"<{predicate}> ({name}: {problem})")
                done.pop(predicate, None)
        logger.info("Scan: resuming %s with %d predicates read", path, len(done))
        if rejected:
            logger.warning(
                "Scan: %d predicates read before are read again, their files are not whole: %s",
                len(rejected),
                "; ".join(rejected[:5]),
            )
    if resumable and (path / "types.parquet").is_file():
        problem = store_file_problem(path / "types.parquet")
        if problem is not None:
            logger.warning("Scan: the type table is read again (%s)", problem)
            (path / "types.parquet").unlink()
    t0 = time.monotonic()
    counted = path / "predicates.parquet"
    sizes = {
        p[1:-1]: n
        for (p,), n in _counts(
            endpoint, "SELECT ?p (COUNT(?s) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p", counted
        )
    }
    named = _has_named_graphs(endpoint, path)
    known = list(named_graphs or [])
    graphs: dict[str, dict[str, int]] | None = None
    unnamed: dict[str, int] = {}
    split: dict[str, Any] = {"state": "by_graph" if named else "no_named_graphs"}
    if named:
        unnamed = {
            p[1:-1]: n
            for (p,), n in _counts(
                endpoint,
                f"SELECT ?p (COUNT(?s) AS ?n) FROM <{QLEVER_DEFAULT_GRAPH}> "  # noqa: S608 (SPARQL)
                "WHERE { ?s ?p ?o } GROUP BY ?p",
                counted,
            )
        }
        try:
            graphs = _graph_counts(endpoint, counted, sizes, unnamed, known)
        except QueryTimeoutError as error:
            # The rows are read without their graphs: every triple is mined, the per-graph
            # schemas and a graph scope are not available (RowStore.view).
            logger.warning(
                "Scan: the rows of each graph could not be counted in time; the rows are read "
                "without their graphs (graph split missing): %s",
                str(error)[:200],
            )
            split = {"state": "missing", "reason": str(error)[:500]}
            named, graphs, unnamed = False, None, {}
            counted.unlink(missing_ok=True)
            counted.with_suffix(".tsv").unlink(missing_ok=True)
        else:
            if unnamed:
                graphs[QLEVER_DEFAULT_GRAPH] = unnamed
            split["graphs_from"] = "registry" if known else "index"
    # Files read before in the other mode (with or without the graph of each row) are read
    # again: a resumed export whose graph split failed this time, or succeeded.
    for predicate, (name, _) in list(done.items()):
        if _has_graph_column(path / "rows" / name) != named:
            del done[predicate]
    types_file = path / "types.parquet"
    if resumable and types_file.is_file() and _has_graph_column(types_file) != named:
        types_file.unlink()
    expected = {
        p: sum(g.get(p, 0) for g in graphs.values()) if graphs is not None else n
        for p, n in sizes.items()
    }
    workers = workers or export_workers()
    streams = ThreadPoolExecutor(max_workers=workers)
    if not (resumable and (path / "types.parquet").is_file()):
        types = []
        for number, prop in enumerate(membership):
            part = path / f"types-{number}.parquet"
            if not named:
                query = f"SELECT ?s ?c WHERE {{ ?s <{prop}> ?c }}"
                _read_query(streams, endpoint, query, part, {"sid": 0}, sizes.get(prop, 0))
                types.append(pl.scan_parquet(part))
                continue
            query = f"SELECT ?g ?s ?c WHERE {{ GRAPH ?g {{ ?s <{prop}> ?c }} }}"
            in_graphs = expected.get(prop, 0) - unnamed.get(prop, 0)
            _read_query(streams, endpoint, query, part, {"sid": 1}, in_graphs)
            types.append(pl.scan_parquet(part).select("s", "c", "g", "sid"))
            if unnamed.get(prop):
                rest = path / f"types-{number}-unnamed.parquet"
                query = (
                    f"SELECT ?s ?c FROM <{QLEVER_DEFAULT_GRAPH}> "  # noqa: S608 (SPARQL)
                    f"WHERE {{ ?s <{prop}> ?c }}"
                )
                _read_query(
                    streams,
                    endpoint,
                    query,
                    rest,
                    {"sid": 0},
                    unnamed[prop],
                    extra={"g": f"<{QLEVER_DEFAULT_GRAPH}>"},
                )
                types.append(pl.scan_parquet(rest).select("s", "c", "g", "sid"))
        # The rows of one query are distinct; parts can repeat a row only when there are
        # several membership properties (deduplicating a large table costs much memory).
        joined = path / "types.parquet.partial"
        if len(membership) == 1 and len(types) == 1:
            (path / "types-0.parquet").rename(joined)
        elif len(membership) == 1:
            pl.concat(types).sink_parquet(joined)
        else:
            pl.concat(types).unique().sink_parquet(joined)
        type_parts = [path / f"types-{n}.parquet" for n in range(len(membership))]
        type_parts += [path / f"types-{n}-unnamed.parquet" for n in range(len(membership))]
        invalid_types = _take_invalid_utf8(*type_parts)
        _write_text(path / TYPES_INVALID_UTF8, json.dumps(invalid_types or {}) + "\n")
        _durable(joined, path / "types.parquet")
        for part in path.glob("types-*.parquet"):
            part.unlink()
    if (path / TYPES_INVALID_UTF8).is_file():
        found = json.loads((path / TYPES_INVALID_UTF8).read_text() or "{}")
        if found:
            invalid_utf8["membership (types.parquet)"] = found
    text = _text_predicates()
    lock = threading.Lock()

    def read(job: tuple[int, str, int]) -> tuple[str, str | None, tuple[str, int, int] | None]:
        """Write the rows of one predicate; return it, its file (None for a gap: a predicate
        whose query the server refused, 400) and any count mismatch.
        """
        import urllib.error

        try:
            return read_predicate(job)
        except urllib.error.HTTPError as error:
            if error.code != 400:
                raise
            body = error.read(1000).decode(errors="replace")
            logger.warning("Scan: the server refused the rows of <%s> (400): %s", job[1], body)
            with lock:
                gaps[job[1]] = f"HTTP 400: {body[:500]}"
            return job[1], None, None

    def _long_literals(
        predicate: str, file: Path, error: QueryTimeoutError, *, graph: bool, keep: bool
    ) -> tuple[int, dict[str, Any]]:
        """Read a predicate whose rows timed out as the ids of its literals with a sample.

        _read_long_literals reads them; it is logged once.
        """
        logger.warning(
            "Scan: the rows of <%s> could not be read in time (%s); its literals are read as "
            "ids with the text of a sample of %d",
            predicate,
            str(error)[:160],
            LONG_TEXT_SAMPLE,
        )
        where = f"GRAPH ?g {{ ?s <{predicate}> ?o }}" if graph else f"?s <{predicate}> ?o"
        rows, info = _read_long_literals(endpoint, file, where, graph=graph, keep_text=keep)
        info["reason"] = str(error)[:300]
        return rows, info

    def read_predicate(
        job: tuple[int, str, int],
    ) -> tuple[str, str | None, tuple[str, int, int] | None]:
        """Write the rows of one predicate (read)."""
        number, predicate, size = job
        if predicate in done:
            name, rows = done[predicate]
            return predicate, name, (predicate, size, rows) if rows != size else None
        file = path / "rows" / f"{number:05d}.parquet"
        keep = predicate in text
        long_text: dict[str, Any] | None = None
        if not named:
            try:
                rows = _read_query(
                    streams,
                    endpoint,
                    _row_query(predicate, datatype=False),
                    file,
                    {"sid": 0, "oid": 1},
                    size,
                    id_query=_row_query(predicate, datatype=False),
                    keep_text=keep,
                )
            except QueryTimeoutError as error:
                rows, long_text = _long_literals(predicate, file, error, graph=False, keep=keep)
        else:
            try:
                rows = _read_query(
                    streams,
                    endpoint,
                    _graph_row_query(predicate, datatype=False),
                    file,
                    {"gid": 0, "sid": 1, "oid": 2},
                    size - unnamed.get(predicate, 0),
                    id_query=_graph_row_query(predicate, datatype=False),
                    keep_text=keep,
                )
            except QueryTimeoutError as error:
                rows, long_text = _long_literals(predicate, file, error, graph=True, keep=keep)
            if unnamed.get(predicate):
                rest = file.with_name(file.stem + "-unnamed.parquet")
                rows += _read_query(
                    streams,
                    endpoint,
                    _unnamed_row_query(predicate, datatype=False),
                    rest,
                    {"sid": 0, "oid": 1},
                    unnamed[predicate],
                    id_query=_unnamed_row_query(predicate, datatype=False),
                    keep_text=keep,
                    extra={"g": f"<{QLEVER_DEFAULT_GRAPH}>"},
                )
                pl.concat(
                    [
                        pl.scan_parquet(file),
                        pl.scan_parquet(rest).with_columns(gid=pl.lit(None, pl.UInt64)),
                    ],
                    how="diagonal_relaxed",
                ).sink_parquet(file.with_name(file.name + ".all"))
                _durable(file.with_name(file.name + ".all"), file)
                rest.unlink()
        # A predicate is marked read only after its file is whole on disk (_durable).
        entry: dict[str, Any] = {"predicate": predicate, "file": file.name, "rows": rows}
        if long_text:
            entry["long_text"] = long_text
        invalid = _take_invalid_utf8(file, file.with_name(file.stem + "-unnamed.parquet"))
        if invalid:
            entry["invalid_utf8"] = invalid
        with lock, progress.open("a") as log:
            log.write(json.dumps(entry) + "\n")
            log.flush()
            os.fsync(log.fileno())
        return predicate, file.name, (predicate, size, rows) if rows != size else None

    gaps: dict[str, str] = {}
    # A predicate whose IRI is not UTF-8 was read with U+FFFD for its bytes (_tsv_to_parquet):
    # it cannot be asked for by name, so its rows are a gap of the store.
    for predicate in [p for p in sizes if "\ufffd" in p]:
        logger.warning(
            "Scan: the IRI of predicate <%s> is not UTF-8; its rows are a gap", predicate
        )
        gaps[predicate] = "the predicate IRI is not valid UTF-8"
        del sizes[predicate]
        expected.pop(predicate, None)
    order = sorted(sizes.items(), key=lambda x: (-x[1], x[0]))
    jobs = [(n, p, expected[p]) for n, (p, _) in enumerate(order)]
    # The predicates are read at once in their own threads, which wait for their queries in
    # the pool of streams (largest first; a thread never waits for a query of its own pool).
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(read, job) for job in jobs]
            try:
                finished = [future.result() for future in futures]
            except BaseException:
                for future in futures:
                    future.cancel()
                streams.shutdown(cancel_futures=True)
                raise
    finally:
        streams.shutdown(cancel_futures=True)
    for stale in (path / "rows").glob("*.slice-*"):
        stale.unlink()
    files = {predicate: name for predicate, name, _ in finished if name is not None}
    long_text: dict[str, dict[str, Any]] = {}
    for line in progress.read_text(errors="replace").splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if files.get(entry.get("predicate")) != entry.get("file"):
            continue
        if entry.get("invalid_utf8"):
            invalid_utf8[entry["predicate"]] = entry["invalid_utf8"]
        if entry.get("long_text"):
            long_text[entry["predicate"]] = entry["long_text"]
    if invalid_utf8:
        logger.warning(
            "Scan: %d rows hold bytes that are not UTF-8 (%s); they are read with U+FFFD for "
            "those bytes (manifest: invalid_utf8)",
            sum(int(found["rows"]) for found in invalid_utf8.values()),
            ", ".join(f"<{p}>" if "://" in p else p for p in list(invalid_utf8)[:5]),
        )
    mismatches = [m for _, _, m in finished if m]
    if mismatches:
        raise RuntimeError(f"Rows read differ from the predicate counts: {mismatches[:5]}")
    record: dict[str, Any] = {
        **plan,
        "endpoint": endpoint,
        "predicates": files,
        "triples": sum(sizes.values()),
        "graphs": graphs,
        "rows": expected,
        "triples_by_predicate": sizes,
        "seconds": round(time.monotonic() - t0, 1),
        "resumed_predicates": len(done),
        "literal_chars": LITERAL_CHARS,
        # The predicates whose rows the server refused: left out of the store (counted in
        # triples and rows, not in predicates).
        "gaps": dict(sorted(gaps.items())),
        "graph_split": split,
        # The predicates whose literals were read as ids with a sample of their text.
        "long_text": dict(sorted(long_text.items())),
        # The rows whose bytes were not UTF-8, read with U+FFFD for those bytes.
        "invalid_utf8": dict(sorted(invalid_utf8.items())),
    }
    _write_text(manifest, json.dumps(record, indent=1) + "\n")
    return RowStore(path)


def _term(node: Any) -> str:
    """Write an rdflib term as SPARQL TSV does."""
    from rdflib import BNode, Literal

    if isinstance(node, BNode):
        return f"_:{node}"
    if isinstance(node, Literal):
        return node.n3()
    return f"<{node}>"


def _quads(graph: Any) -> list[tuple[Any, Any, Any, str | None]]:
    """Return the quads of an rdflib graph or dataset; None as the graph of an unnamed triple."""
    from rdflib import Dataset

    if not isinstance(graph, Dataset):
        return [(s, p, o, None) for s, p, o in graph]
    default = graph.default_graph.identifier
    quads: set[tuple[Any, Any, Any, str | None]] = set()
    for context in graph.graphs():
        name = None if context.identifier == default else str(context.identifier)
        quads.update((s, p, o, name) for s, p, o in context)
    return sorted(quads, key=str)


def store_from_graph(graph: Any, path: Path) -> RowStore:
    """Build a row store from an rdflib graph or dataset, as export_index reads a QLever index.

    A dataset with named graphs gives a store with graphs: each named graph's triples with
    their graph, the triples of the default graph with QLEVER_DEFAULT_GRAPH (QLever keeps input
    without a graph there). The store's default graph is the union of all graphs, as QLever's
    is (an rdflib Dataset with default_union=True answers the same). The id of a value is a hash
    of its term, which identifies it as exactly as QLever's id does, since the term is written
    in full.
    """
    import polars as pl
    from rdflib import Literal

    path = Path(path)
    if path.exists():
        shutil.rmtree(path)
    (path / "rows").mkdir(parents=True)
    quads = _quads(graph)
    named = any(g is not None for *_, g in quads)
    by_predicate: dict[str, set[tuple[str, str, str | None, str | None]]] = {}
    for s, p, o, g in quads:
        datatype = None
        if isinstance(o, Literal):
            datatype = str(o.datatype) if o.datatype else (RDF_LANG if o.language else XSD_STRING)
        name = f"<{g or QLEVER_DEFAULT_GRAPH}>" if named else None
        by_predicate.setdefault(str(p), set()).add(
            (_term(s), _term(o), f"<{datatype}>" if datatype else None, name)
        )
    membership = list(MEMBERSHIP.get())
    types = {(s, o, g) for p in membership for s, o, _, g in by_predicate.get(p, ())}
    type_frame = pl.DataFrame(
        sorted(types, key=str),
        schema={"s": pl.String, "c": pl.String, "g": pl.String},
        orient="row",
    ).with_columns(sid=pl.col("s").hash())
    (type_frame if named else type_frame.drop("g")).write_parquet(path / "types.parquet")
    files = {}
    graphs: dict[str, dict[str, int]] | None = {} if named else None
    for number, (predicate, rows) in enumerate(sorted(by_predicate.items())):
        frame = pl.DataFrame(
            sorted(rows, key=str),
            schema={"s": pl.String, "o": pl.String, "d": pl.String, "g": pl.String},
            orient="row",
        ).with_columns(sid=pl.col("s").hash(), oid=pl.col("o").hash())
        if named and graphs is not None:
            frame = frame.with_columns(gid=pl.col("g").hash()).select(
                "s", "o", "d", "sid", "oid", "g", "gid"
            )
            for g, n in frame.group_by("g").len().iter_rows():
                graphs.setdefault(g[1:-1], {})[predicate] = n
        else:
            frame = frame.select("s", "o", "d", "sid", "oid")
        frame.write_parquet(path / "rows" / f"{number:05d}.parquet")
        files[predicate] = f"{number:05d}.parquet"
    (path / "store.json").write_text(
        json.dumps(
            {
                "version": STORE_VERSION,
                "index": None,
                "membership": membership,
                "predicates": files,
                "triples": sum(len({r[:3] for r in rows}) for rows in by_predicate.values()),
                "graphs": graphs,
                "rows": {p: len(rows) for p, rows in by_predicate.items()},
                "triples_by_predicate": {
                    p: len({r[:3] for r in rows}) for p, rows in by_predicate.items()
                },
            }
        )
    )
    return RowStore(path)


# Anonymous classes

EXPRESSION_IRI = "urn:rdfsolve:class-expression:"


def _curie(iri: str) -> str:
    from rdfsolve._uri import uri_to_curie

    curie = uri_to_curie(iri)[0]
    return curie if curie and curie != iri and ":" in curie and " " not in curie else f"<{iri}>"


LABEL_PROPERTIES = (
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
)


def _expression_terms(outgoing: dict[str, list[tuple[str, str]]]) -> set[str]:
    """Return the IRIs that appear in the expressions (classes, properties, individuals)."""
    return {o[1:-1] for pairs in outgoing.values() for _, o in pairs if o.startswith("<")}


def _labels(store: RowStore, iris: set[str]) -> dict[str, str]:
    """One label of each IRI from the data: rdfs:label before skos:prefLabel, English or no
    language before other languages, then the smallest text, so that the choice is stable.
    """
    import polars as pl

    if not iris:
        return {}
    terms = [f"<{iri}>" for iri in iris]
    found = []
    for rank, prop in enumerate(LABEL_PROPERTIES):
        if prop not in store.predicates:
            continue
        rows = store.rows(prop).filter(pl.col("s").is_in(terms) & (pl.col("kind") == "literal"))
        found.append(
            rows.select(
                "s",
                text=pl.col("o").str.extract(r'^"(.*)"(?:@[A-Za-z0-9-]+|\^\^<[^>]*>)?$', 1),
                lang=pl.col("o").str.extract(r'"@([A-Za-z0-9-]+)$', 1),
                rank=pl.lit(rank),
            ).collect()
        )
    if not found:
        return {}
    table = pl.concat(found).filter(pl.col("text").is_not_null() & (pl.col("text") != ""))
    table = table.with_columns(
        english=~(
            pl.col("lang").is_null() | pl.col("lang").str.to_lowercase().str.starts_with("en")
        )
    ).sort("rank", "english", "text")
    return {
        s[1:-1]: text
        for s, text in table.group_by("s", maintain_order=True)
        .first()
        .select("s", "text")
        .iter_rows()
    }


def name_class_expressions(store: RowStore) -> dict[str, dict[str, Any]]:
    """Give each OWL class expression used as a type a class IRI and a Manchester name.

    A type value that is a blank node (``x rdf:type [ a owl:Restriction ; ... ]``) is an
    anonymous class. Its expression is read from the rows of the expression predicates
    (rdfsolve.ontology.manchester), written in Manchester syntax with full IRIs, and named
    EXPRESSION_IRI + the SHA-256 of that text: the same expression in two places is one class.
    Its members are the subjects typed by any blank node with that expression. A blank node
    that is not a class expression OWL 2 maps to RDF stays out, as before. Writes
    types-named.parquet and class-expressions.json; returns the expressions.
    """
    import hashlib

    import polars as pl

    from rdfsolve.ontology.manchester import (
        EXPRESSION_PREDICATES,
        UnsupportedExpressionError,
        render,
    )

    for stale in ("types-named.parquet", "class-expressions.json"):
        (store.path / stale).unlink(missing_ok=True)
    # The type table is read lazily: on a large index its text does not fit in memory.
    types = pl.scan_parquet(store.path / "types.parquet")
    anonymous = (
        types.filter(pl.col("c").str.starts_with("_:"))
        .select("c")
        .unique()
        .collect()["c"]
        .to_list()
    )
    if not anonymous:
        return {}
    outgoing: dict[str, list[tuple[str, str]]] = {}
    frontier = set(anonymous)
    predicates = [p for p in store.predicates if p in EXPRESSION_PREDICATES]
    while frontier:
        found = pl.concat(
            [
                store.rows(p)
                .filter(pl.col("s").is_in(list(frontier)))
                .select("s", "o", q=pl.lit(p))
                .collect()
                for p in predicates
            ]
        )
        frontier = set()
        for s, o, q in found.iter_rows():
            outgoing.setdefault(s, []).append((q, o))
            if o.startswith("_:") and o not in outgoing:
                frontier.add(o)
    labels = _labels(store, _expression_terms(outgoing))

    def labelled(iri: str) -> str:
        """Write a term by its label in single quotes (Protege's rendering), else its CURIE."""
        label = labels.get(iri)
        return "'" + label.replace("'", "\\'") + "'" if label else _curie(iri)

    names: dict[str, str] = {}
    expressions: dict[str, dict[str, Any]] = {}
    unsupported = 0
    for node in anonymous:
        try:
            canonical = render(node, outgoing, lambda iri: f"<{iri}>")
            readable = render(node, outgoing, _curie)
            by_label = render(node, outgoing, labelled)
        except UnsupportedExpressionError:
            unsupported += 1
            continue
        iri = EXPRESSION_IRI + hashlib.sha256(canonical.encode()).hexdigest()[:24]
        names[node] = f"<{iri}>"
        expressions.setdefault(
            iri,
            {
                "manchester": readable,
                "manchester_labels": by_label,
                "manchester_iris": canonical,
                "nodes": 0,
            },
        )
        expressions[iri]["nodes"] += 1
    mapping = pl.LazyFrame(
        {"c": list(names), "named": list(names.values())},
        schema={"c": pl.String, "named": pl.String},
    )
    named = (
        types.join(mapping, on="c", how="left")
        .with_columns(c=pl.coalesce("named", "c"))
        .select(pl.exclude("named"))
        .unique()
    )
    named.sink_parquet(store.path / "types-named.parquet")
    members = dict(
        pl.scan_parquet(store.path / "types-named.parquet")
        .filter(pl.col("c").is_in([f"<{iri}>" for iri in expressions]))
        .group_by("c")
        .agg(pl.col("s").n_unique())
        .collect()
        .iter_rows()
    )
    for iri in expressions:
        expressions[iri]["members"] = members.get(f"<{iri}>", 0)
    record = dict(sorted(expressions.items()))
    (store.path / "class-expressions.json").write_text(json.dumps(record, indent=1) + "\n")
    if unsupported:
        logger.info("Scan: %d blank-node types are not OWL class expressions", unsupported)
    return record


# Patterns


BATCH_ROWS = 5_000_000
# A predicate with more rows than this is counted in parts (count_patterns): the rows of one
# part are held at once, with about ROW_BYTES a row at the peak of counting when each node has
# one class; a row of a node with k classes gives k classified rows at each end
# (count_patterns scales the parts by the classes a node has).
PARTITION_ROWS = 50_000_000
# A type table with more rows than this is joined in parts (by id). Up to three parts are held
# at once (the subjects' part, an objects' part and the typed ids), each with the hash table of
# its join: about TYPE_ROW_BYTES a row.
TYPE_PARTITION_ROWS = 200_000_000
ROW_BYTES = 300
TYPE_ROW_BYTES = 100
# The share of RDFSOLVE_SCAN_COUNT_GB that the parts of the type table take; the rows take the
# rest.
TYPE_SHARE = 1 / 3
COUNTS = ("count", "distinct_subjects", "distinct_objects")


def count_limits() -> tuple[int, int]:
    """Return the rows of a predicate and of the type table counted at once (PARTITION_ROWS,
    TYPE_PARTITION_ROWS), or both from RDFSOLVE_SCAN_COUNT_GB: the memory counting may use,
    in GB. The type table takes TYPE_SHARE of it, in three parts held at once (TYPE_ROW_BYTES a
    row); the rows of a part take the rest (ROW_BYTES a row when each node has one class).
    """
    import os

    value = os.environ.get("RDFSOLVE_SCAN_COUNT_GB")
    if not value:
        return PARTITION_ROWS, TYPE_PARTITION_ROWS
    budget = float(value) * 1e9
    rows = budget * (1 - TYPE_SHARE) / ROW_BYTES
    types = budget * TYPE_SHARE / 3 / TYPE_ROW_BYTES
    return max(1, int(rows)), max(1, int(types))


# Distinct classes in a count's scope above which count_patterns refuses (or
# RDFSOLVE_SCAN_MAX_CLASSES): it gives a pattern for each class and property, so millions of
# classes do not fit in its final table. Such classes are grouped under their ancestors before
# counting (ontology_group_before_mining).
SCAN_MAX_CLASSES = 1_000_000


class TooManyClassesError(ValueError):
    """The scope of a count holds more classes than count_patterns takes (SCAN_MAX_CLASSES)."""


def _max_classes() -> int:
    """Return SCAN_MAX_CLASSES, or RDFSOLVE_SCAN_MAX_CLASSES when it is set."""
    import os

    value = os.environ.get("RDFSOLVE_SCAN_MAX_CLASSES")
    return int(value) if value else SCAN_MAX_CLASSES


# Rows of a batch read to estimate its classified rows (_batch_expansion), and the rows of a
# batch from which it is estimated (a smaller batch is counted as is).
EXPANSION_SAMPLE = 1_000_000
EXPANSION_MIN_ROWS = 100_000


def _batch_expansion(store: CountedStore, batch: list[str], size: int) -> float:
    """Return the classified rows of a batch per row (at least 1), from a sample of its rows.

    A row whose subject has k classes and object m becomes k * m classified rows, and the
    joins of a part hold them, so parts sized by rows alone can exceed the memory budget. The
    sample is every n-th row (EXPANSION_SAMPLE in all); the classes of its nodes are read from
    the type table in a stream, for those nodes only.
    """
    import polars as pl

    step = max(1, size // EXPANSION_SAMPLE)
    rows = (
        pl.concat([store.rows(p).select("sid", "oid") for p in batch])
        .gather_every(step)
        .collect(engine="streaming")
    )
    if not rows.height:
        return 1.0
    nodes = pl.concat([rows["sid"], rows["oid"]]).unique()
    classes = (
        store.type_ids()
        .filter(pl.col("sid").is_in(nodes.implode()))
        .group_by("sid")
        .agg(k=pl.len())
        .collect(engine="streaming")
    )
    found = (
        rows.join(classes.rename({"k": "ks"}), on="sid", how="left")
        .join(classes.rename({"sid": "oid", "k": "ko"}), on="oid", how="left")
        .select((pl.col("ks").fill_null(1) * pl.col("ko").fill_null(1)).mean())
        .item()
    )
    return max(1.0, float(found or 1.0))


def store_rows(store: CountedStore, predicate: str) -> int:
    """Return the rows of *predicate* in the store's file (from the manifest when it has them)."""
    import polars as pl

    rows = (store.manifest.get("rows") or {}).get(predicate)
    if rows is not None:
        return int(rows)
    return int(pl.scan_parquet(store.predicates[predicate]).select(pl.len()).collect().item())


def _batches(store: CountedStore) -> list[list[str]]:
    """Group the predicates into batches of about BATCH_ROWS rows (a large one alone)."""
    sizes = {p: store_rows(store, p) for p in store.predicates}
    batches: list[list[str]] = []
    rows = BATCH_ROWS
    for predicate in sorted(sizes, key=lambda p: sizes[p]):
        # A store whose smallest predicate has no rows (a slice, a graph scope) starts a batch.
        if not batches or rows + sizes[predicate] > BATCH_ROWS:
            batches.append([])
            rows = 0
        batches[-1].append(predicate)
        rows += sizes[predicate]
    return batches


# The columns counting reads: ids, kind, datatype, and whether the subject is a blank node.
# The text of subjects and objects is not read (on a source with long literals it is most of
# the memory of a batch).
def _counted_columns(frame: pl.LazyFrame, predicate: str, *, by_graph: bool) -> pl.LazyFrame:
    import polars as pl

    # The text columns are categories (a code a row): a row of a large predicate is counted
    # with its ids and codes, not its text.
    return frame.select(
        "sid",
        "oid",
        pl.col("kind").cast(pl.Categorical),
        pl.col("d").cast(pl.Categorical),
        *([pl.col("g").cast(pl.Categorical)] if by_graph else []),
        sb=pl.col("s").str.starts_with("_:"),
        p=pl.lit(predicate).cast(pl.Categorical),
    )


def _batch_frame(store: CountedStore, predicates: list[str], *, by_graph: bool) -> pl.LazyFrame:
    import polars as pl

    read = store.graph_rows if by_graph else store.rows
    return pl.concat(
        [_counted_columns(read(p), p, by_graph=by_graph) for p in predicates],
        how="vertical_relaxed",
    )


def _batch_rows(
    store: CountedStore, predicates: list[str], *, by_graph: bool = False
) -> pl.DataFrame:
    """Return the counted columns of the rows of some predicates, with the predicate as a
    column p (and the graph of each row with *by_graph*).
    """
    return _batch_frame(store, predicates, by_graph=by_graph).collect()


def _spill(frame: pl.LazyFrame, directory: Path, parts: int, column: str) -> None:
    """Write *frame* in *parts* files by its id *column* modulo *parts* (directory/b=<part>)."""
    import polars as pl

    frame.with_columns(b=(pl.col(column) % parts).cast(pl.UInt32)).sink_parquet(
        pl.PartitionBy(directory, key="b", include_key=False), mkdir=True
    )


def _part(directory: Path, part: int, schema: pl.Schema) -> pl.DataFrame:
    """Read one part written by _spill (no rows when the part has none)."""
    import polars as pl

    files = sorted((directory / f"b={part}").glob("*.parquet"))
    if not files:
        return pl.DataFrame(schema=schema)
    return pl.read_parquet(files).select(pl.col(n).cast(t) for n, t in schema.items())


def _part_counts(classified: list[pl.DataFrame], keys: list[str]) -> list[pl.DataFrame]:
    """Count the patterns of classified rows (keys, sid, oid) that hold all rows of each key."""
    import polars as pl

    return [
        f.group_by(keys).agg(
            count=pl.len(),
            distinct_subjects=pl.col("sid").n_unique(),
            distinct_objects=pl.col("oid").n_unique(),
        )
        for f in classified
    ]


def _pattern_term(term: str, *, sentinels: bool = False) -> bool:
    """Return whether *term* can be a class or property of a SchemaPattern.

    It is an IRI (an IRI that is not an RDF IRI, with a space, is kept: iri_findings reports
    it) without control characters; an object may also be a sentinel (Literal, Resource,
    BlankNode). A term of NUL bytes, as a damaged count read can give, is not.
    """
    if sentinels and term in _SENTINEL_OBJECTS:
        return True
    return term.startswith(_URI_SCHEMES) and not any(ord(c) < 32 for c in term)


def _blank_outgoing(store: CountedStore) -> pl.DataFrame:
    """Return the predicates of each blank node (oid, q: an enum of the predicates), rdf:type
    for one that has a type in the data.

    Read before counting, so that a pattern's blank objects are joined with their predicates as
    they are found: keeping the blank objects of every pattern until the end held each
    blank-object row once for each class of its subject. Each predicate's blank subjects are
    read and made distinct on their own, in a stream, and kept as ids with the predicate as an
    enum: a row is a node id and a small integer. Only the rdf:type rows can repeat (a blank
    node typed in the rows of rdf:type and in the type table), and only they are made distinct
    together.
    """
    import polars as pl

    blank = pl.col("s").str.starts_with("_:")
    names = pl.Enum(sorted({*store.predicates, RDF_TYPE}))
    frames: list[pl.DataFrame] = []
    # The blank nodes typed in the data graphs (data_types), read by id from the type rows:
    # making the (node, class) text rows distinct first held every typed blank node's text.
    typed = store.base.graph_types().filter(blank)
    scope = getattr(store, "graph_uris", None)
    if scope:
        typed = typed.filter(pl.col("g").is_in([f"<{g}>" for g in scope]))
    typed = typed.select("sid")
    if RDF_TYPE in store.predicates:
        typed = pl.concat([typed, store.rows(RDF_TYPE).filter(blank).select("sid")])
    frames.append(
        typed.unique()
        .collect(engine="streaming")
        .select(oid="sid", q=pl.lit(RDF_TYPE, dtype=names))
    )
    for p in store.predicates:
        if p == RDF_TYPE:
            continue
        frames.append(
            store.rows(p)
            .filter(blank)
            .select(oid="sid")
            .unique()
            .collect(engine="streaming")
            .with_columns(q=pl.lit(p, dtype=names))
        )
    return pl.concat(frames, rechunk=True)


def _compact(*lists: list[pl.DataFrame]) -> None:
    """Replace the tables of each list by their distinct rows, in one table.

    The blank-node tables of count_patterns grow with every part and cell; kept as categories
    and made distinct as each part ends, they hold each row once.
    """
    import polars as pl

    for tables in lists:
        if len(tables) > 1:
            tables[:] = [pl.concat(tables, how="vertical_relaxed").unique()]


def _term_rows(table: pl.DataFrame, keys: list[str], scope: Sequence[str] | None) -> pl.DataFrame:
    """Return counted rows (keys as categories, COUNTS) as the per-term rows of a release.

    The rows of term_release.term_rows: text keys, the class of an untyped subject
    rdfs:Resource with subject_binding "untyped", the same rows left out as count_patterns
    leaves out.
    """
    import polars as pl

    membership = list(MEMBERSHIP.get())
    rows = table.select(
        *(pl.col(k).cast(pl.String) for k in keys),
        *(pl.col(c).cast(pl.UInt64) for c in COUNTS),
    ).with_columns(
        subject_class=_bare(pl.col("subject_class")), object_class=_bare(pl.col("object_class"))
    )
    rows = rows.filter(
        ~pl.col("subject_class").str.starts_with("_:")
        & ~pl.col("object_class").str.starts_with("_:")
        & ~(pl.col("p").is_in(membership) & (pl.col("object_class") == "Resource"))
    )
    untyped = pl.col("subject_class") == UNTYPED
    return rows.select(
        subject_class=pl.when(untyped).then(pl.lit(UNTYPED_SUBJECT)).otherwise("subject_class"),
        property="p",
        object_class="object_class",
        datatype="datatype",
        graph=pl.col("g").str.strip_prefix("<").str.strip_suffix(">")
        if scope
        else pl.lit(None, pl.String),
        triples="count",
        distinct_subjects="distinct_subjects",
        distinct_objects="distinct_objects",
        subject_binding=pl.when(untyped).then(pl.lit("untyped")).otherwise(pl.lit("type")),
    )


def _flush_rows(
    tables: list[pl.DataFrame], keys: list[str], scope: Sequence[str] | None, directory: Path
) -> list[Path]:
    """Write the counted tables as per-term rows (_term_rows) in *directory*; empty the list."""
    paths = []
    for table in tables:
        path = directory / f"rows-{len(list(directory.glob('rows-*.parquet'))):06d}.parquet"
        _term_rows(table, keys, scope).write_parquet(path)
        paths.append(path)
    tables.clear()
    return paths


def count_patterns(
    store: CountedStore,
    invalid: dict[str, int] | None = None,
    *,
    rows_path: Path | None = None,
) -> list[SchemaPattern]:
    """Return the patterns of the store with triples, distinct subjects and distinct objects.

    The patterns follow the definitions of the two-phase count queries: typed objects
    (_build_batched_typed_count_query) give one row per subject class and object class;
    untyped IRIs (_build_batched_untyped_count_query) "Resource"; blank nodes
    (_build_batched_blank_node_count_query) every blank-node object, typed or not, with the
    predicates of those blank nodes; literals one row per datatype. IRI subjects without a type
    (no membership row in scope: typed_ids) give the same rows with the subject class
    rdfs:Resource and subject_binding "untyped", as rdfsolve.mining.untyped_subjects counts
    them through an endpoint; blank-node subjects without a type are left out (the patterns of
    the nodes that point to them and the structural patterns describe them). Each row of the
    store is read once; small predicates are counted together, in batches of about BATCH_ROWS
    rows.

    Memory is bound by count_limits. A predicate (or batch) with more rows than PARTITION_ROWS
    is written once to a scratch directory in the store, in parts by subject id, and counted
    one part at a time: triples and distinct subjects add up over parts; the distinct
    (pattern, object) pairs of each part are written in parts by object id and counted per
    object part, so an object shared by most rows (a class, a constant) costs one pair, not
    its rows. A type table with more rows than TYPE_PARTITION_ROWS is split by id the same way:
    the subjects of a part are joined with their part of the types, and the objects of each
    part of the types with that part.

    In a graph scope (a StoreView) the counts are those of the count queries, which group by the
    edge graph ?_g (pattern_enrichment.enrich_patterns_with_counts merges them): each edge is
    counted in its data graph, types come from the data and type context graphs; count is the
    sum over the graphs, graphs the triples in each; one data graph gives triples_in_graph with
    its distinct counts, several give quad_occurrences without distinct counts (a node in two
    graphs would be counted twice). The distinct counts of each graph are kept in
    graph_distinct_subjects and graph_distinct_objects, for the per-graph schemas.

    A row whose predicate or class is not a term a pattern can hold (_pattern_term) is left
    out; its triples are added to *invalid* by term (repr), so that the caller reports them.

    With *rows_path*, the counts are written there as the per-term rows of a release
    (_term_rows) as each batch ends, and no pattern is returned: the per-term layer of a
    source whose terms are grouped before counting (millions of classes) is never held whole.
    """
    import tempfile

    import polars as pl

    scope = getattr(store, "graph_uris", None)
    by_graph = bool(scope)
    keys = ["p", *(["g"] if scope else []), "subject_class", "object_class", "datatype"]
    tables: list[pl.DataFrame] = []
    # (subject class, predicate, predicate of the blank object): the blank-node predicates of
    # the patterns, kept small as they are found (_blank_outgoing).
    blank_fields: list[pl.DataFrame] = []
    partition_rows, type_rows = count_limits()
    limit = _max_classes()
    # The distinct classes are those of the type rows: read without making the rows distinct
    # first, which would hold every (node, class) row of a large type table.
    classes = int(
        store.graph_types().select(pl.col("c").approx_n_unique()).collect(engine="streaming").item()
    )
    if classes > limit and rows_path is None:
        raise TooManyClassesError(
            f"The scope holds about {classes:,} classes, more than the {limit:,} that a scan "
            "count takes (a pattern for each class and property): group the ontology terms "
            "used as types before counting (ontology_group_before_mining), or raise "
            "RDFSOLVE_SCAN_MAX_CLASSES"
        )
    typed_ids = store.typed_ids() if hasattr(store, "typed_ids") else store.type_ids().select("sid")
    typed_ids = typed_ids.select("sid").unique()
    # The type rows of the whole index bound those of the scope (a view or a retyped table).
    type_count = int(store.base.graph_types().select(pl.len()).collect().item())
    type_parts = max(1, -(-type_count // type_rows))
    scratch: list[Path] = []
    blank_out = _blank_outgoing(store)
    # The files of per-term rows written so far (rows_path).
    written: list[Path] = []

    def directory() -> Path:
        """Return the scratch directory of this count (made when first needed)."""
        if not scratch:
            scratch.append(Path(tempfile.mkdtemp(prefix=".count-", dir=store.base.path)))
        return scratch[0]

    def classify(
        rows: pl.DataFrame,
        subjects: pl.DataFrame,
        typed_subjects: pl.DataFrame,
        objects: pl.DataFrame,
    ) -> list[pl.DataFrame]:
        """Give each row its patterns: the keys with sid and oid, in four tables (literal,
        typed object, untyped IRI object, blank object).
        """
        # The predicates of a blank node are read in the RDF merge of the data graphs
        # (the OPTIONAL of _build_batched_blank_node_query has no GRAPH).
        typed = rows.join(subjects, on="sid", how="inner")
        # IRI subjects without a type, under one subject class of their own (UNTYPED).
        untyped_subjects = (
            rows.filter(~pl.col("sb"))
            .join(typed_subjects, on="sid", how="anti")
            .with_columns(subject_class=pl.lit(UNTYPED).cast(pl.Categorical))
        )
        if untyped_subjects.height:
            typed = pl.concat(
                [
                    typed.with_columns(pl.col("subject_class").cast(pl.Categorical)),
                    untyped_subjects.with_columns(pl.col("subject_class").cast(pl.Categorical)),
                ],
                how="vertical_relaxed",
            )
        literal = typed.filter(pl.col("kind") == "literal").with_columns(
            object_class=pl.lit("Literal").cast(pl.Categorical), datatype=pl.col("d")
        )
        nonliteral = typed.filter(pl.col("kind") != "literal").with_columns(
            datatype=pl.lit(None, pl.Categorical)
        )
        with_types = nonliteral.join(objects, on="oid", how="left")
        typed_object = with_types.filter(pl.col("oc").is_not_null()).with_columns(
            object_class=pl.col("oc").cast(pl.Categorical)
        )
        untyped = with_types.filter(
            pl.col("oc").is_null() & (pl.col("kind") == "iri")
        ).with_columns(object_class=pl.lit("Resource").cast(pl.Categorical))
        blank = nonliteral.filter(pl.col("kind") == "bnode")
        blank_fields.append(
            blank.select(pl.col("subject_class").cast(pl.Categorical), "p", "oid")
            .unique()
            .join(blank_out, on="oid", how="inner")
            .select("subject_class", "p", "q")
            .unique()
        )
        blank = blank.with_columns(object_class=pl.lit("BlankNode").cast(pl.Categorical))
        return [
            f.with_columns(pl.col("subject_class").cast(pl.Categorical)).select(*keys, "sid", "oid")
            for f in (literal, typed_object, untyped, blank)
        ]

    # Joins use QLever's ids (an IRI or blank node has one id as subject and as object), and
    # the classes as categories: the type table of a large index fits in memory this way.
    if type_parts == 1:
        # type_ids read from the type rows with the classes as categories before they are made
        # distinct: distinct (id, text) rows hold the text of every row.
        types = (
            store.graph_types()
            .select("sid", pl.col("c").cast(pl.Categorical))
            .unique()
            .collect(engine="streaming")
        )
        typed_all = typed_ids.collect(engine="streaming")

        def type_part(part: int) -> pl.DataFrame:
            """Return one part of the type table (sid, c)."""
            return types

        def typed_part(part: int) -> pl.DataFrame:
            """Return one part of the typed ids (sid)."""
            return typed_all

    else:
        for part in range(type_parts):
            selected = pl.col("sid") % type_parts == part
            folder = directory() / "types"
            folder.mkdir(exist_ok=True)
            store.type_ids().filter(selected).with_columns(pl.col("c").cast(pl.String)).collect(
                engine="streaming"
            ).write_parquet(folder / f"{part}.parquet")
            typed_ids.filter(selected).collect(engine="streaming").write_parquet(
                folder / f"typed-{part}.parquet"
            )

        def type_part(part: int) -> pl.DataFrame:
            """Return one part of the type table (sid, c)."""
            return pl.read_parquet(directory() / "types" / f"{part}.parquet").with_columns(
                pl.col("c").cast(pl.Categorical)
            )

        def typed_part(part: int) -> pl.DataFrame:
            """Return one part of the typed ids (sid)."""
            return pl.read_parquet(directory() / "types" / f"typed-{part}.parquet")

    def renamed(types: pl.DataFrame) -> pl.DataFrame:
        """Return a type table as the types of objects (oid, oc)."""
        return types.rename({"sid": "oid", "c": "oc"})

    try:
        for unit, batch in enumerate(_batches(store)):
            size = sum(store_rows(store, p) for p in batch)
            # Parts hold classified rows (rows times the classes at each end), not rows.
            expansion = _batch_expansion(store, batch, size) if size >= EXPANSION_MIN_ROWS else 1.0
            row_parts = -(-int(size * expansion) // partition_rows)
            if row_parts <= 1 and type_parts == 1:
                rows = _batch_rows(store, batch, by_graph=by_graph)
                cells = classify(
                    rows, types.rename({"c": "subject_class"}), typed_all, renamed(types)
                )
                tables.extend(_part_counts(cells, keys))
                if rows_path is not None:
                    written.extend(_flush_rows(tables, keys, scope, directory()))
                _compact(blank_fields)
                continue
            # Parts by subject id; the parts of the type table divide them (parts is a multiple
            # of type_parts), so the subjects of a part are in one part of the types.
            parts = type_parts * -(-row_parts // type_parts)
            logger.info(
                "Scan count: %d rows of %d predicates in %d parts (types in %d; %.1f classified "
                "rows a row)",
                size,
                len(batch),
                parts,
                type_parts,
                expansion,
            )
            frame = _batch_frame(store, batch, by_graph=by_graph)
            schema = frame.collect_schema()
            folder = directory() / f"unit-{unit}"
            _spill(frame, folder / "rows", parts, "sid")
            # A batch with fewer rows than the type table reads the types of its own nodes
            # only, once: each part joins its rows with every part of the type table, and
            # reading those whole for each part made a batch many times slower.
            own: dict[int, pl.DataFrame] = {}
            if size * 2 < type_count:
                nodes = pl.concat(
                    [
                        pl.scan_parquet(folder / "rows" / "*" / "*.parquet").select("sid"),
                        pl.scan_parquet(folder / "rows" / "*" / "*.parquet").select(sid="oid"),
                    ]
                ).unique()
                for type_number in range(type_parts):
                    own[type_number] = (
                        type_part(type_number).lazy().join(nodes, on="sid", how="semi").collect()
                    )

            def unit_types(type_number: int, own: dict[int, pl.DataFrame] = own) -> pl.DataFrame:
                """Return a part of the type table, restricted to this batch's nodes if read."""
                return own[type_number] if own else type_part(type_number)

            sums, subject_counts = [], []
            for part in range(parts):
                rows = _part(folder / "rows", part, schema)
                selected = pl.col("sid") % parts == part
                subjects = unit_types(part % type_parts).filter(selected)
                typed_subjects = typed_part(part % type_parts).filter(selected)
                pairs = []
                for object_part in range(type_parts):
                    cell = rows.filter(pl.col("oid") % type_parts == object_part)
                    objects = renamed(unit_types(object_part))
                    cells = classify(
                        cell,
                        subjects.rename({"c": "subject_class"}),
                        typed_subjects,
                        objects,
                    )
                    for index, f in enumerate(cells):
                        sums.append(f.group_by(keys).agg(count=pl.len()))
                        pairs.append(f.select(*keys, "sid").unique())
                        by_object = (
                            f.select(*keys, "oid")
                            .unique()
                            .with_columns(b=(pl.col("oid") % parts).cast(pl.UInt32))
                        )
                        for (b,), chunk in by_object.partition_by(
                            "b", as_dict=True, include_key=False
                        ).items():
                            target = folder / "objects" / str(b)
                            target.mkdir(parents=True, exist_ok=True)
                            chunk.write_parquet(target / f"{part}-{object_part}-{index}.parquet")
                subject_counts.append(
                    pl.concat(pairs).unique().group_by(keys).agg(distinct_subjects=pl.len())
                )
                # The counts of the parts read so far are added up as each part ends, so that
                # they take the rows of their keys, not of their keys in every part and cell.
                sums = [pl.concat(sums).group_by(keys).agg(pl.col("count").sum())]
                _compact(blank_fields)
                subject_counts = [
                    pl.concat(subject_counts).group_by(keys).agg(pl.col("distinct_subjects").sum())
                ]
            object_counts = [
                pl.read_parquet(sorted(target.glob("*.parquet")))
                .unique()
                .group_by(keys)
                .agg(distinct_objects=pl.len())
                for target in sorted((folder / "objects").glob("*"))
            ]
            counted = (
                pl.concat(sums)
                .group_by(keys)
                .agg(pl.col("count").sum())
                .join(
                    pl.concat(subject_counts).group_by(keys).agg(pl.col("distinct_subjects").sum()),
                    on=keys,
                    how="left",
                    nulls_equal=True,
                )
            )
            if object_counts:
                counted = counted.join(
                    pl.concat(object_counts).group_by(keys).agg(pl.col("distinct_objects").sum()),
                    on=keys,
                    how="left",
                    nulls_equal=True,
                )
            else:
                counted = counted.with_columns(distinct_objects=pl.lit(0))
            tables.append(counted)
            if rows_path is not None:
                written.extend(_flush_rows(tables, keys, scope, directory()))
            shutil.rmtree(folder)
        if rows_path is not None:
            # The rows of all batches, in one file, before the scratch directory is removed.
            frames = [pl.scan_parquet(path) for path in written]
            if frames:
                pl.concat(frames).sink_parquet(rows_path)
            else:
                _term_rows(
                    pl.DataFrame(
                        schema={
                            **dict.fromkeys(keys, pl.String),
                            **dict.fromkeys(COUNTS, pl.Int64),
                        }
                    ),
                    keys,
                    scope,
                ).write_parquet(rows_path)
    finally:
        if scratch:
            shutil.rmtree(scratch[0], ignore_errors=True)
    if rows_path is not None or not tables:
        return []
    membership = list(MEMBERSHIP.get())
    # The keys were counted as categories; the patterns hold their text.
    table = pl.concat(
        [
            t.select(
                *(pl.col(k).cast(pl.String) for k in keys),
                *(pl.col(c).cast(pl.Int64) for c in COUNTS),
            )
            for t in tables
        ]
    ).with_columns(
        subject_class=_bare(pl.col("subject_class")), object_class=_bare(pl.col("object_class"))
    )
    # Classes are IRIs (anonymous classes were given one by name_class_expressions); a blank
    # node that is not a class expression is not a class. (C, rdf:type, Resource) says only
    # that the class IRI has no type (miner.py keeps the rows whose type value has a class).
    table = table.filter(
        ~pl.col("subject_class").str.starts_with("_:")
        & ~pl.col("object_class").str.starts_with("_:")
        & ~(pl.col("p").is_in(membership) & (pl.col("object_class") == "Resource"))
    )
    # The predicates of the blank nodes each (subject class, predicate) points to, with
    # rdf:type when the blank node has a type in the data.
    _compact(blank_fields)
    fields = (
        pl.concat(blank_fields)
        .select(
            pl.col("subject_class").cast(pl.String),
            pl.col("p").cast(pl.String),
            pl.col("q").cast(pl.String),
        )
        .group_by("subject_class", "p")
        .agg(pl.col("q").unique().sort())
        .with_columns(subject_class=_bare(pl.col("subject_class")))
    )
    blank = {(c, p): list(q) for c, p, q in fields.iter_rows()}
    merged: dict[tuple[str, str, str, str | None], dict[str, tuple[int, int, int]]] = {}
    for row in table.iter_rows(named=True):
        key = (row["subject_class"], row["p"], row["object_class"], row["datatype"])
        graph = row["g"][1:-1] if scope else ""
        merged.setdefault(key, {})[graph] = (
            row["count"],
            row["distinct_subjects"],
            row["distinct_objects"],
        )
    semantics = (
        "endpoint_default"
        if not scope
        else "triples_in_graph"
        if len(scope) == 1
        else "quad_occurrences"
    )
    patterns: list[SchemaPattern] = []
    for (key_subject, predicate, obj, datatype), per_graph in merged.items():
        untyped = key_subject == UNTYPED
        subject = UNTYPED_SUBJECT if untyped else key_subject
        bad = [
            term
            for term, ok in (
                (predicate, _pattern_term(predicate)),
                (subject, untyped or _pattern_term(subject)),
                (obj, _pattern_term(obj, sentinels=True)),
            )
            if not ok
        ]
        if bad:
            if invalid is not None:
                for term in bad:
                    name = repr(term[:200])
                    invalid[name] = invalid.get(name, 0) + sum(n for n, _, _ in per_graph.values())
            continue
        # The miner's rule: distinct counts of the whole scope only when it is one graph.
        ds = do = None
        if len(per_graph) == 1 and (not scope or len(scope) == 1):
            _, ds, do = next(iter(per_graph.values()))
        patterns.append(
            SchemaPattern(
                subject_class=subject,
                subject_binding="untyped" if untyped else "type",
                property_uri=predicate,
                object_class=obj,
                datatype=datatype,
                count=sum(n for n, _, _ in per_graph.values()),
                distinct_subjects=ds,
                distinct_objects=do,
                count_semantics=semantics,
                graphs={g: n for g, (n, _, _) in sorted(per_graph.items())} if scope else None,
                graph_distinct_subjects={g: m[1] for g, m in sorted(per_graph.items())}
                if scope
                else None,
                graph_distinct_objects={g: m[2] for g, m in sorted(per_graph.items())}
                if scope
                else None,
                blank_node_predicates=(blank.get((key_subject, predicate)) or None)
                if obj == "BlankNode"
                else None,
                pattern_type={
                    "Literal": PatternType.DATATYPE_PROPERTY,
                    "BlankNode": PatternType.BLANK_NODE_PROPERTY,
                }.get(obj, PatternType.OBJECT_PROPERTY),
            )
        )
    return sorted(
        patterns,
        key=lambda p: (
            p.subject_class,
            p.subject_binding,
            p.property_uri,
            p.object_class,
            p.datatype or "",
        ),
    )


# Strategy


class ScanStrategy(MiningStrategy):
    """Mine a local QLever index by reading its rows into a row store and counting in Polars.

    The patterns come with their counts, so the miner skips its counts phase. *store_dir* is
    where the rows are saved (next to the index); *index* describes the index, so that a store
    of the same index is reused. A *store* already built (tests, a store from files) is used
    as is. The graph scope of the run (context.graph_uris, context.type_context_graph_uris)
    selects a view of the store (RowStore.view); ``store`` is then that view, which the miner's
    other scan phases (rdfsolve.mining.scan_structure) and the tested paths read.
    """

    counted = True

    def __init__(
        self,
        store_dir: Path | None = None,
        *,
        index: dict[str, Any] | None = None,
        store: RowStore | None = None,
        graphs: Sequence[str] | None = None,
    ) -> None:
        """Keep where the rows go, or the store to read, and the index's graphs when known."""
        self.store_dir = store_dir
        self.index = index
        self.graphs = list(graphs) if graphs else None
        self.row_store = store
        self.store: RowStore | StoreView | None = store
        self.grouped: Any = None

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "scan"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Read the rows (unless a store is given) and count the patterns in the run's scope."""
        if self.row_store is None:
            if self.store_dir is None:
                raise ValueError("Give the scan strategy a store or a directory for one")
            phase = context.report.start_phase("scan-export")
            self.row_store = export_index(
                context.helper.endpoint_url,
                self.store_dir,
                index=self.index,
                named_graphs=self.graphs,
            )
            context.report.finish_phase(phase, items=len(self.row_store.predicates))
            context.report.report.config["scan_store"] = {
                "path": str(self.row_store.path),
                "triples": self.row_store.manifest.get("triples"),
                "predicates": len(self.row_store.predicates),
                "graphs": len(self.row_store.graphs or {}),
                "seconds": self.row_store.manifest.get("seconds"),
                "gaps": self.row_store.manifest.get("gaps") or {},
                "graph_split": self.row_store.manifest.get("graph_split"),
            }
        invalid_utf8 = self.row_store.manifest.get("invalid_utf8") or {}
        if invalid_utf8:
            # Rows read with U+FFFD for bytes that are not UTF-8: a finding of the source.
            context.report.report.config["scan_invalid_utf8"] = {
                "rows": sum(int(found["rows"]) for found in invalid_utf8.values()),
                "by_predicate": invalid_utf8,
            }
        from rdfsolve.mining.sampling import flag

        long_text = self.row_store.manifest.get("long_text") or {}
        if long_text:
            # Literals read as ids, with the datatype and language of a sample of their text.
            context.report.report.config["scan_long_literals"] = long_text
        self.store = self.row_store.view(context.graph_uris, context.type_context_graph_uris)
        phase = context.report.start_phase("scan-patterns")
        from rdfsolve.mining.scan_terms import group_before_counting

        # Above group_before_mining classes, the terms used as types are grouped first and the
        # grouped type table is counted (scan_terms.group_before_counting); every later phase
        # reads it through self.store.
        self.grouped = group_before_counting(
            self.store,
            limit=context.group_before_mining if context.ontology_term_budget else None,
            budget=context.ontology_term_budget or 300,
            classes_as_data=bool(getattr(context, "classes_as_data", False)),
            ontology_graph_uris=context.ontology_graph_uris,
            hierarchy_files=context.ontology_hierarchy_files,
        )
        if self.grouped is not None:
            self.store = self.grouped.store
            context.report.report.config["ontology_term_grouping"] = self.grouped.record
            if self.grouped.representative:
                members: dict[str, list[str]] = {}
                for term, rep in sorted(self.grouped.representative.items()):
                    members.setdefault(rep, []).append(term)
                context.grouped_members = members
        literal_types = self.row_store.literal_type_values()
        if literal_types:
            # The SPARQL strategies count them as literal_type_values too (record_dropped_uri).
            context.report.report.config["literal_type_values"] = literal_types
            logger.warning(
                "%d membership rows have a type value that is not a class (%d values, first: %s):"
                " their subjects are mined as untyped (report: literal_type_values)",
                literal_types["count"],
                literal_types["values"],
                literal_types["samples"][0]["value"],
            )
        expressions = name_class_expressions(self.row_store)
        if expressions:
            context.report.report.config["class_expressions"] = expressions
        invalid = {repr(p[:200]): n for p, n in self.row_store.left_out_predicates.items()}
        patterns = count_patterns(self.store, invalid)
        for pattern in patterns:
            found = long_text.get(pattern.property_uri)
            if found and pattern.object_class == "Literal":
                # Exact counts (from the ids); the datatype is that of a sample of the text.
                flag(
                    pattern,
                    {
                        "size": found["sampled_rows"],
                        "unit": "rows",
                        "reason": "timeout: the literal text was read for a sample of the rows",
                    },
                    ["patterns"],
                )
        if invalid:
            # Not a pattern's term: left out and reported, the source goes on.
            context.report.report.config["scan_invalid_terms"] = {
                "triples_left_out": sum(invalid.values()),
                "terms": [{"term": t, "triples": n} for t, n in sorted(invalid.items())],
            }
            logger.warning(
                "Scan: %d triples with %d terms that cannot be a class or property left out "
                "(report: scan_invalid_terms)",
                sum(invalid.values()),
                len(invalid),
            )
        for pattern in patterns:
            if pattern.subject_class in expressions:
                pattern.subject_label = expressions[pattern.subject_class]["manchester_labels"]
            if pattern.object_class in expressions:
                pattern.object_label = expressions[pattern.object_class]["manchester_labels"]
        context.report.finish_phase(phase, items=len(patterns))
        context.discovered_classes = sorted(
            {p.subject_class for p in patterns if not p.untyped_subject}
        )
        return patterns


def index_description(workdir: Path, name: str) -> dict[str, Any] | None:
    """Describe a QLever index by its metadata (build, triples), to reuse a store of it."""
    meta = Path(workdir) / f"{name}.meta-data.json"
    if not meta.is_file():
        return None
    data = json.loads(meta.read_text())
    return {
        "name": name,
        "git-hash": data.get("git-hash"),
        "triples": (data.get("num-triples") or {}).get("normal"),
        "modified": meta.stat().st_mtime_ns,
    }
