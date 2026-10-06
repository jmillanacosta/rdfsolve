"""Scan mining: QLever only reads its index; every count is made by Polars on the rows it reads.

The grouped SPARQL queries of the other strategies join and group inside the server, within
its memory limit (QLever does not spill to disk: Bgee's census asked for more than 455 GB).
Here the server is asked only for the rows of one predicate at a time,
``SELECT ?s ?o (DATATYPE(?o) AS ?d) WHERE { ?s <p> ?o }``, a scan of one range of a sorted
permutation that QLever streams without holding it, and for the members of the classes. The
rows are saved as Parquet (the row store); patterns, their counts and the rest are computed
from the store, one predicate at a time, so memory is bound by the largest predicate.

Each value is saved twice: as the term QLever writes in TSV, and as QLever's 64-bit id of the
value (the same query with ``Accept: application/octet-stream``). The ids count distinct
values exactly: the TSV writes doubles with 13 significant digits, so values that differ further
print the same (WikiPathways gpml:relY: 2,160 values, 2,021 printed).

The patterns follow the definitions of the count queries of the two-phase strategy
(query_builders: typed objects, untyped IRIs, blank nodes, literals), so that the two
strategies give the same schema of the same index.
"""

from __future__ import annotations

import contextlib
import json
import logging
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
from rdfsolve.schema_models._constants import UNTYPED_SUBJECT
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
        """The file of the rows of each predicate."""
        return {p: self.path / "rows" / f for p, f in self.manifest["predicates"].items()}

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
        that is neither an IRI nor a blank node (a literal: monarch-kg's ``?s rdf:type
        "strain"``, job 115614) names no class and is left out, so that a node typed only so is
        an untyped subject (literal_type_values reports them).
        """
        import polars as pl

        types = self._type_rows().filter(_class_term(pl.col("c")))
        # An index without any type (STRING's main graph): the empty table is held in memory,
        # since Polars 2.0 panics on a join with unique() of an empty Parquet scan ("min > max").
        if not types.select(pl.len()).collect().item():
            return pl.DataFrame(schema=types.collect_schema()).lazy()
        return types

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
        return self.type_ids().select("sid").unique()

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
        return self.type_ids().select("sid").unique()

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


def _bare(expr: pl.Expr) -> pl.Expr:
    """Strip the angle brackets of an IRI term."""
    return expr.str.strip_prefix("<").str.strip_suffix(">")


# Building a store


class QueryTimeoutError(RuntimeError):
    """The server stopped the query at its time limit (QLever answers 429 with "timed out")."""


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


def _post(endpoint: str, query: str, accept: str) -> Any:
    """Send *query* to a local endpoint; return the open response (no proxy, no time limit).

    The process has at most 2 * export_workers() queries open at once (_query_slots), fewer
    than the server's slots. A server whose query slots are all taken (queries of other
    clients, or ones it is still ending) answers 429; the query is sent again after a wait
    (1, 2, 4 … 60 s) for as long as this process's other queries go on ending, and up to
    BUSY_WAIT seconds after the last one ended. QLever also answers 429 to a query it stopped
    at its time limit, with "timed out" in the body: that raises QueryTimeoutError at once
    (the caller reads it in slices; sending it again would time out again).
    """
    import urllib.error

    from rdfsolve.sparql_terms import writable_query

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    # A term that is not an RDF IRI (Bio2RDF: <...statistic  n>) is written with IRI("...").
    data = urllib.parse.urlencode({"query": writable_query(query)}).encode()
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


def _tsv_to_parquet(endpoint: str, query: str, path: Path) -> int:
    """Stream the TSV result of *query* to *path* as Parquet; return its rows.

    QLever's TSV escapes tabs and newlines inside terms, so one line is one row.
    """
    import polars as pl

    tsv = path.with_suffix(".tsv")
    with _post(endpoint, query, "text/tab-separated-values") as response, tsv.open("wb") as out:
        tail = b""
        while chunk := response.read(1 << 24):
            _check_trailer(tail + chunk, query, lambda: response.read(4096))
            tail = chunk[-len(QLEVER_ERROR_TRAILER) :]
            out.write(chunk)
    frame = pl.scan_csv(tsv, separator="\t", quote_char=None, has_header=True, infer_schema=False)
    frame.rename(lambda c: c.lstrip("?")).sink_parquet(path)
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
        pl.DataFrame(
            {n: [] for n in [*names, *ids, *(extra or {})]},
            schema={**dict.fromkeys(names, pl.String), **dict.fromkeys(ids, pl.UInt64)},
        ).write_parquet(partial)
    else:
        writer.close()
    partial.replace(path)
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
# QLever streams one query from one thread. On GOA rdf:type (11.8 M rows) the last 1 M rows came
# as fast as the first, and 8 slices read it 6.4 times as fast as one query. Skipping rows is not
# free in every index (PubChem, GRAPH ?g: about 13 s for 4.3 G rows), so a query has at most
# MAX_SLICES slices, of SLICE_ROWS rows or more.
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
        partial.replace(path)
    for file in files:
        file.close()
    for part in parts:
        Path(part).unlink(missing_ok=True)


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

    def read(part: Path, offset: int | None, limit: int | None, size: int) -> int:
        """Write the whole result (*offset* None), or one slice of it, to *part*; return its
        rows. A read the server times out on is read again in two halves, joined in order.
        """
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
        except QueryTimeoutError:
            if size <= max(SPLIT_ROWS, 1):
                raise
        offset = offset or 0
        half = size // 2
        logger.info("Scan: the server timed out on %d rows; reading them in halves", size)
        halves = [part.with_name(part.name + ".a"), part.with_name(part.name + ".b")]
        count = read(halves[0], offset, half, half)
        count += read(
            halves[1], offset + half, None if limit is None else limit - half, size - half
        )
        _join_parts(halves, part)
        return count

    def stream(part: Path, offset: int | None = None, limit: int | None = None) -> int:
        """Write the whole result, or one slice of it, to *part*; return its rows."""
        if offset is None:
            return read(part, None, None, rows)
        if part.is_file():
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


def _count(value: str) -> int:
    """Read a count as QLever's TSV writes it (3, or "3"^^<...#int>)."""
    return int(value.strip('"').split('"')[0])


def _counts(endpoint: str, query: str, path: Path) -> list[tuple[tuple[str, ...], int]]:
    """Return the rows of a grouped count query: the grouping terms, and the count as an int."""
    import polars as pl

    _tsv_to_parquet(endpoint, query, path)
    rows = [(tuple(row[:-1]), _count(row[-1])) for row in pl.read_parquet(path).iter_rows()]
    path.unlink()
    return rows


def _graph_counts(
    endpoint: str, path: Path, sizes: dict[str, int], unnamed: dict[str, int]
) -> dict[str, dict[str, int]]:
    """Return the rows of each predicate in each named graph.

    One grouped query (GROUP BY ?g ?p) answers it, but on a large index QLever sorts every
    triple by graph and predicate (PubChem: timed out after ten minutes, where GROUP BY ?p takes
    a second). Then the graphs are listed; one graph that holds every triple (no triple outside
    the named graphs) has the counts of GROUP BY ?p (*sizes*); otherwise each predicate is
    counted by graph with the predicate bound.
    """
    import polars as pl

    graphs: dict[str, dict[str, int]] = {}
    try:
        for (g, p), n in _counts(
            endpoint,
            "SELECT ?g ?p (COUNT(?s) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ?p",
            path,
        ):
            graphs.setdefault(g[1:-1], {})[p[1:-1]] = n
        return graphs
    except QueryTimeoutError:
        logger.info("Scan: the count by graph and predicate timed out; counting by predicate")
    path.unlink(missing_ok=True)
    path.with_suffix(".tsv").unlink(missing_ok=True)
    _tsv_to_parquet(endpoint, "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }", path)
    names = [g[1:-1] for g in pl.read_parquet(path)["g"].to_list()]
    path.unlink()
    if len(names) == 1 and not unnamed:
        return {names[0]: dict(sizes)}
    for predicate, size in sizes.items():
        for g, n in _predicate_graph_counts(endpoint, path, predicate, 0, None, size).items():
            graphs.setdefault(g[1:-1], {})[predicate] = n
    return graphs


# A read that the server stops at its time limit is read again in two halves (LIMIT, OFFSET),
# down to slices of SPLIT_ROWS rows.
SPLIT_ROWS = 100_000


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


def _has_named_graphs(endpoint: str, path: Path) -> bool:
    """Whether the index holds a named graph (QLever's GRAPH ?g returns only named graphs)."""
    import polars as pl

    probe = path / "graphs.parquet"
    _tsv_to_parquet(endpoint, "SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 1", probe)
    found = pl.read_parquet(probe).height > 0
    probe.unlink()
    return found


def export_index(
    endpoint: str, path: Path, *, index: dict[str, Any] | None = None, workers: int | None = None
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
    ``FROM <QLEVER_DEFAULT_GRAPH>`` and given that graph.
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
    plan_file.write_text(json.dumps(plan) + "\n")
    done: dict[str, tuple[str, int]] = {}
    if resumable and progress.is_file():
        for line in progress.read_text().splitlines():
            entry = json.loads(line)
            if (path / "rows" / entry["file"]).is_file():
                done[entry["predicate"]] = (entry["file"], entry["rows"])
        logger.info("Scan: resuming %s with %d predicates read", path, len(done))
    t0 = time.monotonic()
    counted = path / "predicates.parquet"
    sizes = {
        p[1:-1]: n
        for (p,), n in _counts(
            endpoint, "SELECT ?p (COUNT(?s) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p", counted
        )
    }
    named = _has_named_graphs(endpoint, path)
    graphs: dict[str, dict[str, int]] | None = None
    unnamed: dict[str, int] = {}
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
        graphs = _graph_counts(endpoint, counted, sizes, unnamed)
        if unnamed:
            graphs[QLEVER_DEFAULT_GRAPH] = unnamed
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
        if len(membership) == 1 and len(types) == 1:
            (path / "types-0.parquet").rename(path / "types.parquet")
        elif len(membership) == 1:
            pl.concat(types).sink_parquet(path / "types.parquet")
        else:
            pl.concat(types).unique().sink_parquet(path / "types.parquet")
        for part in path.glob("types-*.parquet"):
            part.unlink()
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
        if not named:
            rows = _read_query(
                streams,
                endpoint,
                _row_query(predicate),
                file,
                {"sid": 0, "oid": 1},
                size,
                id_query=_row_query(predicate, datatype=False),
                keep_text=keep,
            )
        else:
            rows = _read_query(
                streams,
                endpoint,
                _graph_row_query(predicate),
                file,
                {"gid": 0, "sid": 1, "oid": 2},
                size - unnamed.get(predicate, 0),
                id_query=_graph_row_query(predicate, datatype=False),
                keep_text=keep,
            )
            if unnamed.get(predicate):
                rest = file.with_name(file.stem + "-unnamed.parquet")
                rows += _read_query(
                    streams,
                    endpoint,
                    _unnamed_row_query(predicate),
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
                file.with_name(file.name + ".all").replace(file)
                rest.unlink()
        with lock, progress.open("a") as log:
            log.write(json.dumps({"predicate": predicate, "file": file.name, "rows": rows}) + "\n")
        return predicate, file.name, (predicate, size, rows) if rows != size else None

    gaps: dict[str, str] = {}
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
    }
    manifest.write_text(json.dumps(record, indent=1) + "\n")
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
    # The type table is read lazily: on a large index (Bgee: 725 M rows) its text does not fit.
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
# part are held at once, with about 300 bytes a row at the peak of counting.
PARTITION_ROWS = 50_000_000
# A type table with more rows than this is joined in parts (by id), each about 40 bytes a row.
TYPE_PARTITION_ROWS = 200_000_000
COUNTS = ("count", "distinct_subjects", "distinct_objects")


def count_limits() -> tuple[int, int]:
    """Return the rows of a predicate and of the type table counted at once (PARTITION_ROWS,
    TYPE_PARTITION_ROWS), or both from RDFSOLVE_SCAN_COUNT_GB: the memory counting may use,
    in GB, which a part of rows and a part of the type table share.
    """
    import os

    value = os.environ.get("RDFSOLVE_SCAN_COUNT_GB")
    if not value:
        return PARTITION_ROWS, TYPE_PARTITION_ROWS
    budget = float(value) * 1e9
    return max(1, int(budget / 2 / 300)), max(1, int(budget / 2 / 40))


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
        if rows + sizes[predicate] > BATCH_ROWS:
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

    return frame.select(
        "sid",
        "oid",
        "kind",
        "d",
        *(["g"] if by_graph else []),
        sb=pl.col("s").str.starts_with("_:"),
        p=pl.lit(predicate),
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


def count_patterns(store: CountedStore) -> list[SchemaPattern]:
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
    """
    import tempfile

    import polars as pl

    scope = getattr(store, "graph_uris", None)
    by_graph = bool(scope)
    keys = ["p", *(["g"] if scope else []), "subject_class", "object_class", "datatype"]
    tables: list[pl.DataFrame] = []
    blank_objects: list[pl.DataFrame] = []
    blank_outgoing: list[pl.DataFrame] = []
    partition_rows, type_rows = count_limits()
    typed_ids = store.typed_ids() if hasattr(store, "typed_ids") else store.type_ids().select("sid")
    typed_ids = typed_ids.select("sid").unique()
    # The type rows of the whole index bound those of the scope (a view or a retyped table).
    type_count = int(store.base.graph_types().select(pl.len()).collect().item())
    type_parts = max(1, -(-type_count // type_rows))
    scratch: list[Path] = []

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
        blank_outgoing.append(rows.filter(pl.col("sb")).select("sid", "p").unique())
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
                    typed.with_columns(pl.col("subject_class").cast(pl.String)),
                    untyped_subjects.with_columns(pl.col("subject_class").cast(pl.String)),
                ],
                how="vertical_relaxed",
            )
        literal = typed.filter(pl.col("kind") == "literal").with_columns(
            object_class=pl.lit("Literal"), datatype=pl.col("d")
        )
        nonliteral = typed.filter(pl.col("kind") != "literal").with_columns(
            datatype=pl.lit(None, pl.String)
        )
        with_types = nonliteral.join(objects, on="oid", how="left")
        typed_object = with_types.filter(pl.col("oc").is_not_null()).with_columns(
            object_class=pl.col("oc").cast(pl.String)
        )
        untyped = with_types.filter(
            pl.col("oc").is_null() & (pl.col("kind") == "iri")
        ).with_columns(object_class=pl.lit("Resource"))
        blank = nonliteral.filter(pl.col("kind") == "bnode")
        blank_objects.append(
            blank.select(pl.col("subject_class").cast(pl.String), "p", "oid").unique()
        )
        blank = blank.with_columns(object_class=pl.lit("BlankNode"))
        return [
            f.with_columns(pl.col("subject_class").cast(pl.String)).select(*keys, "sid", "oid")
            for f in (literal, typed_object, untyped, blank)
        ]

    # Joins use QLever's ids (an IRI or blank node has one id as subject and as object), and
    # the classes as categories: the type table of a large index fits in memory this way.
    if type_parts == 1:
        types = store.type_ids().with_columns(pl.col("c").cast(pl.Categorical)).collect()
        typed_all = typed_ids.collect()

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
            row_parts = -(-size // partition_rows)
            if row_parts <= 1 and type_parts == 1:
                rows = _batch_rows(store, batch, by_graph=by_graph)
                cells = classify(
                    rows, types.rename({"c": "subject_class"}), typed_all, renamed(types)
                )
                tables.extend(_part_counts(cells, keys))
                continue
            # Parts by subject id; the parts of the type table divide them (parts is a multiple
            # of type_parts), so the subjects of a part are in one part of the types.
            parts = type_parts * -(-row_parts // type_parts)
            logger.info(
                "Scan count: %d rows of %d predicates in %d parts (types in %d)",
                size,
                len(batch),
                parts,
                type_parts,
            )
            frame = _batch_frame(store, batch, by_graph=by_graph)
            schema = frame.collect_schema()
            folder = directory() / f"unit-{unit}"
            _spill(frame, folder / "rows", parts, "sid")
            sums, subject_counts = [], []
            for part in range(parts):
                rows = _part(folder / "rows", part, schema)
                selected = pl.col("sid") % parts == part
                subjects = type_part(part % type_parts).filter(selected)
                typed_subjects = typed_part(part % type_parts).filter(selected)
                pairs = []
                for object_part in range(type_parts):
                    cell = rows.filter(pl.col("oid") % type_parts == object_part)
                    objects = renamed(type_part(object_part))
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
            shutil.rmtree(folder)
    finally:
        if scratch:
            shutil.rmtree(scratch[0], ignore_errors=True)
    if not tables:
        return []
    membership = list(MEMBERSHIP.get())
    table = pl.concat(
        [t.select(*keys, *(pl.col(c).cast(pl.Int64) for c in COUNTS)) for t in tables]
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
    blank_typed = (
        store.data_types()
        .filter(pl.col("s").str.starts_with("_:"))
        .select("s")
        .join(
            store.base.graph_types()
            .filter(pl.col("s").str.starts_with("_:"))
            .select("s", "sid")
            .unique(),
            on="s",
        )
        .select("sid", p=pl.lit(RDF_TYPE))
        .unique()
        .collect()
    )
    outgoing = pl.concat([*blank_outgoing, blank_typed]).rename({"sid": "oid", "p": "q"})
    fields = (
        pl.concat(blank_objects)
        .join(outgoing, on="oid", how="inner")
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
    ) -> None:
        """Keep where the rows go, or the store to read."""
        self.store_dir = store_dir
        self.index = index
        self.row_store = store
        self.store: RowStore | StoreView | None = store

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
                context.helper.endpoint_url, self.store_dir, index=self.index
            )
            context.report.finish_phase(phase, items=len(self.row_store.predicates))
            context.report.report.config["scan_store"] = {
                "path": str(self.row_store.path),
                "triples": self.row_store.manifest.get("triples"),
                "predicates": len(self.row_store.predicates),
                "graphs": len(self.row_store.graphs or {}),
                "seconds": self.row_store.manifest.get("seconds"),
                "gaps": self.row_store.manifest.get("gaps") or {},
            }
        self.store = self.row_store.view(context.graph_uris, context.type_context_graph_uris)
        phase = context.report.start_phase("scan-patterns")
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
        patterns = count_patterns(self.store)
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
