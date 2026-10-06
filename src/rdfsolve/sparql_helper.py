"""SPARQL query execution with automatic GET/POST fallback and retry logic."""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import secrets
import socket
import threading
import time
import warnings
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timezone
from itertools import count
from pathlib import Path
from typing import Any, ClassVar, Literal, NoReturn, TypedDict
from urllib.parse import urlsplit

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=Warning, module="requests")
    import requests
    from requests.adapters import HTTPAdapter
from typing import Self

from rdflib import Graph, URIRef, Variable
from rdflib import Literal as RdfLiteral
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool

from rdfsolve.query_collection import QueryCollection, QueryRun, SavedQuery
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.sparql_terms import writable_query

logger = logging.getLogger(__name__)
# Seconds between the log lines of a query that is still running.
HEARTBEAT_S = 300.0
# Seconds that a request may run past its timeout before it is ended (SparqlHelper.deadline).
DEADLINE_GRACE_S = 30.0


class SelectExecution(TypedDict, total=False):
    """How the last SELECT was executed, as reported to callers."""

    strategy: str
    status: str
    pagination: str
    pages: int
    rows: int
    chunk_size: int
    max_pages: int | None
    elapsed_seconds: float
    completeness_basis: str
    error: str
    first_error: str
    offset_error: str


@dataclass
class QueryRecord:
    """Record of a SPARQL query execution."""

    query: str
    query_type: Literal["SELECT", "CONSTRUCT", "ASK"]
    endpoint_url: str
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    description: str = ""
    keywords: list[str] = field(default_factory=list)
    success: bool = True
    purpose: str = ""
    error: str | None = None
    error_message: str | None = None
    status_code: int | None = None
    response_excerpt: str | None = None
    attempts: int = 0
    fallback_used: bool = False
    result: Any = None
    result_retained: bool = False
    elapsed_seconds: float = 0.0
    wait_seconds: float = 0.0
    request_seconds: float = 0.0
    # How the last request was cut by a limit on the way to the server (see QueryCut).
    cut: QueryCut | None = None

    def query_id(self) -> str:
        """Generate a unique ID for this query based on content hash."""
        content = f"{self.query_type}:{self.query}"
        return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()[:12]


_active_record: ContextVar[QueryRecord | None] = ContextVar("sparql_query_record", default=None)


def _default_agent() -> str:
    """Name the software and its version; people add contact details through settings."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return f"rdfsolve/{version('rdfsolve')}"
    except PackageNotFoundError:
        return "rdfsolve"


class _Deadline:
    """End one request at a wall-clock deadline, whatever the server sends.

    A read timeout bounds one silence only: a server or proxy that sends a few bytes now and
    then holds the read for ever (rehearsal 114981: two remote queries, one on QLever and one
    at IDSM, ran for 16.8 h). At the deadline a watcher shuts the socket of the request down,
    which ends a blocked read in the requesting thread; the request then raises a timeout
    (SparqlHelper._request_serial). The connection pools tell the watcher which connection the
    request uses (_WatchedHTTPConnectionPool).
    """

    def __init__(self, seconds: float) -> None:
        """Start the deadline *seconds* from now."""
        self.seconds = seconds
        self.expired = False
        self.connection: Any = None
        self._done = threading.Event()
        self._token: Any = None

    def __enter__(self) -> Self:
        self._token = _active_deadline.set(self)
        threading.Thread(target=self._watch, daemon=True).start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._done.set()
        _active_deadline.reset(self._token)

    def _watch(self) -> None:
        if self._done.wait(self.seconds):
            return
        self.expired = True
        # Repeated until the request ends: at the deadline its socket may still be connecting.
        while True:
            sock = getattr(self.connection, "watched_sock", None)
            if sock is not None:
                with suppress(OSError):
                    # The shutdown of the plain socket, also under TLS: SSLSocket.shutdown
                    # would change the TLS state that the reading thread uses.
                    socket.socket.shutdown(sock, socket.SHUT_RDWR)
            if self._done.wait(1.0):
                return


_active_deadline: ContextVar[_Deadline | None] = ContextVar("sparql_deadline", default=None)


class _WatchedHTTPConnection(HTTPConnection):
    """A connection that keeps its socket for the deadline: http.client drops ``sock`` when a
    response without a length is read to the closing of the connection.
    """

    watched_sock: socket.socket | None = None

    def connect(self) -> None:
        """Open the connection and keep its socket for the request's deadline."""
        super().connect()
        self.watched_sock = self.sock


class _WatchedHTTPSConnection(HTTPSConnection):
    """A TLS connection that keeps its socket for the deadline (_WatchedHTTPConnection)."""

    watched_sock: socket.socket | None = None

    def connect(self) -> None:
        """Open the TLS connection and keep its socket for the request's deadline."""
        super().connect()
        self.watched_sock = self.sock


class _WatchedHTTPConnectionPool(HTTPConnectionPool):
    """A pool that gives the connection of each request to the request's deadline."""

    ConnectionCls = _WatchedHTTPConnection

    def _get_conn(self, timeout: float | None = None) -> Any:
        conn = super()._get_conn(timeout)
        deadline = _active_deadline.get()
        if deadline is not None:
            deadline.connection = conn
        return conn


class _WatchedHTTPSConnectionPool(HTTPSConnectionPool):
    """A pool that gives the connection of each request to the request's deadline."""

    ConnectionCls = _WatchedHTTPSConnection

    def _get_conn(self, timeout: float | None = None) -> Any:
        conn = super()._get_conn(timeout)
        deadline = _active_deadline.get()
        if deadline is not None:
            deadline.connection = conn
        return conn


_WATCHED_POOLS: dict[str, type[HTTPConnectionPool]] = {
    "http": _WatchedHTTPConnectionPool,
    "https": _WatchedHTTPSConnectionPool,
}


class _KeepaliveAdapter(HTTPAdapter):
    """An HTTP adapter whose connections send TCP keepalive probes and can be ended at a deadline.

    A long query can be silent for minutes, and a live server answers the probes during that
    time. When the peer is gone without a reset (for example after a network change), the
    probes fail and the read ends in about two minutes. Without them, the read waits for ever.
    A live peer that keeps the connection open is ended by the deadline of the request
    (_Deadline), also behind an HTTP proxy.
    """

    SOCKET_OPTIONS: ClassVar[list[tuple[int, int, int]]] = [
        *HTTPConnection.default_socket_options,
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        *(
            (socket.IPPROTO_TCP, getattr(socket, name), value)
            for name, value in (
                ("TCP_KEEPIDLE", 60),
                ("TCP_KEEPALIVE", 60),
                ("TCP_KEEPINTVL", 15),
                ("TCP_KEEPCNT", 4),
            )
            if hasattr(socket, name)
        ),
    ]

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        """Give the socket options to every connection pool."""
        kwargs["socket_options"] = self.SOCKET_OPTIONS
        super().init_poolmanager(*args, **kwargs)
        self.poolmanager.pool_classes_by_scheme = _WATCHED_POOLS

    def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
        """Watch the connections through an HTTP proxy too; a SOCKS proxy keeps its own pools."""
        manager = super().proxy_manager_for(proxy, **proxy_kwargs)
        if not proxy.lower().startswith("socks"):
            manager.pool_classes_by_scheme = _WATCHED_POOLS
        return manager


@dataclass(frozen=True)
class QueryCut:
    """A request that a limit on the way to the data ended: the server, a proxy or a gateway.

    *kind* is how it ended ("HTTP 524", "connection closed without a response"); *seconds* is
    how long the request ran. A fixed limit ends every query that exceeds it at the same time:
    the rehearsal of 2026-10-05 saw Bgee's gateway answer HTTP 524 after 127.0 to 128.2 s, and
    the cluster's proxy close UniProt's connections after 899.9 to 903.0 s. Two cuts are at
    the same limit within 2 s or 2 %, which holds both spreads with a margin.
    """

    kind: str
    seconds: float

    def same_limit(self, other: QueryCut) -> bool:
        """Return whether two cuts are of the same kind, at the same time."""
        window = max(2.0, 0.02 * max(self.seconds, other.seconds))
        return self.kind == other.kind and abs(self.seconds - other.seconds) <= window


# Consecutive cuts at the same limit after which a step stops sending queries of a purpose.
# One cut is one slow query, and two can be two neighbouring queries of one heavy class (steps
# send their queries class by class). Three in a row, with no answer between, show the limit:
# Bgee's path testing (rehearsal 2026-10-05) answered one query, then had 13 in a row cut at
# 128 s until its budget ended; no step that sends its queries unchanged answered after three
# such cuts. Timeouts of the client's own limit do not count (QueryCut): there, runs of three
# timeouts followed by an answer were seen (VoID gap queries, 60 s).
CUTS_BEFORE_STOP = 3

# Consecutive timeouts at the client's own limit after which a light step (QueryCuts with
# client_timeouts) stops a purpose. Runs of three such timeouts followed by an answer were seen
# (VoID gap queries at 60 s, rehearsal 2026-10-05), so the stop waits for five: a purpose whose
# five queries in a row each ran past the limit has its remaining queries recorded as not sent
# (IDSM, rehearsal 2026-10-06: drift re-counts of a 347 M-triple partition each cost 62 s).
TIMEOUTS_BEFORE_STOP = 5
CLIENT_TIME_LIMIT = "client time limit"


@dataclass
class _PurposeCuts:
    """The cuts of the queries of one purpose in a step."""

    last: QueryCut | None = None
    run: int = 0
    cut: int = 0
    not_sent: int = 0
    stopped: str | None = None


class QueryCuts:
    """Stop a step from sending queries that the endpoint cuts at a fixed limit.

    A step reports each answer (answered) and each failure (failed) of a purpose; after
    *after* consecutive cuts at the same limit, the purpose is stopped, and the step asks
    (skip) before each query, which counts the queries not sent. An answer resets the count.
    A failure that is not a cut (a refused query) neither counts nor resets. record() says,
    for each stopped purpose, why, how many queries were cut and how many were not sent.
    """

    def __init__(
        self,
        after: int = CUTS_BEFORE_STOP,
        *,
        client_timeouts: bool = False,
        timeouts_after: int = TIMEOUTS_BEFORE_STOP,
    ) -> None:
        """Stop after *after* consecutive cuts at the same limit.

        With *client_timeouts* (light steps, whose queries all run under one short limit), a
        query that ran past the client's own limit counts too: *timeouts_after* of them in a
        row stop the purpose.
        """
        if after < 1 or timeouts_after < 1:
            raise ValueError("Stop after at least one cut")
        self.after = after
        self.client_timeouts = client_timeouts
        self.timeouts_after = timeouts_after
        self._purposes: dict[str, _PurposeCuts] = {}

    def _of(self, purpose: str) -> _PurposeCuts:
        return self._purposes.setdefault(purpose, _PurposeCuts())

    def stopped(self, purpose: str | None = None) -> str | None:
        """Return why a purpose (by default: any purpose of the step) was stopped."""
        if purpose is not None:
            return self._of(purpose).stopped
        return next((c.stopped for c in self._purposes.values() if c.stopped), None)

    def skip(self, purpose: str, *, whole_step: bool = False) -> bool:
        """Return whether not to send a query of *purpose*; count it as not sent if so.

        With *whole_step*, a stop of any purpose of the step stops this one too.
        """
        if not self.stopped(None if whole_step else purpose):
            return False
        self._of(purpose).not_sent += 1
        return True

    def answered(self, purpose: str) -> None:
        """Record an answer: the queries of this purpose are not all cut."""
        state = self._of(purpose)
        state.last, state.run = None, 0

    def failed(self, purpose: str, error: BaseException) -> str | None:
        """Record a failed query; return why the purpose is stopped, once it is."""
        cut: QueryCut | None = getattr(error, "cut", None)
        if cut is None and self.client_timeouts and isinstance(error, EndpointTimeoutError):
            cut = QueryCut(CLIENT_TIME_LIMIT, 0.0)
        state = self._of(purpose)
        if cut is None or state.stopped:
            return state.stopped
        state.cut += 1
        same = state.last is not None and state.last.same_limit(cut)
        state.run = state.run + 1 if same else 1
        state.last = cut
        client = cut.kind == CLIENT_TIME_LIMIT
        if state.run >= (self.timeouts_after if client else self.after):
            state.stopped = (
                f"{state.run} queries in a row ran past the step's time limit"
                if client
                else f"endpoint cuts queries at {cut.seconds:.0f} s"
            )
            logger.warning(
                "%s: %s (%s) - no more queries of it are sent", purpose, state.stopped, cut.kind
            )
        return state.stopped

    def record(self) -> dict[str, dict[str, Any]]:
        """Return, for each stopped purpose, why it stopped and what was cut and not sent."""
        step = self.stopped()
        return {
            purpose: {
                "stopped": state.stopped or step,
                "cut_by": state.last.kind if state.last else None,
                "queries_cut": state.cut,
                "queries_not_sent": state.not_sent,
            }
            for purpose, state in sorted(self._purposes.items())
            if state.stopped or state.not_sent
        }


class SparqlHelperError(Exception):
    """Base exception for SPARQL helper errors."""

    # How the endpoint, or a proxy or gateway before it, cut the query (QueryCuts).
    cut: QueryCut | None = None
    # Whether a gateway answered for the server (HTTP 502, 503, 504, 522 or 524) without
    # saying that it could not reach it (PROXY_CONNECT_FAILURE_PATTERNS) and without a cost
    # limit of the engine: an overloaded host, worth waiting for when one query decides a
    # whole step (graph discovery). A busy answer already waited out is not one.
    gateway_overload: bool = False


class EndpointError(SparqlHelperError):
    """Raised when the endpoint returns an error."""


class EndpointTimeoutError(EndpointError):
    """Raised when the endpoint times out (read / connect)."""

    def __init__(self, msg: str, status_code: int | None = None) -> None:
        """Initialize with error message and optional HTTP status code."""
        super().__init__(msg)
        self.status_code = status_code


class ResponseLimitError(EndpointTimeoutError):
    """Response exceeded the byte budget; use a smaller query."""


class EndpointRateLimitError(EndpointError):
    """Remote host rate limit; do not split the query into more requests."""


class EndpointUnhealthyError(EndpointError):
    """Raised when the endpoint returns a 200/400 with a non-SPARQL body.

    Typical examples: database in recovery mode, backend proxy errors,
    maintenance pages returned as ``text/plain`` or ``text/html``.
    """


class PaginationTruncatedError(EndpointTimeoutError):
    """Raised by select_chunked when pagination is abandoned mid-stream.

    This means some rows were already yielded before the error, so the
    caller received a partial result set.  The ``offset`` attribute
    records where pagination stopped.
    """

    def __init__(
        self, msg: str, offset: int = 0, partial_rows: list[dict[str, Any]] | None = None
    ) -> None:
        """Initialize with error message and offset where pagination stopped."""
        super().__init__(msg)
        self.offset = offset
        self.partial_rows = partial_rows if partial_rows is not None else []


class QueryError(SparqlHelperError):
    """Raised when the query itself is invalid."""


# MIME types for SPARQL responses
class MimeTypes:
    """Standard MIME types for SPARQL protocol."""

    # SELECT/ASK results
    JSON = "application/sparql-results+json"
    XML = "application/sparql-results+xml"

    # CONSTRUCT/DESCRIBE results (RDF formats)
    TURTLE = "text/turtle"
    N3 = "text/n3"
    NTRIPLES = "application/n-triples"
    RDFXML = "application/rdf+xml"
    JSONLD = "application/ld+json"

    # Accept headers for different query types
    SELECT_ACCEPT = f"{JSON}, {XML};q=0.9"
    CONSTRUCT_ACCEPT = f"{TURTLE}, {N3};q=0.9, {NTRIPLES};q=0.8, {RDFXML};q=0.7"


class SparqlHelper:
    """
    Centralized SPARQL query executor with automatic fallback and retry logic.

    This class provides:
    - Automatic GET/POST method fallback when endpoints return HTML/500 errors
    - Configurable retry with exponential backoff for transient failures
    - Consistent error handling and logging
    - Support for SELECT, CONSTRUCT, and ASK queries

    Uses standard `requests` library.

    Attributes:
        endpoint_url: The SPARQL endpoint URL
        use_post: If True, always use POST method (skip GET attempt)
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff delay in seconds
        max_backoff: Maximum backoff delay in seconds
        timeout: Connection and host-slot wait timeout in seconds
        read_timeout: Longest silence while a response is read; None (the default) lets a long
            query run. TCP keepalive probes find a dead connection in about two minutes.
        deadline: Longest wall-clock time of one request, from sending it to the end of its
            response, however the server sends it; past it the request raises
            EndpointTimeoutError. None (the default) is timeout plus DEADLINE_GRACE_S.
        rate_limit_wait: Longest wait for a cooldown that the server asks for (a 429, 502, 503
            or 504 with Retry-After). A longer cooldown raises EndpointRateLimitError.

    Example:
        >>> helper = SparqlHelper("https://sparql.swisslipids.org/")
        >>> results = helper.select("SELECT ?g { GRAPH ?g { ?s ?p ?o } }")
        >>> for binding in results["results"]["bindings"]:
        ...     print(binding["g"]["value"])
    """

    # Error patterns that indicate POST should be tried
    POST_RETRY_PATTERNS = ("html", "500", "internal", "error", "method not allowed")

    # HTML markers that indicate an error response instead of RDF
    HTML_MARKERS = ("<!DOCTYPE", "<html", "<HTML", "<!doctype")

    # HTTP status codes that warrant a retry
    RETRY_STATUS_CODES = (500, 502, 503, 504, 429)

    # rejection from the endpoint (not a transient server error) raise EndpointTimeoutError.
    # A connection closed without a response after this many seconds means that the server or
    # a proxy gave up on the query: the caller must make it smaller, not repeat it.
    DROPPED_AFTER_SECONDS: ClassVar[float] = 60.0
    DROPPED_PATTERNS: ClassVar[tuple[str, ...]] = (
        "remotedisconnected",
        "remote end closed connection without response",
    )
    COST_LIMIT_PATTERNS: ClassVar[tuple[str, ...]] = (
        "estimated execution time",
        "exceeds the limit",
        "query timed out",
        "operation timed out",
        "timeout expired",
        "execution time limit",
        "statement timeout",
        "cost limit exceeded",
        # Virtuoso: "Query did not complete due to ANYTIME timeout" (S1TAT).
        "anytime timeout",
        # Virtuoso refusals that repeat for the same query, so the caller makes it smaller:
        # "S1T00 Error SR171: Transaction timed out" (IDEAL: 3 tries of 63 s each before the
        # batched fallback answered in 3 s) and "SR319 Max row length is exceeded" (GlyTouCan;
        # a GROUP_CONCAT over too many values), rehearsal 2026-10-06.
        "transaction timed out",
        "sr171",
        "sr319",
        "max row length is exceeded",
        "sorted top clause",
        # Virtuoso refuses these for the query, not for the moment: "42000 Error D1CTX: Hash
        # dictionary is full, exceeded 2000000 entries" (a CONSTRUCT of RIKEN BRC's VoID graph:
        # 3 tries for each of 2 fallbacks, job 115326) and "SQ200 Stack Overflow in cost model"
        # (SIBiLS). The caller's fallback runs at once.
        "d1ctx",
        "hash dictionary is full",
        "sq200",
        "stack overflow in cost model",
        # QLever-specific: query exhausted memory or thread resources
        "waited for a result from another thread which then failed",
        "memory limit exceeded",
        "tried to allocate",
    )

    # A 503 (or a remote 429) whose body says that the server is busy, not that the query is
    # too costly: SwissLipids answers "Too many concurrent queries. Please try again later."
    # (rehearsal 2026-10-06, job 115300). A 502 or 504 from a proxy or gateway that says so, or
    # sends Retry-After, is the same. The request is repeated after OVERLOAD_BACKOFF_S,
    # doubled at each try, or after the Retry-After that the server sends, up to
    # OVERLOAD_RETRIES times, whatever the retries of the step: one busy moment must not lose a
    # source. A 503 that does not say so (a proxy that cannot resolve the host) is not waited for.
    OVERLOAD_PATTERNS: ClassVar[tuple[str, ...]] = (
        "too many concurrent",
        "too many queries",
        "too many requests",
        "try again later",
        "server is busy",
        "overloaded",
    )
    # Row counts at which an engine cuts an unpaged result without saying so: Virtuoso's
    # ResultSetMaxRows (FANAVI answers 1000 of its 33,358 classes, even under LIMIT 5000, with
    # HTTP 200 and no header; rehearsal 2026-10-06), and the defaults of other engines. An
    # unpaged listing with exactly this many rows is read again in pages of that size.
    SUSPECTED_ROW_CAPS: ClassVar[frozenset[int]] = frozenset(
        {1000, 2000, 5000, 10000, 50000, 100000, 1000000}
    )

    @classmethod
    def row_cap_suspected(cls, rows: int) -> bool:
        """Return whether an unpaged result of *rows* rows may have been cut at a server cap."""
        return rows in cls.SUSPECTED_ROW_CAPS

    OVERLOAD_BACKOFF_S: ClassVar[float] = 30.0
    OVERLOAD_RETRIES: ClassVar[int] = 4

    # Statuses with which a proxy or gateway answers for the server. A busy one (Retry-After or
    # OVERLOAD_PATTERNS) is waited out; any one is marked gateway_overload unless its body says
    # that the proxy could not reach the server at all (bio2rdf's squid: "The requested URL
    # could not be retrieved", ERR_DNS_FAIL), which no wait mends.
    GATEWAY_ERROR_STATUS: ClassVar[tuple[int, ...]] = (502, 503, 504, 522, 524)
    PROXY_CONNECT_FAILURE_PATTERNS: ClassVar[tuple[str, ...]] = (
        "could not be retrieved",
        "err_dns_fail",
        "err_connect_fail",
        "unable to determine ip address",
        "could not resolve",
        "name or service not known",
        "name resolution",
        "connection refused",
        "no route to host",
        "origin dns error",
    )

    # HTTP statuses with which a gateway says that it stopped waiting for the server.
    GATEWAY_TIMEOUT_STATUS: ClassVar[tuple[int, ...]] = (504, 522, 524)

    def enable_query_collection(
        self, *, clear: bool = True, include_results: bool | None = None
    ) -> None:
        """Collect query executions, including failures. Optionally keep prior records."""
        self._collect_queries = True
        if include_results is not None:
            self._collect_results = include_results
        if clear:
            self._query_registry.clear()

    def get_collected_queries(self) -> list[QueryRecord]:
        """Return execution records, not proof of complete results."""
        return self._query_registry.copy()

    def _record_query(self, record: QueryRecord) -> None:
        """Collect a query execution; it becomes a portable example when queries is read."""
        if self._collect_queries:
            self._query_registry.append(record)
            self._unsaved.append(record)

    @property
    def queries(self) -> QueryCollection:
        """Return the named queries, with every recorded execution as a portable example.

        Executions are saved here, not when they run: saving parses the query, and a query
        with a large VALUES block takes seconds to parse.
        """
        while self._unsaved:
            record = self._unsaved.pop(0)
            name = record.query_id()
            if not any(
                q.name == name or (not q.prefixes and q.text == record.query)
                for q in self._queries.shacl.queries
            ):
                try:
                    self._queries.add(name, record.query, endpoint=record.endpoint_url)
                except Exception as export_error:
                    logger.warning(
                        "Cannot save query %s as a portable example: %s", name, export_error
                    )
        return self._queries

    def add_query(self, name: str, query: str, *, description: str = "") -> SavedQuery:
        """Save a named read query without executing it."""
        return self.queries.add(name, query, description=description, endpoint=self.endpoint_url)

    def load_shacl(self, source: str | Path | Graph) -> list[str]:
        """Load local query examples and paths. Do not execute or fetch imports."""
        return self.queries.load_shacl(source)

    def run_query(self, name: str) -> dict[str, Any] | bool | Graph:
        """Run a saved standalone query on this helper's endpoint.

        Review imported queries before running them, including SERVICE clauses.
        SHACL validation queries require bindings and are not run here.
        """
        saved = self.queries.queries[name]
        if saved.requires_context:
            raise ValueError("This query requires SHACL validation context")
        started_at = datetime.now(UTC).isoformat()
        started = time.monotonic()
        error_text = ""
        success = False
        try:
            if saved.query_type == "SELECT":
                result: dict[str, Any] | bool | Graph = self.select(saved.query)
            elif saved.query_type == "ASK":
                result = self.ask(saved.query)
            else:
                result = self.construct_graph(saved.query)
            success = True
            return result
        except Exception as error:
            error_text = str(error)
            raise
        finally:
            self.history.append(
                QueryRun(
                    name,
                    self.endpoint_url,
                    started_at,
                    time.monotonic() - started,
                    success,
                    error_text,
                )
            )

    def export_queries_as_ttl(self, output_file: str | Path | None = None) -> str:
        """Export saved queries and retained SHACL RDF, not execution results."""
        return self.queries.to_turtle(output_file)

    def __init__(
        self,
        endpoint_url: str,
        *,
        use_post: bool = False,
        max_retries: int = 3,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
        timeout: float = 30.0,
        sparql_engine: str = "",
        sparql_strategy: str = "",
        inter_request_delay: float = 0.0,
        select_page_size: int = 100,
        select_page_retries: int = 8,
        select_page_cooldown: float = 5.0,
        max_response_bytes: int = 64 * 1024 * 1024,
        user_agent: str | None = None,
        read_timeout: float | None = None,
        rate_limit_wait: float = 600.0,
        deadline: float | None = None,
    ) -> None:
        """Initialize SPARQL helper with retry logic and optional strategy hints.

        user_agent identifies the client to endpoints; some, such as Wikidata, ask for
        contact information in it. Defaults to $RDFSOLVE_USER_AGENT, else rdfsolve/<version>.
        """
        if max_response_bytes < 1 or max_retries < 1 or inter_request_delay < 0:
            raise ValueError("Use positive response/retry limits and nonnegative request delay")
        self._queries = QueryCollection()
        self._unsaved: list[QueryRecord] = []
        self.history: list[QueryRun] = []
        self._collect_results = False
        self._query_registry: list[QueryRecord] = []
        self._collect_queries = False
        self.max_response_bytes = max_response_bytes
        self._last_error_body = ""
        self._last_retry_after: float | None = None
        self.endpoint_url = endpoint_url.rstrip("/")
        # The endpoint URL given, when the endpoint redirected to another (_follow_redirect).
        self.redirected_from: str | None = None
        self.user_agent = user_agent or os.environ.get("RDFSOLVE_USER_AGENT") or _default_agent()
        self.use_post = use_post
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.timeout = timeout
        self.read_timeout = read_timeout
        self.deadline = deadline
        # A timed-out SELECT is retried in adaptive pages, unless a budget turns that off.
        self.page_recovery = True
        self.rate_limit_wait = rate_limit_wait
        self.sparql_engine = sparql_engine
        self.sparql_strategy = sparql_strategy
        # Graphs left out of the default and named graphs of a query without a dataset clause
        # (graph_exclusion_prologue); set only after the endpoint accepted the exclusion.
        self.excluded_graphs: list[str] = []
        self.inter_request_delay = inter_request_delay
        if (
            type(select_page_size) is not int
            or select_page_size < 1
            or type(select_page_retries) is not int
            or select_page_retries < 0
            or select_page_cooldown < 0
        ):
            raise ValueError("Invalid SELECT recovery configuration")
        self.select_page_size = select_page_size
        self.select_page_retries = select_page_retries
        self.select_page_cooldown = select_page_cooldown

        # Derive initial method from strategy hint when available.
        if sparql_strategy and not use_post and "post" in sparql_strategy:
            use_post = True

        # Track if we've detected this endpoint requires POST
        self._requires_post = use_post

        # Session for connection pooling. Connections send TCP keepalive probes.
        self._session = requests.Session()
        for scheme in ("http://", "https://"):
            self._session.mount(scheme, _KeepaliveAdapter())
        if urlsplit(self.endpoint_url).hostname in ("localhost", "127.0.0.1", "::1"):
            self._session.trust_env = False

        logger.debug(f"SparqlHelper initialized for {self.endpoint_url}")

    @classmethod
    def from_source_entry(
        cls,
        entry: dict[str, Any],
        *,
        timeout: float | None = None,
        max_retries: int = 3,
    ) -> SparqlHelper:
        """Create SparqlHelper from sources.yaml entry with endpoint and strategy configuration."""
        endpoint = entry.get("endpoint", "")
        if not endpoint:
            raise ValueError("Source entry needs an endpoint URL")
        engine = entry.get("sparql_engine", "") or ""
        strategy = entry.get("sparql_strategy", "") or ""
        t = timeout if timeout is not None else entry.get("timeout") or 30.0
        return cls(
            endpoint,
            timeout=float(t),
            max_retries=max_retries,
            sparql_engine=engine,
            sparql_strategy=strategy,
            inter_request_delay=float(entry.get("delay") or 0),
            max_response_bytes=int(entry.get("max_response_bytes", 64 * 1024 * 1024)),
        )

    @contextmanager
    def budget(self, seconds: float, *, retries: int = 1, recover: bool = False) -> Iterator[None]:
        """Give each request in the block *seconds* and *retries* tries, without page recovery.

        A probe: a query that does not answer in time raises EndpointTimeoutError at once,
        instead of being retried and recovered in pages. Each request ends at *seconds* plus
        DEADLINE_GRACE_S. The settings are restored after.
        """
        saved = (
            self.timeout,
            self.read_timeout,
            self.deadline,
            self.max_retries,
            self.page_recovery,
        )
        self.timeout, self.read_timeout, self.deadline = seconds, seconds, None
        self.max_retries, self.page_recovery = max(1, retries), recover
        try:
            yield
        finally:
            (
                self.timeout,
                self.read_timeout,
                self.deadline,
                self.max_retries,
                self.page_recovery,
            ) = saved

    def select(
        self,
        query: str,
        purpose: str = "",
    ) -> dict[str, Any]:
        """Execute SELECT query and return SPARQL JSON results."""
        result: dict[str, Any] = self._execute(
            query,
            accept=MimeTypes.SELECT_ACCEPT,
            query_type="SELECT",
            parse_json=True,
            purpose=purpose,
        )
        return result

    def construct(self, query: str) -> str:
        """Execute CONSTRUCT query and return Turtle RDF data."""
        result: str = self._execute(
            query,
            accept=MimeTypes.CONSTRUCT_ACCEPT,
            query_type="CONSTRUCT",
            parse_json=False,
        )
        return result

    def construct_graph(self, query: str) -> Graph:
        """Parse remote RDF while rejecting identities that lack an explicit base."""
        turtle_data = self.construct(query)
        graph = Graph()
        if not turtle_data.strip():
            return graph

        def fail(message: str, cause: BaseException | None = None) -> NoReturn:
            """Mark the recorded query as failed and raise the error."""
            error = EndpointError(message)
            records = self.get_collected_queries()
            if records and records[-1].query == query:
                records[-1].success = False
                records[-1].error = type(error).__name__
                records[-1].error_message = str(error)
            raise error from cause

        marker = f"rdfsolve-{secrets.token_hex(8)}:"
        try:
            graph.parse(data=turtle_data, format="turtle", publicID=marker + "//unresolved/")
        except Exception as error:
            fail("CONSTRUCT returned invalid Turtle RDF", error)
        for triple in graph:
            for term in triple:
                iri = term.datatype if isinstance(term, RdfLiteral) else term
                if isinstance(iri, URIRef) and str(iri).startswith(marker):
                    fail(
                        "CONSTRUCT returned relative IRIs without an explicit RDF base. Use anchored SELECT retrieval to preserve the observed terms."
                    )
        return graph

    def ask(self, query: str) -> bool:
        """Execute ASK query and return boolean result."""
        result: dict[str, Any] = self._execute(
            query, accept=MimeTypes.SELECT_ACCEPT, query_type="ASK", parse_json=True
        )
        raw = result.get("boolean")
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str) and raw.strip().lower() in {"true", "false"}:
            return raw.strip().lower() == "true"
        raise EndpointError("ASK response has no valid boolean result")

    # Characters that are illegal inside a SPARQL IRI literal <...>.
    # Characters that are illegal inside a SPARQL ``<…>`` IRI literal.
    _IRI_UNSAFE_CHARS = frozenset('<>"{}|^`\\ \t\n\r')

    def find_classes_for_iris(
        self,
        iris: list[str],
        values_batch_size: int = 50,
    ) -> dict[str, list[str]]:
        """Find rdf:type classes for IRIs using VALUES batching (no graph grouping)."""
        if values_batch_size < 1:
            raise ValueError("Use a positive VALUES batch size")
        for iri in iris:
            PropertyPath(operator="predicate", iri=iri)
        if not iris:
            return {}

        result: dict[str, list[str]] = {}
        for batch_start in range(0, len(iris), values_batch_size):
            batch = iris[batch_start : batch_start + values_batch_size]
            values_block = "\n    ".join(f"<{iri}>" for iri in batch)
            query = (
                "SELECT DISTINCT ?s ?c\n"
                "WHERE {\n"
                "  VALUES ?s {\n"
                f"    {values_block}\n"
                "  }\n"
                "  ?s a ?c .\n"
                "}"
            )
            out = self.select(query)
            for b in out.get("results", {}).get("bindings", []):
                s = b.get("s", {}).get("value")
                c = b.get("c", {}).get("value")
                if not (s and c):
                    continue
                result.setdefault(s, [])
                if c not in result[s]:
                    result[s].append(c)
        return result

    def _execute(
        self,
        query: str,
        accept: str,
        query_type: Literal["SELECT", "CONSTRUCT", "ASK"] = "SELECT",
        parse_json: bool = True,
        purpose: str = "",
    ) -> Any:
        """Record each logical query, including one that fails after retries."""
        query = writable_query(query)
        if self.excluded_graphs and not has_dataset_clause(query):
            query = graph_exclusion_prologue(self.excluded_graphs) + query
        record = QueryRecord(query, query_type, self.endpoint_url, success=False, purpose=purpose)
        started = time.monotonic()
        token = _active_record.set(record)
        label = f"{query_type} [{purpose or '-'}]"
        done = threading.Event()

        def heartbeat() -> None:
            """Log the query while it runs, so that a long query is seen in the job log."""
            while not done.wait(HEARTBEAT_S):
                logger.info("%s still running after %.0f s", label, time.monotonic() - started)

        threading.Thread(target=heartbeat, daemon=True).start()
        try:
            result = self._execute_request(
                query, accept, query_type, parse_json, purpose, record=record
            )
            record.success = True
            if self._collect_queries and self._collect_results:
                record.result = deepcopy(result)
                record.result_retained = True
            return result
        except Exception as error:
            record.error = type(error).__name__
            record.error_message = str(error)
            if isinstance(error, SparqlHelperError):
                error.cut = record.cut
            raise
        finally:
            done.set()
            _active_record.reset(token)
            self._record_query(record)
            record.elapsed_seconds = time.monotonic() - started
            logger.info(
                "%s %s in %.1f s",
                label,
                "completed" if record.success else "failed",
                record.elapsed_seconds,
            )

    def _execute_request(
        self,
        query: str,
        accept: str,
        query_type: Literal["SELECT", "CONSTRUCT", "ASK"] = "SELECT",
        parse_json: bool = True,
        purpose: str = "",
        *,
        record: QueryRecord | None = None,
    ) -> Any:
        """Execute SPARQL query with GET/POST fallback and retry logic."""
        # Try GET first (unless we know POST is required)
        use_post = self._requires_post or len(query.encode("utf-8")) > 2000
        # Track whether we've tried raw POST (application/sparql-query)
        _tried_raw_post = False
        _use_raw_post = "post+raw" in (self.sparql_strategy or "")
        fallback_used = False

        attempt = 0
        requests_made = 0
        overloads = 0
        while attempt < self.max_retries:
            attempt += 1
            requests_made += 1
            attempt_started = time.monotonic()
            if record is not None:
                record.attempts = requests_made
                record.fallback_used = fallback_used
            try:
                if _use_raw_post:
                    result = self._post_raw_query(query, accept)
                    logger.debug(f"Executing {query_type} with raw POST for {purpose}")
                elif use_post:
                    result = self._post_query(query, accept)
                    logger.debug(f"Executing {query_type} with POST for {purpose}")
                else:
                    result = self._get_query(query, accept)
                    logger.debug(f"Executing {query_type} with GET for {purpose}")

                # Check if we got HTML instead of expected format
                if self._is_html_response(result):
                    if not use_post and not _use_raw_post:
                        logger.info("Fallback: GET returned HTML; use POST")
                        fallback_used = True
                        if record is not None:
                            record.fallback_used = True
                        self._requires_post = True
                        use_post = True
                        attempt -= 1
                        continue
                    elif use_post and not _tried_raw_post:
                        logger.info("Fallback: form POST returned HTML; use raw POST")
                        fallback_used = True
                        if record is not None:
                            record.fallback_used = True
                        _use_raw_post = True
                        _tried_raw_post = True
                        attempt -= 1
                        continue
                    else:
                        raise EndpointError(
                            f"{purpose} | Endpoint returned HTML error even with POST"
                        )

                parsed = json.loads(result) if parse_json else result

                if fallback_used:
                    logger.info("%s [%s] used the HTTP fallback", query_type, purpose or "-")
                return parsed

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else 0
                if record is not None:
                    record.status_code = status_code
                    record.response_excerpt = self._last_error_body[:2000]
                body = self._last_error_body.lower()
                detail = _error_detail(self._last_error_body)
                failure = EndpointError(
                    f"HTTP {status_code}: {detail or 'Endpoint request failed'}"
                )
                failure.gateway_overload = self._gateway_overload(status_code, body)
                if any(
                    marker in body
                    for marker in (
                        "sparql compiler",
                        "syntax error",
                        "parse error",
                        "undefined prefix",
                        "undeclared prefix",
                    )
                ):
                    raise EndpointError(f"HTTP {status_code}: query rejected: {detail}") from e
                # A cost or memory limit is a limit with any client status: QLever behind a web
                # server answers "Tried to allocate 397.1 GB" with HTTP 400 and an HTML page.
                # The caller makes the query smaller; the same query by POST is refused again.
                if (
                    400 <= status_code < 500
                    and status_code != 429
                    and any(pat in body for pat in self.COST_LIMIT_PATTERNS)
                ):
                    raise EndpointTimeoutError(
                        f"Query cost/time limit: {detail or status_code}",
                        status_code=status_code,
                    ) from e

                # Check if this looks like a POST-required error
                # 400 = Bad Request (QLever rejects GET), 405 = Method Not Allowed,
                # 414 = URI Too Long
                if not use_post and not _use_raw_post and status_code in (400, 405, 414):
                    logger.info(
                        "Fallback: GET returned %d; use POST",
                        status_code,
                    )
                    fallback_used = True
                    if record is not None:
                        record.fallback_used = True
                    self._requires_post = True
                    use_post = True
                    attempt -= 1
                    continue

                # Form-encoded POST rejected -> try raw POST body
                if use_post and not _tried_raw_post and status_code in (400, 405, 415):
                    logger.info(
                        "Fallback: form POST returned %d; use raw POST",
                        status_code,
                    )
                    fallback_used = True
                    if record is not None:
                        record.fallback_used = True
                    _use_raw_post = True
                    _tried_raw_post = True
                    attempt -= 1
                    continue

                # A server that says it is busy is waited for, with a long backoff.
                if self._overloaded(status_code, body):
                    overloads += 1
                    wait = self._overload_wait(overloads)
                    tag = f"{query_type}[{purpose}]" if purpose else query_type
                    if overloads > self.OVERLOAD_RETRIES or wait > self.rate_limit_wait:
                        raise EndpointRateLimitError(
                            f"HTTP {status_code}: the server stayed busy after {overloads} "
                            f"tries: {detail}"
                        ) from e
                    from rdfsolve._http_policy import defer_host

                    logger.warning(
                        "%s HTTP %d from %s: the server is busy; waiting %.0f s (try %d of %d)",
                        tag,
                        status_code,
                        self.endpoint_url,
                        wait,
                        overloads + 1,
                        self.OVERLOAD_RETRIES + 1,
                    )
                    defer_host(urlsplit(self.endpoint_url).hostname or self.endpoint_url, wait)
                    attempt -= 1
                    continue

                # Check for retryable status codes
                if status_code in self.RETRY_STATUS_CODES:
                    # 502 Bad Gateway often means rate limiting or temporary overload
                    # Treat as timeout so miner can reduce batch size and retry
                    if status_code == 502:
                        tag = f"{query_type}[{purpose}]" if purpose else query_type
                        logger.warning(
                            "%s 502 Bad Gateway from %s - server overloaded, will retry with backoff",
                            tag,
                            self.endpoint_url,
                        )
                        # Treat as timeout to trigger batch size reduction
                        cut = EndpointTimeoutError(
                            f"HTTP 502 Bad Gateway (overload): {e}",
                            status_code=502,
                        )
                        cut.gateway_overload = failure.gateway_overload
                        raise cut from e
                    # Cost and time limits require smaller queries from the caller.
                    if status_code in (429, 500, 504):
                        body = self._last_error_body.lower()
                        is_cost_limit = status_code == 504 or any(
                            pat in body for pat in self.COST_LIMIT_PATTERNS
                        )
                        if is_cost_limit:
                            tag = f"{query_type}[{purpose}]" if purpose else query_type
                            logger.warning(
                                "%s query cost/time limit on %s - not retrying the unchanged query",
                                tag,
                                self.endpoint_url,
                            )
                            cut = EndpointTimeoutError(
                                f"Query cost/time limit: {detail or status_code}",
                                status_code=status_code,
                            )
                            cut.gateway_overload = failure.gateway_overload
                            raise cut from e
                    # Let adaptive callers reduce work after local capacity errors.
                    if status_code == 429 and urlsplit(self.endpoint_url).hostname in (
                        "localhost",
                        "127.0.0.1",
                        "::1",
                    ):
                        tag = f"{query_type}[{purpose}]" if purpose else query_type
                        logger.warning(
                            "%s 429 Too Many Requests from local QLever %s"
                            " - treating as cost limit, falling back",
                            tag,
                            self.endpoint_url,
                        )
                        raise EndpointTimeoutError(
                            f"QLever 429 (at capacity): {e}",
                            status_code=429,
                        ) from e
                    if status_code == 429:
                        if attempt >= self.max_retries:
                            raise EndpointRateLimitError(
                                "Remote host rate limit; retry budget exhausted"
                            ) from e
                        logger.warning(
                            "Remote host rate-limited %s; honor shared cooldown", self.endpoint_url
                        )
                        continue
                    try:
                        self._handle_retry(attempt, query_type, failure, purpose)
                    except EndpointError as exhausted:
                        exhausted.gateway_overload = failure.gateway_overload
                        raise
                    continue

                # Non-retryable HTTP error
                raise failure from e

            except (EndpointTimeoutError, EndpointRateLimitError):
                raise

            except requests.exceptions.Timeout as e:
                # Adaptive callers can reduce page size after a timeout.
                tag = f"{query_type}[{purpose}]" if purpose else query_type
                logger.warning(
                    "%s timed out against %s: %s",
                    tag,
                    self.endpoint_url,
                    e,
                )
                raise EndpointTimeoutError(f"Timeout: {e}") from e

            except requests.exceptions.RequestException as e:
                error_msg = str(e).lower()

                # Permanent failures: fail fast, don't retry
                if self._is_permanent_failure(e):
                    tag = f"{query_type}[{purpose}]" if purpose else query_type
                    logger.warning(
                        "%s endpoint unreachable (%s)- not retrying",
                        tag,
                        self.endpoint_url,
                    )
                    raise EndpointError(f"Endpoint unreachable: {e}") from e

                # Check if this looks like a POST-required error
                if not use_post and self._should_retry_with_post(error_msg):
                    logger.info("Fallback: GET failed; use POST")
                    fallback_used = True
                    if record is not None:
                        record.fallback_used = True
                    self._requires_post = True
                    use_post = True
                    attempt -= 1
                    continue

                waited = time.monotonic() - attempt_started
                if waited >= self.DROPPED_AFTER_SECONDS and any(
                    pat in error_msg for pat in self.DROPPED_PATTERNS
                ):
                    tag = f"{query_type}[{purpose}]" if purpose else query_type
                    logger.warning(
                        "%s connection closed without a response after %.0f s on %s"
                        " - not retrying the unchanged query",
                        tag,
                        waited,
                        self.endpoint_url,
                    )
                    raise EndpointTimeoutError(
                        f"Connection closed without a response after {waited:.0f} s: {e}"
                    ) from e

                # Handle transient network errors with retry
                self._handle_retry(
                    attempt,
                    query_type,
                    e,
                    purpose,
                )

            except json.JSONDecodeError as e:
                # A body that does not parse (empty, cut off, or with invalid characters) is
                # not repeated unchanged: an empty or cut-off answer to a heavy query is a cut
                # (FANAVI answers its heavy queries with an empty HTTP 200, rehearsal
                # 2026-10-06), so the caller makes the query smaller, as for a timeout.
                tag = f"{query_type}[{purpose}]" if purpose else query_type
                logger.warning(
                    "%s response from %s does not parse - not retrying the unchanged query: %s",
                    tag,
                    self.endpoint_url,
                    e,
                )
                kind = "empty response" if e.pos == 0 else "incomplete or invalid response"
                raise EndpointTimeoutError(f"JSON decode error ({kind}): {e}") from e

            except Exception as e:
                error_msg = str(e).lower()

                # Check if this looks like a POST-required error
                if not use_post and self._should_retry_with_post(error_msg):
                    logger.info("Fallback: GET failed; use POST")
                    fallback_used = True
                    if record is not None:
                        record.fallback_used = True
                    self._requires_post = True
                    use_post = True
                    attempt -= 1
                    continue

                self._handle_retry(
                    attempt,
                    query_type,
                    e,
                    purpose,
                )

        # Catch anything else?
        raise EndpointError(f"Query failed unexpectedly [{purpose}]")

    # Known database / backend error fragments.
    _UNHEALTHY_PATTERNS: ClassVar[tuple[str, ...]] = (
        "recovery mode",
        "database system is",
        "connection refused",
        "service unavailable",
        "backend is not available",
        "server is starting",
        "too many connections",
        "out of memory",
        "psqlexception",
    )

    def _check_response_health(
        self,
        response: requests.Response,
        text: str,
    ) -> None:
        """Raise :class:`EndpointUnhealthyError` for deceptive responses.

        Some endpoints return HTTP 200 (or 400) with a plain-text or
        HTML body that is actually a database / proxy error- not a
        valid SPARQL result.  Detecting these early prevents silent
        empty-result bugs and allows callers to handle them
        gracefully.
        """
        ct = response.headers.get("Content-Type", "").lower()
        body = text.strip()

        # If the response is proper SPARQL JSON, nothing to do.
        if "sparql-results+json" in ct or "application/json" in ct:
            return

        # Check for known unhealthy body signatures.
        body_lower = body[:2000].lower()
        for pat in self._UNHEALTHY_PATTERNS:
            if pat in body_lower:
                short = body[:300].replace("\n", " ")
                raise EndpointUnhealthyError(
                    f"Endpoint returned unhealthy response "
                    f"(HTTP {response.status_code}, "
                    f"{ct or 'no content-type'}): "
                    f"{short}"
                )

    def _get_query(self, query: str, accept: str) -> str:
        """Send a bounded GET request."""
        return self._request("GET", query, accept)

    def _post_query(self, query: str, accept: str) -> str:
        """Send a bounded form POST request."""
        return self._request("POST", query, accept)

    def _post_raw_query(self, query: str, accept: str) -> str:
        """Send a bounded SPARQL protocol POST request."""
        return self._request("POST", query, accept, raw=True)

    def _request(self, method: str, query: str, accept: str, *, raw: bool = False) -> str:
        from rdfsolve._host_gate import HostBusyError, host_request

        host = urlsplit(self.endpoint_url).hostname or self.endpoint_url
        record = _active_record.get()
        started = time.monotonic()
        request_started = None
        try:
            with host_request(
                host,
                timeout=self.timeout,
                interval=self.inter_request_delay,
                cooldown_wait=self.rate_limit_wait,
            ):
                request_started = time.monotonic()
                text = self._request_serial(method, query, accept, raw=raw)
                if record is not None:
                    record.cut = None
                return text
        except HostBusyError as error:
            raise EndpointRateLimitError(str(error)) from error
        except Exception as error:
            if record is not None and request_started is not None:
                record.cut = self._cut(error, time.monotonic() - request_started)
            raise
        finally:
            finished = time.monotonic()
            if record is not None:
                record.wait_seconds += (
                    request_started if request_started is not None else finished
                ) - started
                if request_started is not None:
                    record.request_seconds += finished - request_started

    def _request_serial(self, method: str, query: str, accept: str, *, raw: bool = False) -> str:
        """Send one request and read its response before the deadline (see _Deadline).

        A request past its deadline raises a read timeout, which the caller turns into
        EndpointTimeoutError, as for a server that is silent: a response that the deadline
        ended is incomplete, even when its end looks like the end of the body.
        """
        seconds = self.deadline if self.deadline is not None else self.timeout + DEADLINE_GRACE_S
        message = f"no complete response within the deadline of {seconds:g} s"
        with _Deadline(seconds) as deadline:
            try:
                text = self._send(method, query, accept, raw=raw)
            except Exception as error:
                if deadline.expired:
                    raise requests.exceptions.ReadTimeout(message) from error
                raise
            if deadline.expired:
                raise requests.exceptions.ReadTimeout(message)
            return text

    def _send(self, method: str, query: str, accept: str, *, raw: bool = False) -> str:
        host = urlsplit(self.endpoint_url).hostname or self.endpoint_url
        headers = {"Accept": accept, "User-Agent": self.user_agent}
        if method == "POST":
            headers["Content-Type"] = (
                "application/sparql-query" if raw else "application/x-www-form-urlencoded"
            )
        self._last_error_body = ""
        self._last_retry_after = None
        for _ in range(self.MAX_REDIRECTS + 1):
            with self._session.request(
                method,
                self.endpoint_url,
                params={"query": query} if method == "GET" else None,
                data=(
                    (query.encode("utf-8") if raw else {"query": query})
                    if method == "POST"
                    else None
                ),
                headers=headers,
                timeout=(self.timeout, self.read_timeout),
                stream=True,
                allow_redirects=False,
            ) as response:
                location = response.headers.get("Location")
                if response.status_code not in self.REDIRECT_STATUS or not location:
                    return self._read_response(response, host)
            self._follow_redirect(location)
        raise EndpointError(f"More than {self.MAX_REDIRECTS} redirects from the endpoint")

    # A redirect of the endpoint (http to https: AgroLD's sparql.southgreen.fr answers a POST
    # with 302) is followed by sending the same request, with its method and body, to the new
    # URL, which is kept for every later query. requests turns a POST that it follows after a
    # 302 into a GET without the query (AgroLD: HTTP 406, rehearsal 2026-10-06).
    REDIRECT_STATUS: ClassVar[tuple[int, ...]] = (301, 302, 303, 307, 308)
    MAX_REDIRECTS: ClassVar[int] = 5

    def _follow_redirect(self, location: str) -> None:
        """Move the endpoint to where it redirects, without the query string; log it once."""
        from urllib.parse import urljoin, urlunsplit

        parts = urlsplit(urljoin(self.endpoint_url, location))
        target = urlunsplit((parts.scheme, parts.netloc, parts.path, "", "")).rstrip("/") or (
            self.endpoint_url
        )
        if target == self.endpoint_url:
            raise EndpointError(f"The endpoint redirects to itself: {location}")
        logger.warning(
            "The endpoint %s redirects to %s; queries go there", self.endpoint_url, target
        )
        if self.redirected_from is None:
            self.redirected_from = self.endpoint_url
        self.endpoint_url = target

    def _read_response(self, response: requests.Response, host: str) -> str:
        """Read a response within the byte limit; raise for an error status or a cut."""
        from rdfsolve._http_policy import defer_host, retry_after_seconds

        body = bytearray()
        error_response = response.status_code >= 400
        limit = min(self.max_response_bytes, 65536) if error_response else self.max_response_bytes
        for chunk in response.iter_content(chunk_size=65536):
            available = limit - len(body)
            body.extend(chunk[:available])
            if len(chunk) > available:
                if error_response:
                    break
                raise ResponseLimitError(f"Decompressed response exceeds {limit} bytes")
        encoding = (
            response.encoding
            if "charset=" in response.headers.get("Content-Type", "").lower()
            else "utf-8"
        )
        text = body.decode(encoding or "utf-8", errors="replace" if error_response else "strict")
        if error_response:
            self._last_error_body = text
        if response.status_code == 503 or (
            response.status_code == 429
            and not any(pattern in text.lower() for pattern in self.COST_LIMIT_PATTERNS)
        ):
            cooldown = retry_after_seconds(response.headers.get("Retry-After"))
            self._last_retry_after = cooldown
            defer_host(host, cooldown if cooldown is not None else max(1.0, self.initial_backoff))
        elif response.status_code in (502, 504):
            # A gateway's Retry-After marks it busy (_overloaded); without one it is not deferred.
            self._last_retry_after = retry_after_seconds(response.headers.get("Retry-After"))
        response.raise_for_status()
        state = response.headers.get("X-SQL-State", "")
        if response.status_code == 206 or state == "S1TAT":
            # Virtuoso returns what it found before its ANYTIME limit as HTTP 206 with
            # X-SQL-State S1TAT. The results are incomplete: a count is too low and a
            # FILTER NOT EXISTS keeps rows that the rest of the query would remove.
            message = response.headers.get("X-SQL-Message", "").strip()
            raise EndpointTimeoutError(
                f"Query cost/time limit: incomplete results (HTTP {response.status_code}, "
                f"X-SQL-State {state or 'none'}): {message}",
                status_code=response.status_code,
            )
        self._check_response_health(response, text)
        return text

    def _overloaded(self, status_code: int, body: str) -> bool:
        """Return whether a response says that the server is busy (OVERLOAD_PATTERNS).

        A 502, 503, 504 or remote 429 that says so, or that sends Retry-After, is one; a local
        QLever's 429 is its capacity limit (the caller makes the query smaller), and a proxy
        that could not reach the server (PROXY_CONNECT_FAILURE_PATTERNS) is not busy.
        """
        if status_code not in (429, 502, 503, 504):
            return False
        if any(pattern in body for pattern in self.PROXY_CONNECT_FAILURE_PATTERNS):
            return False
        if status_code == 429 and urlsplit(self.endpoint_url).hostname in (
            "localhost",
            "127.0.0.1",
            "::1",
        ):
            return False
        if any(pattern in body for pattern in self.COST_LIMIT_PATTERNS):
            return False
        return self._last_retry_after is not None or any(
            pattern in body for pattern in self.OVERLOAD_PATTERNS
        )

    def _gateway_overload(self, status_code: int, body: str) -> bool:
        """Return whether a gateway error may be an overloaded host (gateway_overload).

        Not when the proxy says that it could not reach the server, nor when the engine behind
        it states a cost limit of the query.
        """
        return (
            status_code in self.GATEWAY_ERROR_STATUS
            and not any(pattern in body for pattern in self.PROXY_CONNECT_FAILURE_PATTERNS)
            and not any(pattern in body for pattern in self.COST_LIMIT_PATTERNS)
        )

    @classmethod
    def overload_backoff(cls, tries: int) -> float:
        """Return the busy-host wait before the *tries*-th repeat (30, 60, 120, 240 s)."""
        return float(cls.OVERLOAD_BACKOFF_S * 2 ** (tries - 1))

    def _overload_wait(self, tries: int) -> float:
        """Return the seconds to wait after the *tries*-th busy answer in a row."""
        if self._last_retry_after is not None:
            return max(1.0, self._last_retry_after)
        return self.overload_backoff(tries)

    def _cut(self, error: BaseException, seconds: float) -> QueryCut | None:
        """Return how a request was cut, when a limit on the way to the data ended it.

        A gateway-timeout status is a cut; any other server error, or a connection closed
        without a response, is one after DROPPED_AFTER_SECONDS: a timer that fired, not a
        query that was refused. The client's own read timeout is not a cut: it is the limit
        that the caller set, and the queries of a step differ in what they cost.
        """
        if isinstance(error, requests.exceptions.HTTPError) and error.response is not None:
            status = error.response.status_code
            if status in self.GATEWAY_TIMEOUT_STATUS or (
                status >= 500 and seconds >= self.DROPPED_AFTER_SECONDS
            ):
                return QueryCut(f"HTTP {status}", seconds)
            return None
        if (
            isinstance(error, requests.exceptions.ConnectionError)
            and not isinstance(error, requests.exceptions.Timeout)
            and seconds >= self.DROPPED_AFTER_SECONDS
        ):
            return QueryCut("connection closed without a response", seconds)
        return None

    def _handle_retry(
        self,
        attempt: int,
        query_type: str,
        error: Exception,
        purpose: str = "",
    ) -> None:
        """
        Handle retry logic with exponential backoff.

        Args:
            attempt: Current attempt number
            query_type: Type of query for logging
            error: The exception that caused the failure
            purpose: Caller-provided context (e.g. "mining/typed-object")

        Raises:
            EndpointError: If max retries exceeded
        """
        tag = f"{query_type}[{purpose}]" if purpose else query_type
        logger.warning(
            f"{tag} attempt {attempt}/{self.max_retries} "
            f"against {self.endpoint_url} failed: {error}"
        )

        if attempt >= self.max_retries:
            logger.error(f"{tag} failed after {self.max_retries} tries")
            raise EndpointError(
                f"Query failed after {self.max_retries} attempts: {error}"
            ) from error

        # Exponential backoff with jitter
        backoff = min(self.initial_backoff * (2 ** (attempt - 1)), self.max_backoff)
        # Use secrets for cryptographically secure jitter
        jitter = secrets.randbelow(int(backoff * 0.1 * 1000) + 1) / 1000
        sleep_time = backoff + jitter

        logger.info(f"Retrying in {sleep_time:.1f}s (attempt {attempt + 1}/{self.max_retries})")
        time.sleep(sleep_time)

    def _should_retry_with_post(self, error_msg: str) -> bool:
        """Check if error indicates POST method should be tried."""
        return any(pattern in error_msg for pattern in self.POST_RETRY_PATTERNS)

    # Patterns in the stringified exception chain that indicate the
    # endpoint is permanently unreachable (DNS, refused, no route).
    _PERMANENT_FAILURE_PATTERNS: ClassVar[tuple[str, ...]] = (
        "name or service not known",  # DNS resolution failure
        "nameresolutionerror",  # urllib3 wrapper
        "nodename nor servname provided",  # macOS DNS failure
        "getaddrinfo failed",  # generic DNS failure
        "no address associated",  # DNS NXDOMAIN
        "[errno 111]",  # connection refused (Linux)
        "[errno 61]",  # connection refused (macOS)
        "[winerror 10061]",  # connection refused (Windows)
        "no route to host",  # network unreachable
        "[errno 113]",  # no route to host (Linux)
    )

    @classmethod
    def _is_permanent_failure(cls, exc: Exception) -> bool:
        """Return True if the exception indicates a permanent failure.

        DNS resolution errors and connection-refused are not transient -
        retrying will always produce the same result.
        """
        # Walk the full exception chain (cause, context, args)
        msg = str(exc).lower()
        cause = exc.__cause__ or exc.__context__
        if cause:
            msg += " " + str(cause).lower()
            inner = getattr(cause, "reason", None)
            if inner:
                msg += " " + str(inner).lower()
        return any(pat in msg for pat in cls._PERMANENT_FAILURE_PATTERNS)

    def _is_html_response(self, content: str) -> bool:
        """Check if content appears to be HTML (error page) instead of RDF."""
        if not content:
            return False
        stripped = content.strip()
        return any(stripped.startswith(marker) for marker in self.HTML_MARKERS)

    def select_with_fallback(
        self,
        query: str,
        *,
        purpose: str = "",
        max_pages: int | None = 10000,
        exhaustive: bool = False,
    ) -> dict[str, Any]:
        """Execute SELECT with optional paging to exhaustion and adaptive recovery.

        All requests use this helper's transport fallback, host spacing, cooldowns,
        response budgets and journal. No filters or required graph patterns are
        dropped. Original outer LIMIT/OFFSET and duplicate multiplicity survive.
        Paging assumes stable data. Blank-node identities and volatile expressions
        cannot safely be reconstructed across independent endpoint responses.
        """
        from pyparsing import (
            Optional,
            ParseResults,
            StringEnd,
            ZeroOrMore,
            original_text_for,
            restOfLine,
        )
        from rdflib.plugins.sparql import prepareQuery
        from rdflib.plugins.sparql.parser import (
            DatasetClause,
            GroupClause,
            HavingClause,
            LimitOffsetClauses,
            OrderClause,
            Prologue,
            SelectClause,
            ValuesClause,
            WhereClause,
            parseQuery,
        )

        meta: SelectExecution = {"strategy": "single_response", "status": "running", "pages": 0}
        self.last_select_execution = meta
        started = time.monotonic()
        try:
            if not exhaustive:
                try:
                    result = self.select(query, purpose=purpose)
                except EndpointTimeoutError as error:
                    meta.update({"first_error": str(error)})
                    if not self.page_recovery:
                        meta.update({"status": "failed"})
                        raise
                    logger.warning("SELECT[%s] switching to adaptive pages: %s", purpose, error)
                else:
                    meta.update(
                        {
                            "status": "complete",
                            "rows": len(result.get("results", {}).get("bindings", [])),
                            "completeness_basis": "endpoint_response",
                        }
                    )
                    return result
            meta.update({"strategy": "adaptive_offset"})
            parsed = parseQuery(query)[1]
            if parsed.name != "SelectQuery":
                raise QueryError("Pagination recovery requires SELECT")
            if "modifier" in parsed and parsed["modifier"] == "REDUCED":
                raise QueryError("Cannot safely page SELECT REDUCED across independent responses")

            def volatile(node: Any) -> bool:
                """Check whether a parsed expression uses a volatile function."""
                if getattr(node, "name", None) in {
                    "Builtin_RAND",
                    "Builtin_UUID",
                    "Builtin_STRUUID",
                    "Builtin_NOW",
                    "Builtin_BNODE",
                    "Aggregate_Sample",
                    "Aggregate_GroupConcat",
                }:
                    return True
                children = (
                    node.values()
                    if isinstance(node, dict)
                    else node
                    if isinstance(node, (list, tuple, ParseResults))
                    else ()
                )
                return any(volatile(child) for child in children)

            if volatile(parsed):
                raise QueryError(
                    "Cannot paginate volatile expressions without changing their meaning"
                )
            projected = [str(v) for v in prepareQuery(query).algebra["PV"]]
            if not projected:
                raise QueryError("No projected variables to order for pagination")

            def aggregate(node: Any) -> bool:
                """Check whether a parsed expression uses an aggregate function."""
                if str(getattr(node, "name", "")).startswith("Aggregate_"):
                    return True
                children = (
                    node.values()
                    if isinstance(node, dict)
                    else node
                    if isinstance(node, (list, tuple, ParseResults))
                    else ()
                )
                return any(aggregate(child) for child in children)

            # Groups are unique by their keys. Aggregate aliases are not used for the
            # order, because some engines (Virtuoso) refuse them next to GROUP BY.
            if "groupby" in parsed:
                keys = []
                for condition in parsed["groupby"]["condition"]:
                    name = (
                        condition if isinstance(condition, Variable) else dict.get(condition, "var")
                    )
                    if name is None:
                        raise QueryError("Name each GROUP BY expression (expr AS ?var) to page it")
                    keys.append(str(name))
                projected = keys
            elif aggregate(parsed["projection"]):
                projected = []  # One group gives one row.
            # Locate the outer slice with the SPARQL grammar. Do not regex-rewrite
            # LIMIT/OFFSET inside strings, nested queries, IRIs or comments.
            body = (
                Prologue
                + SelectClause
                + ZeroOrMore(DatasetClause)
                + WhereClause
                + Optional(GroupClause)
                + Optional(HavingClause)
                + Optional(OrderClause)
            )
            syntax = (
                original_text_for(body)("body")
                + Optional(original_text_for(LimitOffsetClauses)("slice"))
                + original_text_for(ValuesClause)("values")
                + StringEnd()
            )
            syntax.ignore("#" + restOfLine)
            parts = syntax.parse_string(query)
            # CompValue.get substitutes the key when a property is absent.
            slice_ = dict.get(parsed, "limitoffset", {})
            limit = int(slice_["limit"]) if "limit" in slice_ else None
            offset = int(slice_["offset"]) if "offset" in slice_ else 0
            order = []
            for v in projected:
                order.append(
                    f'ASC(IF(BOUND(?{v}), IF(isIRI(?{v}), CONCAT("I", STR(?{v})), '
                    f'CONCAT("L", ENCODE_FOR_URI(STR(?{v})), "|", '
                    f'COALESCE(ENCODE_FOR_URI(LANG(?{v})), ""), "|", '
                    f'COALESCE(ENCODE_FOR_URI(STR(DATATYPE(?{v}))), ""))), "U"))'
                )
            base = parts["body"]
            if order:
                base += "\n" + ("" if "orderby" in parsed else "ORDER BY ") + " ".join(order)
            template = (
                self.escape_sparql_for_format(base)
                + "\nOFFSET {offset}\nLIMIT {limit}\n"
                + self.escape_sparql_for_format(parts["values"])
            )
            rows = []
            try:
                for page in self.select_chunked(
                    template,
                    chunk_size=self.select_page_size,
                    max_total_results=limit,
                    delay_between_chunks=self.inter_request_delay,
                    purpose=purpose,
                    max_pages=max_pages,
                    until_empty=True,
                    stable_terms=True,
                    max_page_retries=self.select_page_retries,
                    initial_offset=offset,
                    wait_after_timeout=self.select_page_cooldown,
                    detect_repeated_pages=(
                        "modifier" in parsed and parsed["modifier"] == "DISTINCT"
                    ),
                ):
                    rows.extend(page)
                    meta.update({"pages": meta["pages"] + 1, "rows": len(rows)})
                    logger.info(
                        "SELECT[%s] page %d; %d rows retained", purpose, meta["pages"], len(rows)
                    )
            except PaginationTruncatedError as error:
                # Virtuoso sorts at most 10,000 rows for a page (SR353). Rows of SELECT DISTINCT,
                # and of a GROUP BY on projected variables, are unique on those variables, so
                # the pages continue with a cursor on them (SIBiLS: the objects of a property).
                distinct = dict.get(parsed, "modifier") == "DISTINCT"
                conditions = parsed["groupby"]["condition"] if "groupby" in parsed else []
                group_keys = sorted(str(c) for c in conditions if isinstance(c, Variable))
                if len(group_keys) != len(conditions) or not set(group_keys) <= set(projected):
                    group_keys = []
                if not (
                    "SR353" in str(error)
                    and (distinct or group_keys)
                    and limit is None
                    and not offset
                    and "orderby" not in parsed
                ):
                    error.partial_rows = deepcopy(rows)
                    raise
                meta.update({"strategy": "cursor_recovery", "offset_error": str(error), "pages": 0})
                rows = []
                for page in self.select_chunked(
                    self.prepare_paginated_query(query),
                    pagination="cursor",
                    cursor_keys=None if distinct else group_keys,
                    chunk_size=self.select_page_size,
                    max_pages=max_pages,
                    delay_between_chunks=self.inter_request_delay,
                    purpose=purpose,
                    max_page_retries=self.select_page_retries,
                    wait_after_timeout=self.select_page_cooldown,
                ):
                    rows.extend(page)
                    meta.update({"pages": meta["pages"] + 1, "rows": len(rows)})
            meta.update(
                {
                    "status": "complete",
                    "rows": len(rows),
                    "completeness_basis": "original_limit"
                    if limit is not None and len(rows) == limit
                    else "empty_page",
                }
            )
            return {"head": {"vars": projected}, "results": {"bindings": rows}}
        except Exception as error:
            meta.update({"status": "failed", "error": f"{type(error).__name__}: {error}"})
            raise
        finally:
            meta["elapsed_seconds"] = time.monotonic() - started

    def select_chunked(
        self,
        query_template: str,
        chunk_size: int = 100,
        max_total_results: int | None = None,
        delay_between_chunks: float = 0.5,
        purpose: str = "",
        max_pages: int | None = 10000,
        until_empty: bool = False,
        stable_terms: bool = False,
        max_page_retries: int = 3,
        pagination: Literal["offset", "cursor"] = "offset",
        cursor_keys: list[str] | None = None,
        initial_offset: int = 0,
        wait_after_timeout: float = 5.0,
        detect_repeated_pages: bool = True,
    ) -> Any:
        """Execute a SELECT query in chunks using offset or cursor paging.

        On timeout, halve the page size and retry the same offset after a
        pause. Keep the smaller size for later pages. Stop with an error if
        the retry budget is spent or a one-row page fails. Requests run sequentially.

        Args:
            query_template: SPARQL query with ``{offset}`` and
                ``{limit}`` placeholders.
            chunk_size: Initial number of results per chunk.
            max_total_results: Cap on total results (``None`` = all).
            delay_between_chunks:
                Pause between pages in seconds.
            purpose: Caller context for log messages.
            max_pages: Stop with an incomplete result after this many pages; None has no cap.
            until_empty: Continue after short pages; reject repeated pages.
            stable_terms: Reject blank nodes whose identity cannot be kept across pages.
            max_page_retries: Maximum page-size reductions per offset, not a data limit.
            pagination: Use OFFSET, or continue after the last returned key.
            cursor_keys: Projected variables that jointly identify a row; default all projected variables.

        Yields:
            List of bindings (dicts) from each chunk.
        """
        if type(initial_offset) is not int or initial_offset < 0 or wait_after_timeout < 0:
            raise ValueError("Use nonnegative offset and cooldown")
        if pagination == "cursor" and initial_offset:
            raise ValueError("initial_offset requires offset pagination")
        cursor_names: list[str] = []
        cursor: tuple[str, ...] | None = None
        cursor_filter = "true"
        if pagination not in {"offset", "cursor"}:
            raise ValueError("pagination must be offset or cursor")
        if pagination == "cursor":
            from pyparsing import original_text_for
            from rdflib.plugins.sparql import prepareQuery
            from rdflib.plugins.sparql.parser import Prologue, parseQuery

            suffix = "\nOFFSET {offset}\nLIMIT {limit}"
            if not query_template.endswith(suffix):
                raise ValueError("Use prepare_paginated_query for cursor paging")
            base = query_template.removesuffix(suffix).format()
            parsed = parseQuery(base)[1]
            if cursor_keys is None and parsed.get("modifier") != "DISTINCT":
                raise ValueError(
                    "Cursor paging needs SELECT DISTINCT or explicit unique cursor_keys"
                )
            projected = {str(variable) for variable in prepareQuery(base).algebra["PV"]}
            cursor_keys = cursor_keys if cursor_keys is not None else sorted(projected)
            if not cursor_keys or not set(cursor_keys) <= projected:
                raise ValueError("Supply projected cursor_keys that jointly identify each row")
            if any(name.startswith("__rdfsolve_cursor") for name in projected):
                raise ValueError("The __rdfsolve_cursor prefix is reserved for paging")
            cursor_names = [f"__rdfsolve_cursor{i}" for i in range(len(cursor_keys))]
            binds = []
            for key, name in zip(cursor_keys, cursor_names, strict=True):
                # Encode term identity, not numeric or human display order.
                expression = (
                    f'IF(BOUND(?{key}), IF(isIRI(?{key}), CONCAT("I", STR(?{key})), '
                    f'CONCAT("L", ENCODE_FOR_URI(STR(?{key})), "|", '
                    f'COALESCE(ENCODE_FOR_URI(LANG(?{key})), ""), "|", '
                    f'COALESCE(ENCODE_FOR_URI(STR(DATATYPE(?{key}))), ""))), "U")'
                )
                binds.append(f"BIND({expression} AS ?{name})")
            prologue = str(original_text_for(Prologue).parse_string(base)[0])
            body = base.lstrip().removeprefix(prologue)
            wrapped = prologue + "\nSELECT * WHERE { { " + body + " } " + " ".join(binds)
            query_template = (
                self.escape_sparql_for_format(wrapped)
                + " FILTER({cursor_filter}) }} ORDER BY "
                + " ".join("?" + name for name in cursor_names)
                + " LIMIT {limit}"
            )

        current_offset = initial_offset
        total_fetched = 0
        current_chunk_size = chunk_size
        if chunk_size < 1 or (max_pages is not None and max_pages < 1) or max_page_retries < 0:
            raise ValueError("Use positive page sizes and nonnegative page retry budgets")
        page_hashes: set[str] = set()

        for _ in count() if max_pages is None else range(max_pages):
            # Honour max_total_results cap
            if max_total_results is not None:
                remaining = max_total_results - total_fetched
                if remaining <= 0:
                    break
                effective_limit = min(current_chunk_size, remaining)
            else:
                effective_limit = current_chunk_size

            query = query_template.format(
                offset=current_offset,
                limit=effective_limit,
                cursor_filter=cursor_filter,
            )

            # attempt this page (with adaptive retries)
            success = False
            last_error: SparqlHelperError | None = None
            reductions = 0

            while True:
                try:
                    logger.debug(
                        "Chunked %s: offset=%d limit=%d",
                        purpose or "query",
                        current_offset,
                        effective_limit,
                    )
                    t0 = time.monotonic()
                    results = self.select(query, purpose=purpose)
                    elapsed = time.monotonic() - t0

                    result_body = results.get("results")
                    bindings = (
                        result_body.get("bindings") if isinstance(result_body, dict) else None
                    )
                    if not isinstance(bindings, list) or any(
                        not isinstance(row, dict) for row in bindings
                    ):
                        raise EndpointUnhealthyError("Expected SELECT result bindings")
                    logger.debug(
                        "Chunked %s: offset=%d returned %d rows in %.1fs",
                        purpose or "query",
                        current_offset,
                        len(bindings),
                        elapsed,
                    )
                    success = True
                    break

                except EndpointTimeoutError as error:
                    last_error = error
                    if reductions >= max_page_retries:
                        logger.warning(
                            "Page recovery budget exhausted at offset %d", current_offset
                        )
                        break
                    # adaptive reduction
                    new_limit = max(effective_limit // 2, 1)

                    if new_limit >= effective_limit:
                        # Can't shrink more
                        logger.warning(
                            "Timeout at offset %d; one-row page failed (%d)",
                            current_offset,
                            effective_limit,
                        )
                        break

                    logger.warning(
                        "Timeout at offset %d - reducing chunk %d -> %d (cooling %ds)",
                        current_offset,
                        effective_limit,
                        new_limit,
                        int(wait_after_timeout),
                    )
                    effective_limit = new_limit
                    reductions += 1
                    current_chunk_size = new_limit  # sticky
                    query = query_template.format(
                        offset=current_offset,
                        limit=effective_limit,
                        cursor_filter=cursor_filter,
                    )
                    time.sleep(wait_after_timeout)

                except SparqlHelperError as e:
                    last_error = e
                    logger.warning(
                        "Chunk query failed at offset %d: %s",
                        current_offset,
                        e,
                    )
                    break  # non-timeout error -> stop paging

            if not success:
                # Raise so callers know the result set is incomplete.
                raise PaginationTruncatedError(
                    f"Pagination abandoned at offset {current_offset}: {last_error}",
                    offset=current_offset,
                ) from last_error

            if not bindings:
                logger.debug("No more results, pagination complete")
                break

            if (stable_terms or cursor_names) and any(
                term.get("type") == "bnode" for row in bindings for term in row.values()
            ):
                raise PaginationTruncatedError(
                    "Blank-node identity cannot be preserved across pages; use a local RDF graph",
                    offset=current_offset,
                )
            if cursor_names:
                for row in bindings:
                    try:
                        next_cursor = tuple(str(row[name]["value"]) for name in cursor_names)
                    except KeyError as error:
                        raise PaginationTruncatedError(
                            "Missing cursor key in endpoint response", offset=current_offset
                        ) from error
                    if cursor is not None and next_cursor <= cursor:
                        raise PaginationTruncatedError(
                            "Cursor keys are not unique or the endpoint did not advance",
                            offset=current_offset,
                        )
                    cursor = next_cursor
                if cursor is None:
                    raise EndpointUnhealthyError("No cursor key returned")
                equal: list[str] = []
                clauses = []
                for name, value in zip(cursor_names, cursor, strict=True):
                    literal = RdfLiteral(value).n3()
                    clauses.append("(" + " && ".join([*equal, f"?{name} > {literal}"]) + ")")
                    equal.append(f"?{name} = {literal}")
                cursor_filter = " || ".join(clauses)
                bindings = [
                    {key: value for key, value in row.items() if key not in cursor_names}
                    for row in bindings
                ]
            if until_empty and detect_repeated_pages:
                digest = hashlib.sha256(json.dumps(bindings, sort_keys=True).encode()).hexdigest()
                if digest in page_hashes:
                    raise PaginationTruncatedError(
                        "The endpoint repeated a page; OFFSET may be ignored", offset=current_offset
                    )
                page_hashes.add(digest)

            # Yield this chunk's results
            yield bindings

            chunk_count = len(bindings)
            total_fetched += chunk_count
            current_offset += chunk_count

            logger.debug(
                "Chunked %s: fetched %d rows (total so far: %d, limit: %d)",
                purpose or "query",
                chunk_count,
                total_fetched,
                effective_limit,
            )

            if (
                chunk_count < effective_limit
                and not until_empty
                and not cursor_names
                and self.row_cap_suspected(chunk_count)
            ):
                # A short page of a common server cap may be cut, not the end: the next page is
                # asked for, in pages of the cap (FANAVI: 1000 rows whatever the LIMIT).
                logger.warning(
                    "Chunked %s: %d rows of %d asked, a common server cap; paging by %d",
                    purpose or "query",
                    chunk_count,
                    effective_limit,
                    chunk_count,
                )
                current_chunk_size = chunk_count
                continue
            if chunk_count < effective_limit and not until_empty and not cursor_names:
                logger.debug(
                    "Partial chunk received, pagination complete",
                )
                break

            if max_total_results is not None and total_fetched >= max_total_results:
                break

            # Delay between pages
            if delay_between_chunks > 0:
                time.sleep(delay_between_chunks)

        else:
            raise PaginationTruncatedError(
                f"Pagination reached the limit of {max_pages} pages",
                offset=current_offset,
            )

    @staticmethod
    def prepare_paginated_query(base_query: str) -> str:
        """
        Prepare a SPARQL query for use with select_chunked by escaping braces.

        Args:
            base_query: SPARQL query WITHOUT OFFSET/LIMIT clauses.
                        Should be a complete query ready to execute.

        Returns:
            Query template safe for use with str.format(offset=N, limit=M)
        """
        # Escape existing braces for .format() compatibility
        escaped = base_query.replace("{", "{{").replace("}", "}}")
        # Add pagination placeholders (single braces, these get substituted)
        return escaped + "\nOFFSET {offset}\nLIMIT {limit}"

    @staticmethod
    def escape_sparql_for_format(query: str) -> str:
        """
        Escape SPARQL braces so the query can be used with str.format().

        This is useful when you need to add your own placeholders to a query
        that contains SPARQL curly braces.

        Args:
            query: SPARQL query with literal curly braces

        Returns:
            Query with braces doubled for .format() compatibility
        """
        return query.replace("{", "{{").replace("}", "}}")

    def close(self) -> None:
        """Close the underlying requests session."""
        self._session.close()

    def __enter__(self) -> Self:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit, close session."""
        self.close()

    def __repr__(self) -> str:
        url = self.endpoint_url
        return f"SparqlHelper({url!r}, use_post={self._requires_post})"


_DATASET_CLAUSE = re.compile(r"(?i)\bFROM\s+(?:NAMED\s+)?(?:<|[A-Za-z_][\w.-]*:)")


def has_dataset_clause(query: str) -> bool:
    """Return whether *query* names its graphs with FROM or FROM NAMED."""
    return bool(_DATASET_CLAUSE.search(query))


def graph_exclusion_prologue(graphs: list[str]) -> str:
    """Return the Virtuoso pragmas that leave *graphs* out of the default and named graphs.

    Without a dataset clause, Virtuoso's default graph is the union of every graph, its system
    graphs included (virtrdf#, the WebDAV graph): the census of AOP-Wiki counted 340,949 triples
    of which 2,479 are virtrdf# and 23 the service description. The pragmas are sent only with a
    query that has no FROM or FROM NAMED: Virtuoso drops a FROM that names an excluded graph and
    reads every other graph instead.
    """
    return "".join(
        f"DEFINE input:default-graph-exclude {URIRef(g).n3()}\n"
        f"DEFINE input:named-graph-exclude {URIRef(g).n3()}\n"
        for g in graphs
    )


# Convenience function for one-off queries


def _error_detail(body: str) -> str:
    """Give the reason of an endpoint error without the echoed query.

    A JSON body (QLever) carries the reason in "exception", which itself may start with
    "Invalid SPARQL query:"; other bodies are cut where the echoed query begins. An HTML page is
    read as its text, which may hold such a JSON body (sparql.uniprot.org, HTTP 400: "Query
    evaluation exception. { "exception": "Tried to allocate 397.1 GB ..." }").
    """
    try:
        reason = json.loads(body).get("exception")
    except (ValueError, AttributeError):
        reason = None
    if reason is None and any(marker in body[:1000] for marker in SparqlHelper.HTML_MARKERS):
        body = re.sub(r"(?is)<(script|style)\b.*?</\1>|<[^>]+>", " ", body)
        body = re.sub(r"\s+", " ", html.unescape(body))
        found = re.search(r'"exception"\s*:\s*"((?:[^"\\]|\\.)*)"', body)
        reason = found.group(1) if found else None
    if isinstance(reason, str) and reason.strip():
        return reason.strip()[:500]
    return body.split("SPARQL query:", 1)[0].strip()[:500]
