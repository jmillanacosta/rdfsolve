"""SPARQL query execution with automatic GET/POST fallback and retry logic."""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, Literal
from urllib.parse import urlsplit

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=Warning, module="requests")
    import requests
from rdflib import Graph
from typing_extensions import Self

from rdfsolve.query_collection import QueryCollection, QueryRun, SavedQuery
from rdfsolve.schema_models.paths import PropertyPath

logger = logging.getLogger(__name__)


@dataclass
class QueryRecord:
    """Record of a SPARQL query execution."""

    query: str
    query_type: Literal["SELECT", "CONSTRUCT", "ASK"]
    endpoint_url: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    description: str = ""
    keywords: list[str] = field(default_factory=list)
    success: bool = True
    purpose: str = ""
    error: str | None = None
    attempts: int = 0
    fallback_used: bool = False

    def query_id(self) -> str:
        """Generate a unique ID for this query based on content hash."""
        content = f"{self.query_type}:{self.query}"
        return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()[:12]


class SparqlHelperError(Exception):
    """Base exception for SPARQL helper errors."""


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
        timeout: Request timeout in seconds

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
    COST_LIMIT_PATTERNS: ClassVar[tuple[str, ...]] = (
        "estimated execution time",
        "exceeds the limit",
        "query timed out",
        "timeout expired",
        "execution time limit",
        "statement timeout",
        "cost limit exceeded",
        # QLever-specific: query exhausted memory or thread resources
        "waited for a result from another thread which then failed",
        "memory limit exceeded",
    )

    def enable_query_collection(self, *, clear: bool = True) -> None:
        """Collect query executions, including failures. Optionally keep prior records."""
        self._collect_queries = True
        if clear:
            self._query_registry.clear()

    def disable_query_collection(self) -> None:
        """Stop automatic query collection."""
        self._collect_queries = False

    def get_collected_queries(self) -> list[QueryRecord]:
        """Return execution records, not proof of complete results."""
        return self._query_registry.copy()

    def clear_collected_queries(self) -> None:
        """Clear request records. Keep the named query collection."""
        self._query_registry.clear()

    def _record_query(self, record: QueryRecord) -> None:
        """Collect a query execution without storing its results."""
        if self._collect_queries:
            self._query_registry.append(record)
            if not any(saved.query == record.query for saved in self.queries.queries.values()):
                name = record.query_id()
                if name not in self.queries.queries:
                    try:
                        self.queries.add(name, record.query, endpoint=record.endpoint_url)
                    except Exception as export_error:
                        logger.warning(
                            "Cannot save query %s as a portable example: %s", name, export_error
                        )

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
        started_at = datetime.now(timezone.utc).isoformat()
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
        max_response_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        """Initialize SPARQL helper with retry logic and optional strategy hints."""
        if max_response_bytes < 1 or max_retries < 1 or inter_request_delay < 0:
            raise ValueError("Use positive response/retry limits and nonnegative request delay")
        self.queries = QueryCollection()
        self.history: list[QueryRun] = []
        self._query_registry: list[QueryRecord] = []
        self._collect_queries = False
        self.max_response_bytes = max_response_bytes
        self._last_error_body = ""
        self.endpoint_url = endpoint_url.rstrip("/")
        self.use_post = use_post
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.timeout = timeout
        self.sparql_engine = sparql_engine
        self.sparql_strategy = sparql_strategy
        self.inter_request_delay = inter_request_delay

        # Derive initial method from strategy hint when available.
        if sparql_strategy and not use_post and "post" in sparql_strategy:
            use_post = True

        # Track if we've detected this endpoint requires POST
        self._requires_post = use_post

        # Session for connection pooling
        self._session = requests.Session()
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
        """Execute CONSTRUCT query and return RDFLib Graph."""
        # construct() calls _execute which handles GET->POST fallback
        turtle_data = self.construct(query)

        graph = Graph()
        if turtle_data.strip():
            try:
                graph.parse(data=turtle_data, format="turtle")
            except Exception as error:
                raise EndpointError("CONSTRUCT returned invalid Turtle RDF") from error

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

    def _next_safe_char(self, ch: str) -> str | None:
        """Return next codepoint safe inside IRI literal (scans up to 16 forward)."""
        for offset in range(1, 17):
            candidate = chr(ord(ch) + offset)
            if candidate not in self._IRI_UNSAFE_CHARS:
                return candidate
        return None

    def find_classes_for_uri_pattern(self, uri_prefix: str) -> list[str]:
        """Find rdf:type classes for instances matching URI prefix using IRI-range filter."""
        if not uri_prefix:
            return []

        # Build the exclusive upper bound by finding the next IRI-safe char.
        next_char = self._next_safe_char(uri_prefix[-1])
        if next_char is None:
            # Extremely rare: no safe char found - STRSTARTS fallback.
            escaped = uri_prefix.replace("\\", "\\\\").replace('"', '\\"')
            query = (
                f'SELECT DISTINCT ?c WHERE {{ ?s a ?c . FILTER(STRSTARTS(STR(?s), "{escaped}")) }}'
            )
        else:
            uri_prefix_next = uri_prefix[:-1] + next_char
            query = (
                "SELECT DISTINCT ?c\n"
                "WHERE {\n"
                "  ?s a ?c .\n"
                "  FILTER(\n"
                f"    ?s >= <{uri_prefix}> &&\n"
                f"    ?s <  <{uri_prefix_next}>\n"
                "  )\n"
                "}"
            )
        try:
            out = self.select(query)
        except Exception:
            return []
        bindings = out.get("results", {}).get("bindings", [])
        return [b["c"]["value"] for b in bindings if "c" in b]

    def find_classes_for_iris_by_graph(
        self,
        iris: list[str],
        values_batch_size: int = 50,
    ) -> dict[str, dict[str, list[str]]]:
        """Find rdf:type classes for IRIs grouped by named graph using VALUES batching."""
        if values_batch_size < 1:
            raise ValueError("Use a positive VALUES batch size")
        for iri in iris:
            PropertyPath(operator="predicate", iri=iri)
        if not iris:
            return {}

        result: dict[str, dict[str, list[str]]] = {}
        for batch_start in range(0, len(iris), values_batch_size):
            batch = iris[batch_start : batch_start + values_batch_size]
            values_block = "\n    ".join(f"<{iri}>" for iri in batch)
            query = (
                "SELECT DISTINCT ?s ?g ?c\n"
                "WHERE {\n"
                "  VALUES ?s {\n"
                f"    {values_block}\n"
                "  }\n"
                "  GRAPH ?g { ?s a ?c }\n"
                "}"
            )
            out = self.select(query)
            for b in out.get("results", {}).get("bindings", []):
                s = b.get("s", {}).get("value")
                g = b.get("g", {}).get("value")
                c = b.get("c", {}).get("value")
                if not (s and g and c):
                    continue
                result.setdefault(s, {}).setdefault(g, [])
                if c not in result[s][g]:
                    result[s][g].append(c)
        return result

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

    def find_all_classes(self) -> dict[str, list[str]]:
        """Return all rdf:type classes from endpoint with their instances (no filters)."""
        query = "SELECT DISTINCT ?s ?c\nWHERE {\n  ?s a ?c .\n}"
        try:
            out = self.select(query)
        except Exception:
            return {}

        bindings = out.get("results", {}).get("bindings", [])
        result: dict[str, list[str]] = {}
        for b in bindings:
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
        record = QueryRecord(query, query_type, self.endpoint_url, success=False, purpose=purpose)
        try:
            result = self._execute_request(
                query, accept, query_type, parse_json, purpose, record=record
            )
            record.success = True
            return result
        except Exception as error:
            record.error = type(error).__name__
            raise
        finally:
            self._record_query(record)

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
        use_post = self._requires_post
        # Track whether we've tried raw POST (application/sparql-query)
        _tried_raw_post = False
        _use_raw_post = "post+raw" in (self.sparql_strategy or "")
        fallback_used = False

        for attempt in range(1, self.max_retries + 1):
            if record is not None:
                record.attempts = attempt
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
                        continue
                    elif use_post and not _tried_raw_post:
                        logger.info("Fallback: form POST returned HTML; use raw POST")
                        fallback_used = True
                        if record is not None:
                            record.fallback_used = True
                        _use_raw_post = True
                        _tried_raw_post = True
                        continue
                    else:
                        raise EndpointError(
                            f"{purpose} | Endpoint returned HTML error even with POST"
                        )

                parsed = json.loads(result) if parse_json else result

                logger.info(
                    "%s completed (%s)",
                    query_type,
                    "HTTP fallback used" if fallback_used else "no HTTP fallback",
                )
                return parsed

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else 0

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
                        raise EndpointTimeoutError(
                            f"HTTP 502 Bad Gateway (overload): {e}",
                            status_code=502,
                        ) from e
                    # 429 from a local QLever means the server is at capacity
                    # (query too expensive / concurrent limit hit).
                    # Raise as EndpointTimeoutError immediately so the miner
                    # uses chunked/paginated queries instead of
                    # retrying the same heavy one-shot query 10 more times —
                    # each of which will also run for many minutes before 429.
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
                    # A 500/504 whose body signals "query too expensive"
                    # (Virtuoso cost limit, statement timeout, gateway
                    # timeout, etc.) is not a transient server error -
                    # retrying the identical query will always fail.
                    # Raise as EndpointTimeoutError so callers (e.g.
                    # the two-phase miner) can use pagination.
                    if status_code in (500, 504):
                        body = self._last_error_body.lower()
                        is_cost_limit = status_code == 504 or any(
                            pat in body for pat in self.COST_LIMIT_PATTERNS
                        )
                        if is_cost_limit:
                            tag = f"{query_type}[{purpose}]" if purpose else query_type
                            logger.warning(
                                "%s query cost/time limit on %s - not retrying",
                                tag,
                                self.endpoint_url,
                            )
                            raise EndpointTimeoutError(f"Query cost/time limit: {e}") from e
                    self._handle_retry(
                        attempt,
                        query_type,
                        e,
                        purpose,
                    )
                    continue

                # Non-retryable HTTP error
                raise EndpointError(f"HTTP {status_code}: {e}") from e

            except (EndpointTimeoutError, EndpointRateLimitError):
                raise

            except requests.exceptions.Timeout as e:
                # Timeouts are surfaced immediately so that callers
                # (e.g. select_chunked) can apply adaptive strategies
                # such as reducing the page size, rather than blindly
                # retrying the same expensive query.
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
                    continue

                # Handle transient network errors with retry
                self._handle_retry(
                    attempt,
                    query_type,
                    e,
                    purpose,
                )

            except json.JSONDecodeError as e:
                # JSON parse error raises fast.
                err_msg = str(e).lower()
                if "control character" in err_msg or "invalid" in err_msg:
                    tag = f"{query_type}[{purpose}]" if purpose else query_type
                    logger.warning(
                        "%s JSON parse error (invalid/control-char) from %s - not retrying: %s",
                        tag,
                        self.endpoint_url,
                        e,
                    )
                    raise EndpointTimeoutError(f"JSON decode error (non-retriable): {e}") from e
                # Other JSON parse error, is HTML response - retry.
                self._handle_retry(
                    attempt,
                    query_type,
                    e,
                    purpose,
                )

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
        try:
            with host_request(host, timeout=self.timeout, interval=self.inter_request_delay):
                return self._request_serial(method, query, accept, raw=raw)
        except HostBusyError as error:
            raise EndpointRateLimitError(str(error)) from error

    def _request_serial(self, method: str, query: str, accept: str, *, raw: bool = False) -> str:
        from rdfsolve._http_policy import defer_host, retry_after_seconds

        host = urlsplit(self.endpoint_url).hostname or self.endpoint_url
        headers = {"Accept": accept, "User-Agent": "rdfsolve (SPARQL client)"}
        if method == "POST":
            headers["Content-Type"] = (
                "application/sparql-query" if raw else "application/x-www-form-urlencoded"
            )
        self._last_error_body = ""
        started = time.monotonic()
        with self._session.request(
            method,
            self.endpoint_url,
            params={"query": query} if method == "GET" else None,
            data=(query.encode("utf-8") if raw else {"query": query}) if method == "POST" else None,
            headers=headers,
            timeout=self.timeout,
            stream=True,
        ) as response:
            if response.status_code in (429, 503):
                cooldown = retry_after_seconds(response.headers.get("Retry-After"))
                defer_host(
                    host, cooldown if cooldown is not None else max(1.0, self.initial_backoff)
                )
            body = bytearray()
            error_response = response.status_code >= 400
            limit = (
                min(self.max_response_bytes, 65536) if error_response else self.max_response_bytes
            )
            for chunk in response.iter_content(chunk_size=65536):
                if time.monotonic() - started > self.timeout:
                    raise EndpointTimeoutError("Response stream exceeded the request time budget")
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
            text = body.decode(
                encoding or "utf-8", errors="replace" if error_response else "strict"
            )
            if error_response:
                self._last_error_body = text
            response.raise_for_status()
            self._check_response_health(response, text)
            return text

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

    def get_bindings(self, query: str, purpose: str = "") -> list[dict[str, str]]:
        """
        Execute SELECT query and return simplified bindings list.

        Convenience method that extracts just the variable values.

        Args:
            query: SPARQL SELECT query string
            purpose: Optional tag for log identification

        Returns:
            List of dicts mapping variable names to their values

        Example:
            >>> bindings = helper.get_bindings("SELECT ?s ?p { ?s ?p ?o }")
            >>> for row in bindings:
            ...     print(row["s"], row["p"])
        """
        results = self.select(query, purpose=purpose)
        bindings = results.get("results", {}).get("bindings", [])

        simplified = []
        for binding in bindings:
            row = {}
            for var, val in binding.items():
                row[var] = val.get("value", "")
            simplified.append(row)

        return simplified

    def select_chunked(
        self,
        query_template: str,
        chunk_size: int = 100,
        max_total_results: int | None = None,
        delay_between_chunks: float = 0.5,
        purpose: str = "",
        max_pages: int = 10000,
    ) -> Any:
        """Execute a SELECT query in chunks using OFFSET/LIMIT pagination.

        Uses **adaptive pagination**: when the endpoint times out, the
        chunk (LIMIT) is reduced by ~15 % and the *same* offset is
        retried after a wait period.  The chunk size will never
        shrink below 60 % of the original value (i.e. a maximum
        cumulative reduction of ~40 %).  Up to 3 consecutive shrinks
        are attempted per offset before giving up on that page.

        After a successful fetch with a reduced chunk size, the smaller
        size is kept for subsequent pages (the endpoint is consistently
        slow).

        Args:
            query_template: SPARQL query with ``{offset}`` and
                ``{limit}`` placeholders.
            chunk_size: Initial number of results per chunk.
            max_total_results: Cap on total results (``None`` = all).
            delay_between_chunks:
                Pause between pages in seconds.
            purpose: Caller context for log messages.
            max_pages: Stop with an incomplete result after this many pages.

        Yields:
            List of bindings (dicts) from each chunk.
        """
        # adaptive pagination
        shrink_factor = 0.85  # reduce LIMIT by 15 % each time
        min_chunk_size = max(  # never go below 60 % of original
            int(chunk_size * 0.60),
            1,
        )
        max_shrinks_per_offset = 3  # stop after 3 reductions
        wait_after_timeout = 5.0  # seconds to wait after a timeout

        current_offset = 0
        total_fetched = 0
        current_chunk_size = chunk_size
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        max_iterations = max_pages

        for _ in range(max_iterations):
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
            )

            # attempt this page (with adaptive retries)
            shrink_attempts = 0
            success = False
            last_error: SparqlHelperError | None = None

            while shrink_attempts <= max_shrinks_per_offset:
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
                    # adaptive reduction
                    new_limit = max(
                        int(effective_limit * shrink_factor),
                        min_chunk_size,
                    )

                    if new_limit >= effective_limit:
                        # Can't shrink more
                        logger.warning(
                            "Timeout at offset %d; chunk size already at minimum (%d) - skipping",
                            current_offset,
                            effective_limit,
                        )
                        break

                    shrink_attempts += 1
                    logger.warning(
                        "Timeout at offset %d - reducing chunk "
                        "%d -> %d (attempt %d/%d, cooling %ds)",
                        current_offset,
                        effective_limit,
                        new_limit,
                        shrink_attempts,
                        max_shrinks_per_offset,
                        int(wait_after_timeout),
                    )
                    effective_limit = new_limit
                    current_chunk_size = new_limit  # sticky
                    query = query_template.format(
                        offset=current_offset,
                        limit=effective_limit,
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

            if chunk_count < effective_limit:
                logger.debug(
                    "Partial chunk received, pagination complete",
                )
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


# Convenience function for one-off queries
def sparql_select(
    endpoint_url: str,
    query: str,
    use_post: bool = False,
    purpose: str = "",
) -> dict[str, Any]:
    """
    Execute a one-off SELECT query.

    Convenience function when you don't need to reuse the helper.

    Args:
        endpoint_url: SPARQL endpoint URL
        query: SPARQL SELECT query
        use_post: Force POST method
        purpose: Optional tag for log identification

    Returns:
        SPARQL JSON results
    """
    with SparqlHelper(endpoint_url, use_post=use_post) as helper:
        return helper.select(query, purpose=purpose)


def sparql_construct(
    endpoint_url: str,
    query: str,
    use_post: bool = False,
) -> Graph:
    """
    Execute a one-off CONSTRUCT query.

    Convenience function when you don't need to reuse the helper.

    Args:
        endpoint_url: SPARQL endpoint URL
        query: SPARQL CONSTRUCT query
        use_post: Force POST method

    Returns:
        RDFLib Graph with constructed triples
    """
    with SparqlHelper(endpoint_url, use_post=use_post) as helper:
        return helper.construct_graph(query)
