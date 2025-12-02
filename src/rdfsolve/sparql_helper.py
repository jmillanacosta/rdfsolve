"""
SPARQL Helper - Centralized SPARQL query execution with automatic fallback.

This module is a SPARQL client that handles:
- Automatic GET → POST fallback for endpoints that require POST
- Exponential backoff retry logic for transient failures
- Support for SELECT (JSON) and CONSTRUCT (Turtle/N3) queries
- HTML error detection in responses
- Consistent logging across all SPARQL operations
- Support for pagination (limit and offset usage)

Usage:
    from rdfsolve.sparql_helper import SparqlHelper

    # Create a helper for an endpoint
    helper = SparqlHelper("https://sparql.example.org/")

    # Execute SELECT query (returns dict)
    results = helper.select("SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10")

    # Execute CONSTRUCT query (returns bytes/string)
    turtle_data = helper.construct("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")

    # Execute ASK query (returns bool)
    exists = helper.ask("ASK { ?s a <http://example.org/Class> }")
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import requests
from rdflib import Graph

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

    def query_id(self) -> str:
        """Generate a unique ID for this query based on content hash."""
        content = f"{self.query_type}:{self.query}"
        return hashlib.md5(content.encode()).hexdigest()[:12]


class SparqlHelperError(Exception):
    """Base exception for SPARQL helper errors."""

    pass


class EndpointError(SparqlHelperError):
    """Raised when the endpoint returns an error."""

    pass


class QueryError(SparqlHelperError):
    """Raised when the query itself is invalid."""

    pass


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

    Uses standard `requests` library - no SPARQLWrapper dependency required.

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

    # Class-level query registry to collect all executed queries
    _query_registry: list[QueryRecord] = []
    _collect_queries: bool = False

    @classmethod
    def enable_query_collection(cls) -> None:
        """Enable collection of all executed queries."""
        cls._collect_queries = True
        cls._query_registry = []
        logger.debug("Query collection enabled")

    @classmethod
    def disable_query_collection(cls) -> None:
        """Disable query collection."""
        cls._collect_queries = False
        logger.debug("Query collection disabled")

    @classmethod
    def get_collected_queries(cls) -> list[QueryRecord]:
        """Get all collected queries."""
        return cls._query_registry.copy()

    @classmethod
    def clear_collected_queries(cls) -> None:
        """Clear all collected queries."""
        cls._query_registry = []

    @classmethod
    def _record_query(
        cls,
        query: str,
        query_type: Literal["SELECT", "CONSTRUCT", "ASK"],
        endpoint_url: str,
        description: str = "",
        keywords: list[str] | None = None,
        success: bool = True,
    ) -> None:
        """Record a query if collection is enabled."""
        if cls._collect_queries:
            record = QueryRecord(
                query=query,
                query_type=query_type,
                endpoint_url=endpoint_url,
                description=description,
                keywords=keywords or [],
                success=success,
            )
            cls._query_registry.append(record)

    @classmethod
    def export_queries_as_ttl(
        cls,
        output_file: str | None = None,
        base_uri: str = "https://example.org/sparql-queries/",
        dataset_name: str = "dataset",
    ) -> str:
        """
        Export collected queries as TTL using SHACL SPARQL representation.

        Args:
            output_file: Optional file path to write TTL
            base_uri: Base URI for query IRIs
            dataset_name: Name of the dataset for namespacing

        Returns:
            TTL string with all collected queries
        """
        # Deduplicate queries by content hash
        seen_hashes: set[str] = set()
        unique_queries: list[QueryRecord] = []
        for record in cls._query_registry:
            query_hash = record.query_id()
            if query_hash not in seen_hashes:
                seen_hashes.add(query_hash)
                unique_queries.append(record)

        # Build TTL
        lines = [
            f"@prefix ex: <{base_uri}{dataset_name}/> .",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
            "@prefix schema: <https://schema.org/> .",
            "@prefix sh: <http://www.w3.org/ns/shacl#> .",
            "@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .",
            "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
            "",
        ]

        for record in unique_queries:
            query_id = record.query_id()
            query_type_class = {
                "SELECT": "sh:SPARQLSelectExecutable",
                "CONSTRUCT": "sh:SPARQLConstructExecutable",
                "ASK": "sh:SPARQLAskExecutable",
            }.get(record.query_type, "sh:SPARQLExecutable")

            query_predicate = {
                "SELECT": "sh:select",
                "CONSTRUCT": "sh:construct",
                "ASK": "sh:ask",
            }.get(record.query_type, "sh:select")

            # Escape the query for TTL (triple-quoted string)
            escaped_query = record.query.replace('\\', '\\\\').replace('"""', '\\"\\"\\"')

            lines.append(f"ex:{query_id} a sh:SPARQLExecutable,")
            lines.append(f"        {query_type_class} ;")

            if record.description:
                escaped_desc = record.description.replace('"', '\\"')
                lines.append(f'    rdfs:comment "{escaped_desc}" ;')

            lines.append(f'    {query_predicate} """')
            lines.append(escaped_query)
            lines.append('""" ;')

            if record.keywords:
                kw_str = " , ".join(f'"{kw}"' for kw in record.keywords)
                lines.append(f"    schema:keywords {kw_str} ;")

            lines.append(f'    schema:dateCreated "{record.timestamp}"^^xsd:dateTime ;')
            lines.append("    schema:target [")
            lines.append("        a sd:Service ;")
            lines.append(f'        sd:endpoint <{record.endpoint_url}>')
            lines.append("    ] .")
            lines.append("")

        ttl_content = "\n".join(lines)

        if output_file:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(ttl_content)
            logger.info(f"Exported {len(unique_queries)} queries to {output_file}")

        return ttl_content

    def __init__(
        self,
        endpoint_url: str,
        *,
        use_post: bool = False,
        max_retries: int = 3,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
        timeout: float = 60.0,
    ) -> None:
        """
        Initialize the SPARQL helper.

        Args:
            endpoint_url: SPARQL endpoint URL
            use_post: Always use POST (default: False, tries GET first)
            max_retries: Maximum retry attempts for transient failures
            initial_backoff: Initial delay between retries (seconds)
            max_backoff: Maximum delay between retries (seconds)
            timeout: Request timeout in seconds (default: 60)
        """
        self.endpoint_url = endpoint_url.rstrip("/")
        self.use_post = use_post
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.timeout = timeout

        # Track if we've detected this endpoint requires POST
        self._requires_post = use_post

        # Session for connection pooling
        self._session = requests.Session()

        logger.debug(f"SparqlHelper initialized for {self.endpoint_url}")

    def select(self, query: str) -> dict[str, Any]:
        """
        Execute a SELECT query and return JSON results.

        Args:
            query: SPARQL SELECT query string

        Returns:
            Dictionary with SPARQL JSON results format:
            {
                "head": {"vars": ["s", "p", "o"]},
                "results": {"bindings": [...]}
            }

        Raises:
            EndpointError: If the endpoint returns an error after all retries
            QueryError: If the query is malformed
        """
        result: dict[str, Any] = self._execute(
            query, accept=MimeTypes.SELECT_ACCEPT, query_type="SELECT", parse_json=True
        )
        return result

    def construct(self, query: str) -> str:
        """
        Execute a CONSTRUCT query and return Turtle RDF data.

        Args:
            query: SPARQL CONSTRUCT query string

        Returns:
            Turtle-formatted RDF string

        Raises:
            EndpointError: If the endpoint returns an error after all retries
            QueryError: If the query is malformed
        """
        result: str = self._execute(
            query,
            accept=MimeTypes.CONSTRUCT_ACCEPT,
            query_type="CONSTRUCT",
            parse_json=False,
        )
        return result

    def construct_graph(self, query: str) -> Graph:
        """
        Execute a CONSTRUCT query and return an RDFLib Graph.

        Args:
            query: SPARQL CONSTRUCT query string

        Returns:
            RDFLib Graph containing the constructed triples

        Raises:
            EndpointError: If the endpoint returns an error after all retries
            QueryError: If the query is malformed
        """
        turtle_data = self.construct(query)

        # Skip if we got HTML error response
        if self._is_html_response(turtle_data):
            logger.warning("Received HTML instead of RDF data")
            return Graph()

        graph = Graph()
        if turtle_data.strip():
            try:
                graph.parse(data=turtle_data, format="turtle")
            except Exception as e:
                logger.warning(f"Failed to parse CONSTRUCT as Turtle: {e}")
                # Try N3 format as fallback
                try:
                    graph.parse(data=turtle_data, format="n3")
                except Exception:
                    logger.error("Failed to parse CONSTRUCT result")

        return graph

    def ask(self, query: str) -> bool:
        """
        Execute an ASK query and return boolean result.

        Args:
            query: SPARQL ASK query string

        Returns:
            True if the pattern exists, False otherwise

        Raises:
            EndpointError: If the endpoint returns an error after all retries
            QueryError: If the query is malformed
        """
        result: dict[str, Any] = self._execute(
            query, accept=MimeTypes.SELECT_ACCEPT, query_type="ASK", parse_json=True
        )
        return bool(result.get("boolean", False))

    def _execute(
        self,
        query: str,
        accept: str,
        query_type: Literal["SELECT", "CONSTRUCT", "ASK"] = "SELECT",
        parse_json: bool = True,
    ) -> Any:
        """
        Execute a SPARQL query with automatic GET/POST fallback and retry.

        Args:
            query: SPARQL query string
            accept: Accept header value for content negotiation
            query_type: Type of query for logging
            parse_json: Whether to parse response as JSON

        Returns:
            Query results (dict for JSON, str for RDF formats)

        Raises:
            EndpointError: If query fails after all retries
        """
        # Try GET first (unless we know POST is required)
        use_post = self._requires_post

        for attempt in range(1, self.max_retries + 1):
            try:
                if use_post:
                    result = self._post_query(query, accept)
                    logger.debug(f"Executing {query_type} with POST")
                else:
                    result = self._get_query(query, accept)
                    logger.debug(f"Executing {query_type} with GET")

                # Check if we got HTML instead of expected format
                if self._is_html_response(result):
                    if not use_post:
                        logger.debug("GET returned HTML, switching to POST")
                        self._requires_post = True
                        use_post = True
                        continue
                    else:
                        raise EndpointError("Endpoint returned HTML error even with POST")

                # Record successful query
                SparqlHelper._record_query(
                    query=query,
                    query_type=query_type,
                    endpoint_url=self.endpoint_url,
                    success=True,
                )

                # Parse JSON if requested
                if parse_json:
                    return json.loads(result)

                return result

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else 0

                # Check if this looks like a POST-required error (405 Method Not Allowed)
                if not use_post and status_code == 405:
                    logger.debug("GET returned 405, switching to POST")
                    self._requires_post = True
                    use_post = True
                    continue

                # Check for retryable status codes
                if status_code in self.RETRY_STATUS_CODES:
                    self._handle_retry(attempt, query_type, e)
                    continue

                # Non-retryable HTTP error
                raise EndpointError(f"HTTP {status_code}: {e}") from e

            except requests.exceptions.RequestException as e:
                error_msg = str(e).lower()

                # Check if this looks like a POST-required error
                if not use_post and self._should_retry_with_post(error_msg):
                    logger.debug(f"GET failed, switching to POST: {e}")
                    self._requires_post = True
                    use_post = True
                    continue

                # Handle transient network errors with retry
                self._handle_retry(attempt, query_type, e)

            except json.JSONDecodeError as e:
                # JSON parse error - might be HTML response
                self._handle_retry(attempt, query_type, e)

            except Exception as e:
                error_msg = str(e).lower()

                # Check if this looks like a POST-required error
                if not use_post and self._should_retry_with_post(error_msg):
                    logger.debug(f"GET failed, switching to POST: {e}")
                    self._requires_post = True
                    use_post = True
                    continue

                self._handle_retry(attempt, query_type, e)

        # Should not reach here, but just in case
        raise EndpointError("Query failed unexpectedly")

    def _get_query(self, query: str, accept: str) -> str:
        """
        Execute SPARQL query using HTTP GET.

        Args:
            query: SPARQL query string
            accept: Accept header for content negotiation

        Returns:
            Response body as string

        Raises:
            requests.exceptions.HTTPError: On HTTP errors
        """
        headers = {
            "Accept": accept,
            "User-Agent": "rdfsolve/1.0 (SPARQL client)",
        }

        params = {"query": query}

        response = self._session.get(
            self.endpoint_url,
            params=params,
            headers=headers,
            timeout=self.timeout,
        )
        response.raise_for_status()

        return response.text

    def _post_query(self, query: str, accept: str) -> str:
        """
        Execute SPARQL query using HTTP POST.

        Uses application/x-www-form-urlencoded encoding as per SPARQL protocol.

        Args:
            query: SPARQL query string
            accept: Accept header for content negotiation

        Returns:
            Response body as string

        Raises:
            requests.exceptions.HTTPError: On HTTP errors
        """
        headers = {
            "Accept": accept,
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "rdfsolve/1.0 (SPARQL client)",
        }

        data = {"query": query}

        response = self._session.post(
            self.endpoint_url,
            data=data,
            headers=headers,
            timeout=self.timeout,
        )
        response.raise_for_status()

        return response.text

    def _handle_retry(self, attempt: int, query_type: str, error: Exception) -> None:
        """
        Handle retry logic with exponential backoff.

        Args:
            attempt: Current attempt number
            query_type: Type of query for logging
            error: The exception that caused the failure

        Raises:
            EndpointError: If max retries exceeded
        """
        logger.warning(f"Query attempt {attempt}/{self.max_retries} failed: {error}")

        if attempt >= self.max_retries:
            logger.error(f"{query_type} failed after {self.max_retries} tries")
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

    def _is_html_response(self, content: str) -> bool:
        """Check if content appears to be HTML (error page) instead of RDF."""
        if not content:
            return False
        stripped = content.strip()
        return any(stripped.startswith(marker) for marker in self.HTML_MARKERS)

    def get_bindings(self, query: str) -> list[dict[str, str]]:
        """
        Execute SELECT query and return simplified bindings list.

        Convenience method that extracts just the variable values.

        Args:
            query: SPARQL SELECT query string

        Returns:
            List of dicts mapping variable names to their values

        Example:
            >>> bindings = helper.get_bindings("SELECT ?s ?p { ?s ?p ?o }")
            >>> for row in bindings:
            ...     print(row["s"], row["p"])
        """
        results = self.select(query)
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
    ) -> Any:
        """
        Execute a SELECT query in chunks using OFFSET/LIMIT pagination.

        This is useful for large result sets that might timeout or overwhelm
        the endpoint if requested all at once.

        Args:
            query_template: SPARQL query with {offset} and {limit} placeholders
            chunk_size: Number of results per chunk (default: 100)
            max_total_results: Maximum total results to fetch (None = all)
            delay_between_chunks: Sleep time between chunks in seconds

        Yields:
            List of bindings from each chunk

        Example:
            >>> query = "SELECT ?s WHERE {{ ?s a ?class }} OFFSET {offset} LIMIT {limit}"
            >>> for bindings in helper.select_chunked(query, chunk_size=1000):
            ...     for binding in bindings:
            ...         print(binding["s"]["value"])
        """
        current_offset = 0
        total_fetched = 0
        max_iterations = 10000  # Safety limit

        for _ in range(max_iterations):
            # Calculate chunk size for this iteration
            if max_total_results is not None:
                remaining = max_total_results - total_fetched
                if remaining <= 0:
                    break
                current_chunk_size = min(chunk_size, remaining)
            else:
                current_chunk_size = chunk_size

            # Format query with current pagination
            query = query_template.format(offset=current_offset, limit=current_chunk_size)

            try:
                logger.debug(
                    f"Executing chunked query: offset={current_offset}, limit={current_chunk_size}"
                )
                results = self.select(query)

                # Extract bindings
                bindings = results.get("results", {}).get("bindings", [])

                if not bindings:
                    logger.debug("No more results, pagination complete")
                    break

                # Yield this chunk's results
                yield bindings

                # Update counters
                chunk_count = len(bindings)
                total_fetched += chunk_count
                current_offset += chunk_count

                logger.debug(f"Fetched chunk: {chunk_count} results (total: {total_fetched})")

                # If chunk was smaller than requested, we've reached the end
                if chunk_count < current_chunk_size:
                    logger.debug("Partial chunk received, pagination complete")
                    break

                # Respectful delay between chunks
                if delay_between_chunks > 0:
                    time.sleep(delay_between_chunks)

            except Exception as e:
                logger.warning(f"Chunk query failed at offset {current_offset}: {e}")
                break

        else:
            logger.warning(f"Chunked query hit max iterations ({max_iterations})")

    @staticmethod
    def prepare_paginated_query(base_query: str) -> str:
        """
        Prepare a SPARQL query for use with select_chunked by escaping braces.

        SPARQL queries contain curly braces {} which conflict with Python's
        str.format() used for pagination placeholders. This method:
        1. Escapes all existing braces ({{ and }})
        2. Appends OFFSET {offset} and LIMIT {limit} placeholders

        Args:
            base_query: SPARQL query WITHOUT OFFSET/LIMIT clauses.
                        Should be a complete query ready to execute.

        Returns:
            Query template safe for use with str.format(offset=N, limit=M)

        Example:
            >>> query = "SELECT ?s WHERE { ?s a ?class }"
            >>> template = SparqlHelper.prepare_paginated_query(query)
            >>> # template is now safe for: template.format(offset=0, limit=100)
            >>> for bindings in helper.select_chunked(template):
            ...     process(bindings)
        """
        # Escape existing braces for .format() compatibility
        escaped = base_query.replace("{", "{{").replace("}", "}}")
        # Add pagination placeholders (single braces - these get substituted)
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

        Example:
            >>> q = "SELECT ?s WHERE { ?s a <{class_uri}> }"  # Won't work!
            >>> # Instead:
            >>> q = SparqlHelper.escape_sparql_for_format(
            ...     "SELECT ?s WHERE { ?s a <CLASS_PLACEHOLDER> }"
            ... )
            >>> q = q.replace("CLASS_PLACEHOLDER", "{class_uri}")
        """
        return query.replace("{", "{{").replace("}", "}}")

    def close(self) -> None:
        """Close the underlying requests session."""
        self._session.close()

    def __enter__(self) -> SparqlHelper:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - close session."""
        self.close()

    def __repr__(self) -> str:
        url = self.endpoint_url
        return f"SparqlHelper({url!r}, use_post={self._requires_post})"


# Convenience function for one-off queries
def sparql_select(
    endpoint_url: str,
    query: str,
    use_post: bool = False,
) -> dict[str, Any]:
    """
    Execute a one-off SELECT query.

    Convenience function when you don't need to reuse the helper.

    Args:
        endpoint_url: SPARQL endpoint URL
        query: SPARQL SELECT query
        use_post: Force POST method

    Returns:
        SPARQL JSON results
    """
    with SparqlHelper(endpoint_url, use_post=use_post) as helper:
        return helper.select(query)


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
