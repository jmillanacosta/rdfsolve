"""
Schema Miner – extract RDF schema patterns via simple SELECT queries.

Instead of building VoID on the endpoint with heavy CONSTRUCT + BIND
queries, this module runs three lightweight SELECT DISTINCT queries
and assembles the schema in Python:

1. **Typed-object patterns**::

       SELECT DISTINCT ?sc ?p ?oc WHERE {
         ?s ?p ?o . ?s a ?sc . ?o a ?oc .
       }

2. **Literal patterns** (datatype properties)::

       SELECT DISTINCT ?sc ?p (DATATYPE(?o) AS ?dt) WHERE {
         ?s ?p ?o . ?s a ?sc . FILTER(isLiteral(?o))
       }

3. **Untyped-URI patterns** (URI objects without ``rdf:type``)::

       SELECT DISTINCT ?sc ?p WHERE {
         ?s ?p ?o . ?s a ?sc .
         FILTER(isURI(?o))
         FILTER NOT EXISTS { ?o a ?any }
       }

All queries use OFFSET / LIMIT pagination via
:pymethod:`SparqlHelper.select_chunked`.

The primary export is :class:`MinedSchema` (→ JSON-LD).  It can also
be converted to a VoID graph for downstream LinkML / SHACL / RDF-config
export via :pyclass:`VoidParser`.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from rdfsolve.models import (
    AboutMetadata,
    MinedSchema,
    SchemaPattern,
)
from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = [
    "SchemaMiner",
    "mine_schema",
]


# -------------------------------------------------------------------
# SPARQL query templates (braces pre-escaped for str.format)
# -------------------------------------------------------------------

def _graph_clause(
    graph_uris: list[str] | None,
) -> tuple[str, str]:
    """Return (open, close) strings for an optional GRAPH clause.

    If *graph_uris* is ``None`` → empty strings (default graph).
    If a single URI → ``GRAPH <uri> {`` / ``}``.
    If multiple → VALUES-based pattern.
    """
    if not graph_uris:
        return "", ""
    if len(graph_uris) == 1:
        return f"GRAPH <{graph_uris[0]}> {{", "}"
    # Multiple graphs – use VALUES
    values = " ".join(f"(<{u}>)" for u in graph_uris)
    open_ = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"
    return open_, "}"


def _build_typed_object_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 1: typed-object patterns (``?o a ?oc``)."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p ?oc
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    ?o a ?oc .
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_literal_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 2: literal patterns with datatype."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p ?dt
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_untyped_uri_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 3: URI objects that lack an explicit ``rdf:type``."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_typed_count_query(
    graph_uris: list[str] | None,
) -> str:
    """Count query for typed-object patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?sc ?p ?oc (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    ?o a ?oc .
  {g_close}
}}
GROUP BY ?sc ?p ?oc"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_literal_count_query(
    graph_uris: list[str] | None,
) -> str:
    """Count query for literal patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?sc ?p ?dt (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}
GROUP BY ?sc ?p ?dt"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_untyped_count_query(
    graph_uris: list[str] | None,
) -> str:
    """Count query for untyped-URI patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?sc ?p (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}
GROUP BY ?sc ?p"""
    return SparqlHelper.prepare_paginated_query(q)


# -------------------------------------------------------------------
# SchemaMiner
# -------------------------------------------------------------------

class SchemaMiner:
    """Mine RDF schema patterns from a SPARQL endpoint.

    Parameters
    ----------
    endpoint_url:
        SPARQL endpoint URL.
    graph_uris:
        Optional named-graph URI(s) to restrict queries to.
    chunk_size:
        Number of rows per paginated request.
    delay:
        Seconds to sleep between pagination requests.
    timeout:
        HTTP timeout per request (seconds).
    counts:
        Whether to also run COUNT queries for triple counts.
    """

    def __init__(
        self,
        endpoint_url: str,
        graph_uris: str | list[str] | None = None,
        chunk_size: int = 10_000,
        delay: float = 0.5,
        timeout: float = 120.0,
        counts: bool = True,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.graph_uris: list[str] | None = (
            [graph_uris] if isinstance(graph_uris, str)
            else graph_uris
        )
        self.chunk_size = chunk_size
        self.delay = delay
        self.timeout = timeout
        self.counts = counts
        self._helper = SparqlHelper(
            endpoint_url, timeout=timeout,
        )

    # ---- public API -----------------------------------------------

    def mine(
        self,
        dataset_name: str | None = None,
    ) -> MinedSchema:
        """Run all queries and return a :class:`MinedSchema`.

        Parameters
        ----------
        dataset_name:
            Optional human-readable name attached to the metadata.
        """
        patterns: list[SchemaPattern] = []
        t0 = time.monotonic()

        # 1. Typed-object patterns
        logger.info("Mining typed-object patterns …")
        typed = self._run_typed_object()
        patterns.extend(typed)
        logger.info(f"  → {len(typed)} typed-object patterns")

        # 2. Literal patterns
        logger.info("Mining literal patterns …")
        literals = self._run_literal()
        patterns.extend(literals)
        logger.info(f"  → {len(literals)} literal patterns")

        # 3. Untyped-URI patterns
        logger.info("Mining untyped-URI patterns …")
        untyped = self._run_untyped_uri()
        patterns.extend(untyped)
        logger.info(f"  → {len(untyped)} untyped-URI patterns")

        # 4. Optional: counts
        if self.counts:
            logger.info("Fetching triple counts …")
            patterns = self._enrich_counts(patterns)

        dt = time.monotonic() - t0
        logger.info(
            f"Mining complete: {len(patterns)} patterns "
            f"in {dt:.1f}s"
        )

        about = AboutMetadata.build(
            endpoint=self.endpoint_url,
            dataset_name=dataset_name,
            graph_uris=self.graph_uris,
            pattern_count=len(patterns),
            strategy="miner",
        )

        return MinedSchema(patterns=patterns, about=about)

    # ---- private query runners ------------------------------------

    def _collect_bindings(
        self, query_template: str,
    ) -> list[dict[str, Any]]:
        """Paginate through a SELECT query and collect all bindings."""
        all_bindings: list[dict[str, Any]] = []
        for chunk in self._helper.select_chunked(
            query_template,
            chunk_size=self.chunk_size,
            delay_between_chunks=self.delay,
        ):
            all_bindings.extend(chunk)
        return all_bindings

    def _run_typed_object(self) -> list[SchemaPattern]:
        """Run the typed-object SELECT query."""
        q = _build_typed_object_query(self.graph_uris)
        bindings = self._collect_bindings(q)
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            oc = b.get("oc", {}).get("value", "")
            if sc and p and oc:
                results.append(SchemaPattern(
                    subject_class=sc,
                    property_uri=p,
                    object_class=oc,
                ))
        return results

    def _run_literal(self) -> list[SchemaPattern]:
        """Run the literal-property SELECT query."""
        q = _build_literal_query(self.graph_uris)
        bindings = self._collect_bindings(q)
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            dt = b.get("dt", {}).get("value")
            if sc and p:
                results.append(SchemaPattern(
                    subject_class=sc,
                    property_uri=p,
                    object_class="Literal",
                    datatype=dt if dt else None,
                ))
        return results

    def _run_untyped_uri(self) -> list[SchemaPattern]:
        """Run the untyped-URI SELECT query."""
        q = _build_untyped_uri_query(self.graph_uris)
        bindings = self._collect_bindings(q)
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            if sc and p:
                results.append(SchemaPattern(
                    subject_class=sc,
                    property_uri=p,
                    object_class="Resource",
                ))
        return results

    def _enrich_counts(
        self, patterns: list[SchemaPattern],
    ) -> list[SchemaPattern]:
        """Run COUNT queries and merge counts into patterns."""
        # Build lookup: (sc, p, oc) → count
        counts: dict[tuple[str, str, str], int] = {}

        # Typed-object counts
        try:
            q = _build_typed_count_query(self.graph_uris)
            for b in self._collect_bindings(q):
                key = (
                    b.get("sc", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    b.get("oc", {}).get("value", ""),
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(cnt)
        except Exception as e:
            logger.warning(f"Typed-object count query failed: {e}")

        # Literal counts
        try:
            q = _build_literal_count_query(self.graph_uris)
            for b in self._collect_bindings(q):
                dt = b.get("dt", {}).get("value", "")
                key = (
                    b.get("sc", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    f"Literal:{dt}" if dt else "Literal",
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(cnt)
        except Exception as e:
            logger.warning(f"Literal count query failed: {e}")

        # Untyped-URI counts
        try:
            q = _build_untyped_count_query(self.graph_uris)
            for b in self._collect_bindings(q):
                key = (
                    b.get("sc", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    "Resource",
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(cnt)
        except Exception as e:
            logger.warning(f"Untyped-URI count query failed: {e}")

        # Merge counts into patterns
        enriched: list[SchemaPattern] = []
        for pat in patterns:
            # Try exact key
            if pat.object_class == "Literal":
                dt_key = (
                    f"Literal:{pat.datatype}"
                    if pat.datatype
                    else "Literal"
                )
                key = (
                    pat.subject_class, pat.property_uri, dt_key,
                )
            else:
                key = (
                    pat.subject_class,
                    pat.property_uri,
                    pat.object_class,
                )
            cnt = counts.get(key)
            enriched.append(pat.model_copy(update={"count": cnt}))

        return enriched


# -------------------------------------------------------------------
# Convenience function
# -------------------------------------------------------------------

def mine_schema(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    dataset_name: str | None = None,
    chunk_size: int = 10_000,
    delay: float = 0.5,
    timeout: float = 120.0,
    counts: bool = True,
) -> MinedSchema:
    """One-shot helper: mine a schema and return :class:`MinedSchema`.

    Parameters
    ----------
    endpoint_url:
        SPARQL endpoint URL.
    graph_uris:
        Named-graph URI(s) to restrict queries to.
    dataset_name:
        Human-readable name for the dataset.
    chunk_size:
        Pagination page size.
    delay:
        Delay between pages (seconds).
    timeout:
        HTTP timeout per request.
    counts:
        Fetch triple counts per pattern.

    Returns
    -------
    MinedSchema
        Contains patterns and provenance metadata.
    """
    miner = SchemaMiner(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        chunk_size=chunk_size,
        delay=delay,
        timeout=timeout,
        counts=counts,
    )
    return miner.mine(dataset_name=dataset_name)
