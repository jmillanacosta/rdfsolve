"""SPARQL query execution service — thin Flask wrapper with caching.

All core logic lives in :mod:`rdfsolve.query`.  This service adds
response caching via :mod:`~rdfsolve.backend.services.cache_service`.
"""

from __future__ import annotations

from rdfsolve.query import QueryResult, execute_sparql
from rdfsolve.backend.services.cache_service import cache, cache_key


class SparqlService:
    """Execute SPARQL queries — delegates to :func:`rdfsolve.query.execute_sparql`."""

    def execute(
        self,
        query: str,
        endpoint: str,
        method: str = "GET",
        timeout: int = 30,
        variable_map: dict[str, str] | None = None,
    ) -> QueryResult:
        """Execute a SPARQL query with result caching."""
        key = f"sparql:{cache_key(query, endpoint)}"
        cached = cache.get(key)
        if cached is not None:
            return cached

        result = execute_sparql(
            query=query,
            endpoint=endpoint,
            method=method,
            timeout=timeout,
            variable_map=variable_map or {},
        )

        if result.error is None:
            cache.set(key, result, ttl=300)

        return result
