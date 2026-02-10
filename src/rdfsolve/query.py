"""SPARQL query execution — pure-library module (no Flask dependency).

This module provides structured SPARQL query execution built on top of
:class:`~rdfsolve.sparql_helper.SparqlHelper`.  It adds:

* Pydantic result models (:class:`ResultCell`, :class:`QueryResult`)
  that give strongly-typed access to SPARQL JSON result bindings.
* A single :func:`execute_sparql` convenience function with a clean
  dict-return signature suitable for the public API.

All HTTP / retry / GET→POST logic is delegated to ``SparqlHelper``.
"""

from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel, Field

from rdfsolve.sparql_helper import SparqlHelper

# ── Result models ─────────────────────────────────────────────────


class ResultCell(BaseModel):
    """One cell in a SPARQL result row."""

    value: str
    type: str  # "uri" | "literal" | "bnode"
    lang: str | None = None
    datatype: str | None = None


class QueryResult(BaseModel):
    """Structured result from a SPARQL query execution."""

    query: str
    endpoint: str
    variables: list[str]
    rows: list[dict[str, ResultCell]]
    variable_map: dict[str, str] = Field(default_factory=dict)
    row_count: int
    duration_ms: int
    error: str | None = None


# ── Public helper ─────────────────────────────────────────────────


def execute_sparql(
    query: str,
    endpoint: str,
    *,
    method: str = "GET",
    timeout: int = 30,
    variable_map: dict[str, str] | None = None,
) -> QueryResult:
    """Execute a SPARQL SELECT query and return a :class:`QueryResult`.

    Parameters
    ----------
    query:
        Full SPARQL query string.
    endpoint:
        URL of the SPARQL endpoint.
    method:
        HTTP method (``"GET"`` or ``"POST"``).  If ``"GET"`` fails the
        underlying :class:`SparqlHelper` will automatically retry with
        POST.
    timeout:
        Request timeout in seconds.
    variable_map:
        Optional mapping of SPARQL ``?variable`` names to schema URIs.

    Returns
    -------
    QueryResult
        Pydantic model with ``query``, ``endpoint``, ``variables``,
        ``rows``, ``variable_map``, ``row_count``, ``duration_ms``, and
        optionally ``error``.
    """
    t0 = time.monotonic()

    try:
        helper = SparqlHelper(
            endpoint,
            use_post=(method.upper() == "POST"),
            timeout=float(timeout),
        )
        json_result = helper.select(query)
    except Exception as exc:
        return QueryResult(
            query=query,
            endpoint=endpoint,
            variables=[],
            rows=[],
            variable_map=variable_map or {},
            row_count=0,
            duration_ms=int((time.monotonic() - t0) * 1000),
            error=str(exc),
        )

    variables: list[str] = json_result.get("head", {}).get("vars", [])
    bindings: list[dict[str, Any]] = (
        json_result.get("results", {}).get("bindings", [])
    )

    rows: list[dict[str, ResultCell]] = []
    for binding in bindings:
        row: dict[str, ResultCell] = {}
        for var in variables:
            cell_data = binding.get(var)
            if cell_data:
                cell_type = cell_data.get("type", "literal")
                if cell_type == "uri":
                    rtype = "uri"
                elif cell_type == "bnode":
                    rtype = "bnode"
                else:
                    rtype = "literal"
                row[var] = ResultCell(
                    value=cell_data["value"],
                    type=rtype,
                    lang=cell_data.get("xml:lang"),
                    datatype=cell_data.get("datatype"),
                )
        rows.append(row)

    duration_ms = int((time.monotonic() - t0) * 1000)

    return QueryResult(
        query=query,
        endpoint=endpoint,
        variables=variables,
        rows=rows,
        variable_map=variable_map or {},
        row_count=len(rows),
        duration_ms=duration_ms,
    )
