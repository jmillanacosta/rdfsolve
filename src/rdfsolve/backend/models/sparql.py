"""Pydantic models for SPARQL query results — re-exports from :mod:`rdfsolve.query`.

Kept for backward compatibility with existing backend code.
"""

from rdfsolve.query import QueryResult, ResultCell

__all__ = ["QueryResult", "ResultCell"]
