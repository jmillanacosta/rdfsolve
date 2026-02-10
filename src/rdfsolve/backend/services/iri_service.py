"""IRI resolution service — thin Flask wrapper.

All core logic lives in :mod:`rdfsolve.iri`.
"""

from __future__ import annotations

from typing import Any

from rdfsolve.iri import resolve_iris


class IriService:
    """Resolve IRIs — delegates to :func:`rdfsolve.iri.resolve_iris`."""

    def resolve(
        self,
        iris: list[str],
        endpoints: list[dict[str, Any]],
        timeout: int = 15,
    ) -> dict[str, Any]:
        """Resolve IRIs against SPARQL endpoints."""
        return resolve_iris(
            iris=iris, endpoints=endpoints, timeout=timeout,
        )
