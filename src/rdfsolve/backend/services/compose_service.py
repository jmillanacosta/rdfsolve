"""SPARQL query composition service — thin Flask wrapper.

All core logic lives in :mod:`rdfsolve.compose`.
"""

from __future__ import annotations

from typing import Any

from rdfsolve.compose import compose_query_from_paths


class ComposeService:
    """Generate SPARQL queries — delegates to the package."""

    def compose_from_paths(
        self,
        paths: list[dict[str, Any]],
        prefixes: dict[str, str],
        options: dict[str, Any],
    ) -> dict[str, Any]:
        """Generate a SPARQL query from diagram paths."""
        return compose_query_from_paths(
            paths=paths,
            prefixes=prefixes,
            options=options,
        )
