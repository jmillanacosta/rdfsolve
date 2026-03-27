"""MCP ontology sub-package — re-exports from :mod:`rdfsolve.ontology`."""

from __future__ import annotations

from rdfsolve.ontology import (
    OlsClient,
    OntologyIndex,
    build_ontology_index,
    load_ontology_index,
    save_ontology_index,
)

__all__ = [
    "OlsClient",
    "OntologyIndex",
    "build_ontology_index",
    "load_ontology_index",
    "save_ontology_index",
]
