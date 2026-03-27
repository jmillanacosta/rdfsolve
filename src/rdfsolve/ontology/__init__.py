"""Ontology grounding library for rdfsolve.

Core library module — used directly by rdfsolve consumers and also by
the MCP layer (``rdfsolve.mcp``).

Public surface
--------------
:class:`OntologyIndex`
    In-memory knowledge base: term labels → class IRIs, class → ontology,
    ancestor chains, import-graph, and base-URI crosswalk.

:class:`~rdfsolve.ontology.ols_client.OlsClient`
    Rate-limited, cache-aware HTTP client for the EBI OLS4 REST API v2.

:func:`build_ontology_index`
    Fetch OLS4 metadata and compile an :class:`OntologyIndex`.

:func:`save_ontology_index`
    Persist an index to ``ontology_index.pkl.gz`` + ``ontology_graph.graphml``.

:func:`load_ontology_index`
    Restore a previously saved index from disk.
"""

from __future__ import annotations

from rdfsolve.ontology.index import (
    OntologyIndex,
    build_ontology_index,
    load_ontology_index,
    load_ontology_index_from_db,
    save_ontology_index,
    save_ontology_index_to_db,
)
from rdfsolve.ontology.ols_client import OlsClient

__all__ = [
    "OlsClient",
    "OntologyIndex",
    "build_ontology_index",
    "load_ontology_index",
    "load_ontology_index_from_db",
    "save_ontology_index",
    "save_ontology_index_to_db",
]
