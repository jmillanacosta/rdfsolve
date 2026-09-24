"""Public entry points, re-exported from the modules that implement them."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rdfsolve.analysis.connectivity import build_connectivity, compare_schemas
from rdfsolve.client.api import Client
from rdfsolve.client.collections import RDFList
from rdfsolve.client.ontology import OntologyLookup
from rdfsolve.client.query import execute_sparql
from rdfsolve.client.query_fragments import PreparedQuery, QueryPattern
from rdfsolve.client.retrieval import Requirement
from rdfsolve.metadata import query_metadata
from rdfsolve.mining.miner import mine_schema
from rdfsolve.models import MinedSchema
from rdfsolve.query_collection import QueryCollection
from rdfsolve.source_enrichment import enrich_source
from rdfsolve.sources import enrich_source_with_bioregistry, get_bioregistry_metadata, load_sources
from rdfsolve.void_source import (
    discover_all_graphs,
    discover_void_graphs,
    discover_void_source,
    export_schema_artifacts,
)

if TYPE_CHECKING:
    from rdfsolve.mcp.workflow import ask_rdf

__all__ = [
    "Client",
    "OntologyLookup",
    "PreparedQuery",
    "QueryCollection",
    "QueryPattern",
    "RDFList",
    "Requirement",
    "ask_rdf",
    "build_connectivity",
    "compare_schemas",
    "discover_all_graphs",
    "discover_void_graphs",
    "discover_void_source",
    "enrich_source",
    "enrich_source_with_bioregistry",
    "execute_sparql",
    "export_schema_artifacts",
    "get_bioregistry_metadata",
    "load_sources",
    "mine_schema",
    "query_metadata",
]


def __getattr__(name: str) -> Any:
    if name == "ask_rdf":
        from rdfsolve.mcp.workflow import ask_rdf

        return ask_rdf
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
