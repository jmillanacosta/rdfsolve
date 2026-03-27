"""RDFSolve: A library for RDF schema analysis and VoID generation.

Main modules:
- api: High-level API functions (all re-exported here)
- parser: VoidParser class for parsing VoID descriptions and schemas
- miner: SchemaMiner class for direct SPARQL schema mining
- query: SPARQL query execution with structured results
- iri: IRI resolution against SPARQL endpoints
- compose: SPARQL query composition from diagram paths
"""

from .api import (
    compose_query_from_paths,
    enrich_source_with_bioregistry,
    execute_sparql,
    get_bioregistry_metadata,
    graph_to_jsonld,
    graph_to_linkml,
    graph_to_schema,
    graph_to_shacl,
    import_semra_source,
    import_sssom_source,
    infer_mappings,
    load_mapping_jsonld,
    load_parser_from_file,
    load_parser_from_graph,
    load_parser_from_jsonld,
    mine_all_sources,
    mine_schema,
    probe_instance_mapping,
    resolve_iris,
    seed_inferenced_mappings,
    seed_instance_mappings,
    seed_semra_mappings,
    seed_sssom_mappings,
    sources_to_jsonld,
    to_jsonld_from_file,
    to_linkml_from_file,
    to_rdfconfig_from_file,
    to_shacl_from_file,
    to_void_from_file,
)
from .miner import SchemaMiner
from .models import (
    AboutMetadata,
    Mapping,
    MappingEdge,
    MinedSchema,
    SchemaPattern,
)
from .parser import VoidParser
from .query import QueryResult, ResultCell
from .version import VERSION

__all__ = [
    # ── version ──────────────────────────────────────────────────
    "VERSION",
    # ── models ───────────────────────────────────────────────────
    "AboutMetadata",
    "Mapping",
    "MappingEdge",
    "MinedSchema",
    "QueryResult",
    "ResultCell",
    "SchemaMiner",
    "SchemaPattern",
    "VoidParser",
    # ── api: SPARQL / IRI ────────────────────────────────────────
    "compose_query_from_paths",
    # ── api: bioregistry enrichment ──────────────────────────────
    "enrich_source_with_bioregistry",
    "execute_sparql",
    "get_bioregistry_metadata",
    # ── api: conversion / export ─────────────────────────────────
    "graph_to_jsonld",
    "graph_to_linkml",
    "graph_to_schema",
    "graph_to_shacl",
    "import_semra_source",
    "import_sssom_source",
    "infer_mappings",
    "load_mapping_jsonld",
    # ── api: parsing / loading ───────────────────────────────────
    "load_parser_from_file",
    "load_parser_from_graph",
    "load_parser_from_jsonld",
    # ── api: mining ──────────────────────────────────────────────
    "mine_all_sources",
    "mine_schema",
    # ── api: mappings ────────────────────────────────────────────
    "probe_instance_mapping",
    "resolve_iris",
    "seed_inferenced_mappings",
    "seed_instance_mappings",
    "seed_semra_mappings",
    "seed_sssom_mappings",
    "sources_to_jsonld",
    "to_jsonld_from_file",
    "to_linkml_from_file",
    "to_rdfconfig_from_file",
    "to_shacl_from_file",
    "to_void_from_file",
]
