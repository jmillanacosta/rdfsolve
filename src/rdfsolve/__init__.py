"""RDF schema mining and LOD analysis toolkit."""

import logging

from .api import (
    discover_all_graphs,
    discover_void_graphs,
    discover_void_source,
    enrich_source,
    enrich_source_with_bioregistry,
    execute_sparql,
    export_schema_artifacts,
    extract_metadata_from_void_graphs,
    get_bioregistry_metadata,
    graph_to_jsonld,
    graph_to_schema,
    load_mapping_jsonld,
    load_parser_from_file,
    load_parser_from_graph,
    load_parser_from_jsonld,
    load_sources,
    mine_schema,
    query_metadata,
    resolve_void_uri_base,
    sources_to_jsonld,
    to_jsonld_from_file,
    to_rdfconfig_from_file,
    to_void_from_file,
)
from .class_derivation import ClassPair, derive_class_mappings

# Inference modules
from .class_index import ClassIndex, EntityClassInfo
from .hydration import HydrationLimitError, Hydrator
from .instance_matcher import probe_endpoint, probe_resource
from .miner import SchemaMiner
from .models import (
    AboutMetadata,
    Mapping,
    MappingEdge,
    MinedSchema,
    SchemaPattern,
)
from .query import QueryResult, ResultCell
from .schema_models.void_schema import VoidSchema
from .sources import classify_source_mode
from .sources_updater import update_multiple_sources, update_sources_yaml_with_graphs
from .version import VERSION
from .void_discover import VoidParser

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "VERSION",
    "AboutMetadata",
    "ClassIndex",
    "ClassPair",
    "EntityClassInfo",
    "HydrationLimitError",
    "Hydrator",
    "Mapping",
    "MappingEdge",
    "MinedSchema",
    "QueryResult",
    "ResultCell",
    "SchemaMiner",
    "SchemaPattern",
    "VoidParser",
    "VoidSchema",
    "classify_source_mode",
    "derive_class_mappings",
    "discover_all_graphs",
    "discover_void_graphs",
    "discover_void_source",
    "enrich_source",
    "enrich_source_with_bioregistry",
    "execute_sparql",
    "export_schema_artifacts",
    "extract_metadata_from_void_graphs",
    "get_bioregistry_metadata",
    "graph_to_jsonld",
    "graph_to_schema",
    "load_mapping_jsonld",
    "load_parser_from_file",
    "load_parser_from_graph",
    "load_parser_from_jsonld",
    "load_sources",
    "mine_schema",
    "probe_endpoint",
    "probe_resource",
    "query_metadata",
    "resolve_void_uri_base",
    "sources_to_jsonld",
    "to_jsonld_from_file",
    "to_rdfconfig_from_file",
    "to_void_from_file",
    "update_multiple_sources",
    "update_sources_yaml_with_graphs",
]
