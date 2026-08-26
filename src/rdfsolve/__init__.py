"""RDF schema mining and LOD analysis toolkit."""

from .api import (
    discover_void_graphs,
    discover_void_source,
    enrich_source_with_bioregistry,
    execute_sparql,
    export_schema_artifacts,
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
from .sources import classify_source_mode
from .version import VERSION
from .void_discover import VoidParser

__all__ = [
    "VERSION",
    "AboutMetadata",
    "ClassIndex",
    "ClassPair",
    "EntityClassInfo",
    "Mapping",
    "MappingEdge",
    "MinedSchema",
    "QueryResult",
    "ResultCell",
    "SchemaMiner",
    "SchemaPattern",
    "VoidParser",
    "classify_source_mode",
    "derive_class_mappings",
    "discover_void_graphs",
    "discover_void_source",
    "enrich_source_with_bioregistry",
    "execute_sparql",
    "export_schema_artifacts",
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
]
