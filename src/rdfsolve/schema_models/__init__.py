"""Schema models - public API re-exports."""

from rdfsolve.schema_models._constants import (
    _BASE_URI,
    _GRAPH_SKIP_KEYS,
    _RESOURCE_URIS,
    _SENTINEL_OBJECTS,
    _URI_SCHEMES,
    SERVICE_NAMESPACE_PREFIXES,
)
from rdfsolve.schema_models.core import (
    AboutMetadata,
    MinedSchema,
    SchemaPattern,
    _merge_into_list,
    _object_value_and_key,
    _parse_schema_entry,
    _parse_schema_graph,
)
from rdfsolve.schema_models.linkml import (
    make_valid_linkml_name,
    to_linkml,
    to_linkml_yaml,
)
from rdfsolve.schema_models.rdfconfig import to_rdfconfig
from rdfsolve.schema_models.report import (
    MiningReport,
    OneShotQueryResult,
    PhaseReport,
    QueryStats,
)
from rdfsolve.schema_models.shacl import to_shacl

__all__ = [
    # constants
    "SERVICE_NAMESPACE_PREFIXES",
    "_BASE_URI",
    "_GRAPH_SKIP_KEYS",
    "_RESOURCE_URIS",
    "_SENTINEL_OBJECTS",
    "_URI_SCHEMES",
    # core
    "AboutMetadata",
    "MinedSchema",
    # report
    "MiningReport",
    "OneShotQueryResult",
    "PhaseReport",
    "QueryStats",
    "SchemaPattern",
    "_merge_into_list",
    "_object_value_and_key",
    "_parse_schema_entry",
    "_parse_schema_graph",
    # conversions
    "make_valid_linkml_name",
    "to_linkml",
    "to_linkml_yaml",
    "to_rdfconfig",
    "to_shacl",
]
