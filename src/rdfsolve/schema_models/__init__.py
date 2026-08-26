"""Schema models - public API re-exports."""

from __future__ import annotations

import importlib

from rdfsolve.schema_models._constants import (
    _BASE_URI,
    _BLANK_NODE_URIS,
    _GRAPH_SKIP_KEYS,
    _RESOURCE_URIS,
    _SENTINEL_OBJECTS,
    _URI_SCHEMES,
    SERVICE_NAMESPACE_PREFIXES,
)
from rdfsolve.schema_models.core import (
    AboutMetadata,
    MinedSchema,
    PatternType,
    SchemaPattern,
    _merge_into_list,
    _object_value_and_key,
    _parse_schema_entry,
    _parse_schema_graph,
)
from rdfsolve.schema_models.rdfconfig import to_rdfconfig
from rdfsolve.schema_models.report import (
    MiningReport,
    OneShotQueryResult,
    PhaseReport,
    QueryStats,
)

# Names that should be resolved lazily via __getattr__
_LAZY_LINKML = {
    "make_valid_linkml_name": "rdfsolve.schema_models.linkml",
    "to_linkml": "rdfsolve.schema_models.linkml",
    "to_linkml_yaml": "rdfsolve.schema_models.linkml",
}

_LAZY_SHACL = {
    "to_shacl": "rdfsolve.schema_models.shacl",
}


def __getattr__(name: str) -> object:
    """Lazily import LinkML/SHACL-dependent symbols on first access."""
    if name in _LAZY_LINKML:
        module = importlib.import_module(_LAZY_LINKML[name])
        return getattr(module, name)
    if name in _LAZY_SHACL:
        module = importlib.import_module(_LAZY_SHACL[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # constants
    "SERVICE_NAMESPACE_PREFIXES",
    "_BASE_URI",
    "_BLANK_NODE_URIS",
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
    "PatternType",
    "PhaseReport",
    "QueryStats",
    "SchemaPattern",
    "_merge_into_list",
    "_object_value_and_key",
    "_parse_schema_entry",
    "_parse_schema_graph",
    # linkml
    "make_valid_linkml_name",
    "to_linkml",
    "to_linkml_yaml",
    # conversions
    "to_rdfconfig",
    # shacl
    "to_shacl",
]
