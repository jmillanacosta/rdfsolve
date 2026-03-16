"""Schema models - public API re-exports."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

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

# rdfconfig has no heavy dependencies, safe to import eagerly.
from rdfsolve.schema_models.rdfconfig import to_rdfconfig
from rdfsolve.schema_models.report import (
    MiningReport,
    OneShotQueryResult,
    PhaseReport,
    QueryStats,
)

# LinkML-dependent modules (linkml.py, shacl.py) are loaded lazily because
# `linkml.__init__` can crash on certain Python / linkml version combinations
# (e.g. ``AttributeError: JSON`` on Python 3.10).  The symbols are still
# importable via ``from rdfsolve.schema_models import to_linkml`` etc.,
# they are resolved on first access through ``__getattr__``.

if TYPE_CHECKING:
    from rdfsolve.schema_models.linkml import (
        make_valid_linkml_name as make_valid_linkml_name,
    )
    from rdfsolve.schema_models.linkml import (
        to_linkml as to_linkml,
    )
    from rdfsolve.schema_models.linkml import (
        to_linkml_yaml as to_linkml_yaml,
    )
    from rdfsolve.schema_models.shacl import to_shacl as to_shacl

# Names that should be resolved lazily via __getattr__
_LAZY_LINKML = {
    "make_valid_linkml_name": "rdfsolve.schema_models.linkml",
    "to_linkml": "rdfsolve.schema_models.linkml",
    "to_linkml_yaml": "rdfsolve.schema_models.linkml",
    "to_shacl": "rdfsolve.schema_models.shacl",
}


def __getattr__(name: str) -> object:
    """Lazily import LinkML-dependent symbols on first access."""
    if name in _LAZY_LINKML:
        module = importlib.import_module(_LAZY_LINKML[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
