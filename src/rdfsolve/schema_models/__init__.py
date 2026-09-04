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
    MiningResult,
    PatternType,
    SchemaPattern,
    _merge_into_list,
    _object_value_and_key,
    _parse_schema_entry,
    _parse_schema_graph,
)
from rdfsolve.schema_models.metadata import (
    DatasetDescription,
    MetadataPatterns,
    ServiceDescription,
)
from rdfsolve.schema_models.ontology import (
    DomainAssertion,
    InverseRelation,
    OntologyStructure,
    PropertyCharacteristic,
    RangeAssertion,
    Restriction,
    SubClassRelation,
)
from rdfsolve.schema_models.rdfconfig import to_rdfconfig
from rdfsolve.schema_models.report import (
    MiningReport,
    OneShotQueryResult,
    PhaseReport,
    QueryStats,
)
from rdfsolve.schema_models.shacl_convert import (
    minedschema_to_shacl,
    shacl_to_minedschema,
)
from rdfsolve.schema_models.shacl_model import (
    ShaclNodeShape,
    ShaclPropertyShape,
    ShaclShapesGraph,
)
from rdfsolve.schema_models.void_convert import (
    minedschema_to_void,
    void_to_minedschema,
)
from rdfsolve.schema_models.void_model import (
    VoidClassPartition,
    VoidDataset,
    VoidDatasetDescription,
    VoidDatatypePartition,
    VoidLinkset,
    VoidPropertyPartition,
)

# Names that should be resolved lazily via __getattr__
_LAZY_LINKML = {
    "make_valid_linkml_name": "rdfsolve.schema_models.linkml",
    "to_linkml": "rdfsolve.schema_models.linkml",
    "to_linkml_yaml": "rdfsolve.schema_models.linkml",
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
    "_BLANK_NODE_URIS",
    "_GRAPH_SKIP_KEYS",
    "_RESOURCE_URIS",
    "_SENTINEL_OBJECTS",
    "_URI_SCHEMES",
    # core
    "AboutMetadata",
    # metadata
    "DatasetDescription",
    # ontology
    "DomainAssertion",
    "InverseRelation",
    "MetadataPatterns",
    "MinedSchema",
    # report
    "MiningReport",
    "MiningResult",
    "OneShotQueryResult",
    "OntologyStructure",
    "PatternType",
    "PhaseReport",
    "PropertyCharacteristic",
    "QueryStats",
    "RangeAssertion",
    "Restriction",
    "SchemaPattern",
    "ServiceDescription",
    "SubClassRelation",
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
    # shacl models
    "ShaclNodeShape",
    "ShaclPropertyShape",
    "ShaclShapesGraph",
    "minedschema_to_shacl",
    "shacl_to_minedschema",
    # void models
    "VoidClassPartition",
    "VoidDataset",
    "VoidDatasetDescription",
    "VoidDatatypePartition",
    "VoidLinkset",
    "VoidPropertyPartition",
    "minedschema_to_void",
    "void_to_minedschema",
]
