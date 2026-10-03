"""Unified lightweight public models.

Core schema, mapping, endpoint and source models are re-exported eagerly. The
LinkML helpers depend on optional packages and resolve only on first access,
so importing this module does not import LinkML.
"""

from __future__ import annotations

from rdfsolve import schema_models as _schema_models
from rdfsolve._uri import (
    _build_br_prefix_map,
    _ns_from_uri,
    _prefix_from_ns,
    uri_to_curie,
)
from rdfsolve._uri import make_expander as _make_expander
from rdfsolve.mappings.models import MappingEdge
from rdfsolve.models.endpoint import Endpoint, EndpointHealth, EndpointStatus
from rdfsolve.models.source_model import PublicationRef, SourceModel, SourcesRegistry
from rdfsolve.schema_models import (
    _BLANK_NODE_URIS,
    _RESOURCE_URIS,
    _SENTINEL_OBJECTS,
    _URI_SCHEMES,
    SERVICE_NAMESPACE_PREFIXES,
    AboutMetadata,
    DisjointClassRelation,
    DomainAssertion,
    EquivalentClassRelation,
    EquivalentPropertyRelation,
    InverseRelation,
    MetadataDocument,
    MinedSchema,
    MiningReport,
    MiningResult,
    OneShotQueryResult,
    OntologyStructure,
    PatternType,
    PhaseReport,
    PropertyCharacteristic,
    QueryStats,
    RangeAssertion,
    Restriction,
    SchemaPattern,
    ShaclNodeShape,
    ShaclPropertyShape,
    ShaclShapesGraph,
    SubClassRelation,
    SubPropertyRelation,
    VoidClassPartition,
    VoidDataset,
    VoidDatasetDescription,
    VoidDatatypePartition,
    VoidLinkset,
    VoidPropertyPartition,
    minedschema_to_shacl,
    minedschema_to_void,
    shacl_to_minedschema,
    to_rdfconfig,
    void_to_minedschema,
)

_OPTIONAL_SCHEMA_EXPORTS = frozenset({"make_valid_linkml_name", "to_linkml", "to_linkml_yaml"})


def __getattr__(name: str) -> object:
    """Resolve the optional LinkML helpers on first access."""
    if name in _OPTIONAL_SCHEMA_EXPORTS:
        return getattr(_schema_models, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "SERVICE_NAMESPACE_PREFIXES",
    "_BLANK_NODE_URIS",
    "_RESOURCE_URIS",
    "_SENTINEL_OBJECTS",
    "_URI_SCHEMES",
    "AboutMetadata",
    "DisjointClassRelation",
    "DomainAssertion",
    "Endpoint",
    "EndpointHealth",
    "EndpointStatus",
    "EquivalentClassRelation",
    "EquivalentPropertyRelation",
    "InverseRelation",
    "MappingEdge",
    "MetadataDocument",
    "MinedSchema",
    "MiningReport",
    "MiningResult",
    "OneShotQueryResult",
    "OntologyStructure",
    "PatternType",
    "PhaseReport",
    "PropertyCharacteristic",
    "PublicationRef",
    "QueryStats",
    "RangeAssertion",
    "Restriction",
    "SchemaPattern",
    "ShaclNodeShape",
    "ShaclPropertyShape",
    "ShaclShapesGraph",
    "SourceModel",
    "SourcesRegistry",
    "SubClassRelation",
    "SubPropertyRelation",
    "VoidClassPartition",
    "VoidDataset",
    "VoidDatasetDescription",
    "VoidDatatypePartition",
    "VoidLinkset",
    "VoidPropertyPartition",
    "_build_br_prefix_map",
    "_make_expander",
    "_ns_from_uri",
    "_prefix_from_ns",
    "make_valid_linkml_name",
    "minedschema_to_shacl",
    "minedschema_to_void",
    "shacl_to_minedschema",
    "to_linkml",
    "to_linkml_yaml",
    "to_rdfconfig",
    "uri_to_curie",
    "void_to_minedschema",
]
