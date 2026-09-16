"""Mapping models - public API re-exports."""

from rdfsolve.mappings.models.class_derived import ClassDerivedMapping
from rdfsolve.mappings.models.core import (
    SKOS_NARROW_MATCH,
    InstanceMatchResult,
    Mapping,
    MappingEdge,
)
from rdfsolve.mappings.models.inference import (
    InferencedMapping,
)
from rdfsolve.mappings.models.instance import (
    InstanceMapping,
    merge_instance_jsonld,
)
from rdfsolve.mappings.models.semra import SemraMapping
from rdfsolve.mappings.models.sssom import SsomMapping
from rdfsolve.schema_models.about import AboutMetadata

__all__ = [
    "SKOS_NARROW_MATCH",
    "AboutMetadata",
    "ClassDerivedMapping",
    "InferencedMapping",
    "InstanceMapping",
    "InstanceMatchResult",
    "Mapping",
    "MappingEdge",
    "SemraMapping",
    "SsomMapping",
    "merge_instance_jsonld",
]
