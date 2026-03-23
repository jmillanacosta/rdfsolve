"""Mapping models - public API re-exports."""

from rdfsolve.mapping_models.class_derived import ClassDerivedMapping
from rdfsolve.mapping_models.core import (
    SKOS_NARROW_MATCH,
    InstanceMatchResult,
    Mapping,
    MappingEdge,
)
from rdfsolve.mapping_models.inference import (
    InferencedMapping,
)
from rdfsolve.mapping_models.instance import (
    InstanceMapping,
    merge_instance_jsonld,
)
from rdfsolve.mapping_models.semra import SemraMapping
from rdfsolve.mapping_models.sssom import SsomMapping
from rdfsolve.schema_models.core import AboutMetadata

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
