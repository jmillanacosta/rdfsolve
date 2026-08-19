"""Connection models for LOD cloud relationship discovery and provenance tracking."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class ConnectionType(str, Enum):
    """Semantic type of a connection between LOD cloud entities.

    Connections are organized into three levels:

    **Schema-level (vocabulary/ontology):**
    - vocabulary_reuse: Same predicate used across datasets
    - class_equivalence: owl:equivalentClass relationship
    - class_hierarchy: rdfs:subClassOf relationship
    - property_equivalence: owl:equivalentProperty relationship
    - property_hierarchy: rdfs:subPropertyOf relationship
    - namespace_import: owl:imports relationship

    **Instance-level:**
    - instance_identity: Same entity appears in multiple datasets
    - instance_relationship: Cross-dataset triple linking entities

    **Derived (inferred from evidence):**
    - inferred_class: Class mapping derived from instance patterns
    - inferred_property: Property mapping derived from usage patterns
    """

    # Schema-level (vocabulary/ontology)
    VOCABULARY_REUSE = "vocabulary_reuse"
    """Same predicate URI used in multiple datasets."""

    CLASS_EQUIVALENCE = "class_equivalence"
    """owl:equivalentClass relationship between classes."""

    CLASS_HIERARCHY = "class_hierarchy"
    """rdfs:subClassOf relationship between classes."""

    PROPERTY_EQUIVALENCE = "property_equivalence"
    """owl:equivalentProperty relationship between properties."""

    PROPERTY_HIERARCHY = "property_hierarchy"
    """rdfs:subPropertyOf relationship between properties."""

    NAMESPACE_IMPORT = "namespace_import"
    """owl:imports relationship between ontologies."""

    # Instance-level
    INSTANCE_IDENTITY = "instance_identity"
    """Same entity (URI) appears in multiple datasets."""

    INSTANCE_RELATIONSHIP = "instance_relationship"
    """Cross-dataset triple linking entities from different datasets."""

    # Derived
    INFERRED_CLASS_MAPPING = "inferred_class"
    """Class mapping derived from instance co-occurrence patterns."""

    INFERRED_PROPERTY_MAPPING = "inferred_property"
    """Property mapping derived from usage pattern analysis."""


# Type alias for evidence source types
EvidenceSourceType = Literal[
    "schema_mining",
    "sssom_mapping",
    "semra_mapping",
    "uri_matching",
    "instance_matching",
    "class_derivation",
]


class EvidenceSource(BaseModel):
    """Provenance record for a connection discovery.

    Each connection can have multiple evidence sources that
    independently support its existence. This allows aggregating
    evidence from schema mining, external mappings, and instance
    analysis.
    """

    source_type: EvidenceSourceType = Field(
        ...,
        description=(
            "How this evidence was discovered: "
            "'schema_mining' (found via schema overlap), "
            "'sssom_mapping' (from SSSOM file), "
            "'semra_mapping' (from SeMRA), "
            "'uri_matching' (same URI in multiple datasets), "
            "'instance_matching' (entity resolution), "
            "'class_derivation' (inferred from instance patterns)"
        ),
    )
    source_file: str | None = Field(
        None,
        description="Path or URI of the source file (for imported mappings)",
    )
    evidence_count: int = Field(
        default=1,
        ge=0,
        description="Number of observations supporting this evidence",
    )
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Confidence score for this evidence source (0.0-1.0)",
    )
    details: dict[str, Any] | None = Field(
        None,
        description="Additional source-specific metadata",
    )


class Connection(BaseModel):
    """A discovered relationship between entities in the LOD cloud.

    Represents a connection between two entities (classes, properties,
    or instances) across datasets. Connections have:

    - **Identity**: What entities are connected
    - **Semantics**: What kind of connection this is
    - **Evidence**: How the connection was discovered
    - **Quality**: Aggregated confidence score
    - **Provenance**: When and how it was discovered

    Example
    -------
    >>> conn = Connection(
    ...     source_dataset="uniprot",
    ...     source_entity="http://purl.uniprot.org/core/Protein",
    ...     target_dataset="chebi",
    ...     target_entity="http://purl.obolibrary.org/obo/CHEBI_36080",
    ...     connection_type=ConnectionType.CLASS_EQUIVALENCE,
    ...     evidence_sources=[
    ...         EvidenceSource(source_type="sssom_mapping", source_file="uniprot-chebi.sssom.tsv")
    ...     ],
    ...     confidence=0.95,
    ... )
    """

    # Identity: What's connected ---
    source_dataset: str = Field(
        ...,
        description="Name/identifier of the source dataset",
    )
    source_entity: str = Field(
        ...,
        description="URI of the source entity (class, property, or instance)",
    )
    target_dataset: str = Field(
        ...,
        description="Name/identifier of the target dataset",
    )
    target_entity: str = Field(
        ...,
        description="URI of the target entity (class, property, or instance)",
    )

    # Semantics: Connection type ---
    connection_type: ConnectionType = Field(
        ...,
        description="Semantic type of this connection",
    )
    predicate: str | None = Field(
        None,
        description="The linking predicate URI if applicable (e.g., owl:equivalentClass)",
    )

    # Evidence chain ---
    evidence_sources: list[EvidenceSource] = Field(
        default_factory=list,
        description="Evidence sources supporting this connection",
    )

    # Aggregated quality ---
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Aggregated confidence score across all evidence (0.0-1.0)",
    )
    instance_count: int = Field(
        default=0,
        ge=0,
        description="Number of supporting observations/instances",
    )

    # Provenance ---
    discovered_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="ISO-8601 timestamp when this connection was discovered",
    )
    discovered_by: str = Field(
        default="unknown",
        description="Pipeline stage or tool that discovered this connection",
    )

    def add_evidence(self, evidence: EvidenceSource) -> None:
        """Add an evidence source and update aggregated confidence."""
        self.evidence_sources.append(evidence)
        self._update_aggregated_confidence()

    def _update_aggregated_confidence(self) -> None:
        """Recalculate aggregated confidence from all evidence sources.

        Uses a simple weighted average based on evidence counts.
        """
        if not self.evidence_sources:
            self.confidence = 0.0
            return

        total_weight = sum(e.evidence_count for e in self.evidence_sources)
        if total_weight == 0:
            self.confidence = sum(e.confidence for e in self.evidence_sources) / len(
                self.evidence_sources
            )
        else:
            weighted_sum = sum(e.confidence * e.evidence_count for e in self.evidence_sources)
            self.confidence = weighted_sum / total_weight

    @property
    def total_evidence_count(self) -> int:
        """Sum of evidence counts across all sources."""
        return sum(e.evidence_count for e in self.evidence_sources)


__all__ = [
    "Connection",
    "ConnectionType",
    "EvidenceSource",
    "EvidenceSourceType",
]
