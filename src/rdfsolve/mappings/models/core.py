"""Class-level mapping assertions from external mapping sources."""

from __future__ import annotations

from pydantic import BaseModel, Field


class MappingEdge(BaseModel):
    """One mapping assertion read from an external mapping source.

    source_class and target_class hold the mapped terms: classes for class
    mappings, entities for entity mappings. predicate is the asserted relation
    and has no default.
    """

    source_class: str = Field(
        ...,
        description="URI of the source class",
    )
    target_class: str = Field(
        ...,
        description="URI of the target class",
    )
    predicate: str = Field(..., min_length=1, description="Asserted mapping predicate IRI")
    source_dataset: str = Field(
        ...,
        description=("Dataset name for source_class"),
    )
    target_dataset: str = Field(
        ...,
        description=("Dataset name for target_class"),
    )
    source_endpoint: str | None = Field(None)
    target_endpoint: str | None = Field(None)
    source_uri_format: str | None = Field(
        None,
        description="URI namespace prefix that was actually matched for source_class",
    )
    target_uri_format: str | None = Field(
        None,
        description="URI namespace prefix that was actually matched for target_class",
    )
    mapping_source: str | None = Field(None, description="Original mapping set or file identifier")
    confidence: float | None = Field(
        None,
        ge=0,
        le=1,
        description="Optional match confidence score 0-1",
    )
    mapping_justification: str | None = Field(
        None,
        description="Mapping justification URI (e.g., semapv:ManualMappingCuration, semapv:LexicalMatching)",
    )
