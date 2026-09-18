"""Class-level mapping assertions from external mapping sources."""

from __future__ import annotations

from pydantic import BaseModel, Field

SKOS_NARROW_MATCH = "http://www.w3.org/2004/02/skos/core#narrowMatch"


class MappingEdge(BaseModel):
    """A single mapping edge between two classes."""

    source_class: str = Field(
        ...,
        description="URI of the source class",
    )
    target_class: str = Field(
        ...,
        description="URI of the target class",
    )
    predicate: str = Field(
        SKOS_NARROW_MATCH,
        description=("Mapping predicate URI (default: skos:narrowMatch)"),
    )
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
