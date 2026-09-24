"""Graph-scoped observations of RDF collections."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class CollectionProfile(BaseModel):
    """Observed list members and lengths for one class, predicate and graph."""

    model_config = ConfigDict(extra="forbid")

    subject_class: str
    property_uri: str
    graph_uri: str | None = None
    member_types: list[str] = Field(default_factory=list)
    member_kinds: list[Literal["IRI", "BlankNode", "Literal"]] = Field(default_factory=list)
    member_datatypes: list[str] = Field(default_factory=list)
    member_languages: list[str] = Field(default_factory=list)
    min_length: int | None = Field(None, ge=0)
    max_length: int | None = Field(None, ge=0)
    list_count: int = Field(0, ge=0)
    invalid_count: int = Field(0, ge=0)
