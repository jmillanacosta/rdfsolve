"""Observed edges between records with stated outgoing properties."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from rdfsolve.schema_models.paths import PropertyPath


class StructuralPattern(BaseModel):
    """An observed edge profile with explicit shape and subject-selection semantics."""

    model_config = ConfigDict(extra="forbid")

    subject_properties: list[str]
    subject_kind: Literal["IRI", "BlankNode"]
    property_uri: str
    object_properties: list[str] = Field(default_factory=list)
    object_kind: Literal["IRI", "BlankNode", "Literal"]
    datatype: str | None = None
    language: str | None = None
    graph_uri: str | None = None
    type_graph_uris: list[str] = Field(default_factory=list)
    subject_selection: Literal["all", "untyped"] = "all"
    shape_semantics: Literal["exact_property_sets", "property_profile"] = "exact_property_sets"
    count: int = Field(ge=0)
    distinct_subjects: int = Field(ge=0)
    distinct_objects: int = Field(ge=0)
    evidence_source: Literal["mined"] = "mined"
    scope_semantics: Literal["single_graph"] = "single_graph"
    witness_query: str
    recount_query: str
    examples: list[dict[str, dict[str, str]]] = Field(default_factory=list)

    @field_validator("subject_properties", "object_properties", "type_graph_uris")
    @classmethod
    def check_properties(cls, values: list[str]) -> list[str]:
        """Validate and sort the property set."""
        for value in values:
            PropertyPath(operator="predicate", iri=value)
        return sorted(set(values))

    @field_validator("property_uri", "datatype", "graph_uri")
    @classmethod
    def check_iri(cls, value: str | None) -> str | None:
        """Accept SPARQL-safe IRIs."""
        if value is not None:
            PropertyPath(operator="predicate", iri=value)
        return value
