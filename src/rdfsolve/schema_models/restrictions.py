"""Relations that an ontology states between its terms with OWL restrictions."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# The filler of a restriction that is a class expression (a blank node), not a named class.
CLASS_EXPRESSION = "(class expression)"


class RestrictionPattern(BaseModel):
    """Terms of one namespace that are related to fillers of one namespace by one property.

    A class is a subclass of, or equivalent to an intersection with, a restriction on
    property_uri. The terms are grouped by the namespace of their IRI. count is the number of
    restrictions, classes the number of distinct subject terms.

    evidence is asserted for an axiom of the ontology, and materialized for a relation that a
    reasoner stored as a direct edge: X R Y for X SubClassOf (R some Y), and X rdfs:subClassOf
    Y (form named) for a named superclass (UberGraph relation graphs).
    """

    model_config = ConfigDict(extra="forbid")

    subject_namespace: str
    axiom: Literal["SubClassOf", "EquivalentTo"]
    property_uri: str
    form: Literal["some", "only", "value", "named"]
    filler_namespace: str
    count: int = Field(ge=0)
    classes: int = Field(ge=0)
    example_subject: str | None = None
    example_filler: str | None = None
    label: str = Field(description="The pattern in Manchester syntax, with source labels")
    evidence: Literal["asserted", "materialized"] = "asserted"
    graph_uri: str | None = Field(None, description="The graph of a materialized pattern")


class RestrictionPatterns(BaseModel):
    """The restriction patterns of a dataset and the state of their mining."""

    model_config = ConfigDict(extra="forbid")

    state: Literal["complete", "partial", "failed"] = "complete"
    patterns: list[RestrictionPattern] = Field(default_factory=list)
    query_count: int = Field(default=0, ge=0)
    failures: list[str] = Field(default_factory=list)
