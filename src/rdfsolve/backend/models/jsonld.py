"""Pydantic models for JSON-LD data contracts.

These models define the exact data shapes the frontend produces and
consumes. The backend must read and write these formats precisely.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class AboutSection(BaseModel):
    """Metadata embedded in the JSON-LD under @about."""

    generated_by: str = ""
    generated_at: str = ""
    endpoint: str = ""
    endpoints: list[str] = Field(default_factory=list)
    dataset_name: str = ""
    pattern_count: int = 0
    strategy: str = "miner"


class JSONLDNode(BaseModel):
    """A single node in @graph. Arbitrary predicates allowed."""

    id: str = Field(alias="@id")
    type: str | list[str] | None = Field(default=None, alias="@type")

    model_config = {"extra": "allow", "populate_by_name": True}


class JSONLDSchema(BaseModel):
    """Complete JSON-LD schema document."""

    context: dict[str, str] = Field(alias="@context")
    graph: list[JSONLDNode] = Field(
        default_factory=list, alias="@graph",
    )
    about: AboutSection | None = Field(default=None, alias="@about")

    model_config = {"extra": "allow", "populate_by_name": True}


class ServiceTarget(BaseModel):
    """sd:Service target in SPARQL Executable JSON-LD."""

    type: str = Field(default="sd:Service", alias="@type")
    endpoint: str = Field(alias="sd:endpoint")

    model_config = {"populate_by_name": True}


class SPARQLExecutableJSONLD(BaseModel):
    """A sh:SPARQLExecutable JSON-LD document."""

    context: dict[str, str] = Field(alias="@context")
    id: str = Field(alias="@id")
    type: list[str] = Field(alias="@type")

    sh_select: str | None = Field(default=None, alias="sh:select")
    sh_construct: str | None = Field(
        default=None, alias="sh:construct",
    )
    sh_ask: str | None = Field(default=None, alias="sh:ask")

    sh_prefixes: dict[str, str] | None = Field(
        default=None, alias="sh:prefixes",
    )
    date_created: str | None = Field(
        default=None, alias="schema:dateCreated",
    )
    description: str | None = Field(
        default=None, alias="schema:description",
    )
    target: ServiceTarget | None = Field(
        default=None, alias="schema:target",
    )

    model_config = {"populate_by_name": True, "extra": "allow"}
