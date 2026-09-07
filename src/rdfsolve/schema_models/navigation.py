"""Keep schema-composed routes separate from observed instance paths."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.pattern import SchemaPattern


class NavigationPath(BaseModel):
    """A candidate route. Step counts do not establish end-to-end support."""

    steps: list[SchemaPattern] = Field(min_length=2, max_length=6)
    evidence: Literal["schema_composed"] = "schema_composed"
    instance_support: Literal["not_checked"] = "not_checked"

    def property_path(self) -> PropertyPath:
        """Return the predicate sequence, without intermediate class filters."""
        return PropertyPath(
            operator="sequence",
            items=[
                PropertyPath(operator="predicate", iri=step.property_uri) for step in self.steps
            ],
        )


class NavigationSummary(BaseModel):
    """Bounded samples and exact walk counts for the supplied schema graph."""

    max_hops: int = Field(ge=2, le=6)
    max_paths_per_length: int = Field(ge=0)
    edge_count: int = Field(ge=0)
    walk_counts: dict[int, int]
    paths: list[NavigationPath] = Field(default_factory=list)
    truncated_lengths: list[int] = Field(default_factory=list)
