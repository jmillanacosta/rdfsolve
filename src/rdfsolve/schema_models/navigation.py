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
    instance_support: Literal["not_checked", "matched", "no_match", "timeout", "error"] = (
        "not_checked"
    )
    source_count: int | None = Field(
        default=None, ge=0, description="Distinct focus nodes in the union of data graphs"
    )
    matched_sources: int | None = Field(default=None, ge=0)
    min_count: int | None = Field(default=None, ge=0)
    max_count: int | None = Field(default=None, ge=0)
    graph_uris: list[str] = Field(default_factory=list)
    type_context_graph_uris: list[str] = Field(default_factory=list)
    query: str | None = None
    observed_at: str | None = None
    error: str | None = None

    def signature(self) -> tuple[tuple[str, str, str, str | None], ...]:
        """Identify route structure independently of labels and observed counts."""
        return tuple(
            (s.subject_class, s.property_uri, s.object_class, s.datatype) for s in self.steps
        )

    def label(self) -> str:
        """Describe the endpoints and intermediate classes using retained labels."""
        first, last = self.steps[0], self.steps[-1]
        start = first.subject_label or first.subject_class
        target = last.object_label or last.datatype or last.object_class
        via = ", ".join(s.object_label or s.object_class for s in self.steps[:-1])
        return f"{start} → {target} via {via}"

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
    omitted_by_class: dict[int, dict[str, int]] = Field(default_factory=dict)
    probe_limit: int = Field(default=0, ge=0)
    probe_selection: Literal["retained_prefix", "explicit"] = "retained_prefix"
