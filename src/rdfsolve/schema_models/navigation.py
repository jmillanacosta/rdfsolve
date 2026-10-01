"""Keep schema-composed routes separate from paths tested on the data."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_serializer, model_validator

from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.pattern import SchemaPattern


class NavigationPath(BaseModel):
    """A route. A schema-composed route is a candidate; an instance-tested route was followed by
    at least one instance of its start class (matched_sources of them).
    """

    steps: list[SchemaPattern] = Field(min_length=2, max_length=6)
    evidence: Literal["schema_composed", "instance_tested"] = "schema_composed"
    instance_support: Literal[
        "not_checked", "matched", "no_match", "no_sources", "timeout", "error"
    ] = "not_checked"
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
    # Paths tested on the data (strategy "tested"): only matched paths are kept in paths.
    strategy: Literal["schema_sample", "tested"] = "schema_sample"
    tested_by_length: dict[int, int] = Field(default_factory=dict)
    matched_by_length: dict[int, int] = Field(default_factory=dict)
    complete_lengths: list[int] = Field(default_factory=list)
    budget_s: float | None = Field(default=None, ge=0)
    stop_reason: Literal["budget"] | None = None
    query_count: int = Field(default=0, ge=0)
    failed_queries: int = Field(default=0, ge=0)
    # Member terms of the groups of ontology terms that kept paths go through (for their queries)
    member_terms: dict[str, list[str]] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _read_packed_paths(cls, data: Any) -> Any:
        """Read tested paths written as edge numbers (see _write_packed_paths)."""
        if not isinstance(data, dict) or not isinstance(data.get("paths"), dict):
            return data
        packed = data["paths"]
        edges = packed.get("edges", [])
        shared = {
            "evidence": "instance_tested",
            "instance_support": "matched",
            "graph_uris": packed.get("graph_uris", []),
            "type_context_graph_uris": packed.get("type_context_graph_uris", []),
        }
        rows = [
            {
                **shared,
                "steps": [edges[i] for i in row["edges"]],
                "matched_sources": row.get("matched"),
                "source_count": row.get("sources"),
                "observed_at": row.get("at"),
            }
            for row in packed.get("rows", [])
        ]
        return {**data, "paths": rows}

    @model_serializer(mode="wrap")
    def _write_packed_paths(self, handler: Any) -> Any:
        """Write tested paths compactly: each step once in a table, each path as edge numbers.

        A path keeps its matched and start instance counts and the time of its test. Its query
        is not written: rdfsolve.mining.navigation.support_query makes it from the steps. The
        routes of a schema sample (strategy schema_sample) are written in full, as before.
        """
        data = handler(self)
        if self.strategy != "tested" or not isinstance(data, dict) or "paths" not in data:
            return data  # a dump without the paths (exclude) has nothing to pack
        numbers: dict[tuple[str, str, str, str | None], int] = {}
        edges: list[Any] = []
        rows: list[dict[str, Any]] = []
        for path, written in zip(self.paths, data["paths"], strict=True):
            ids = []
            for step, step_written in zip(path.steps, written["steps"], strict=True):
                key = (step.subject_class, step.property_uri, step.object_class, step.datatype)
                if key not in numbers:
                    numbers[key] = len(edges)
                    edges.append(step_written)
                ids.append(numbers[key])
            rows.append(
                {
                    "edges": ids,
                    "matched": path.matched_sources,
                    "sources": path.source_count,
                    "at": path.observed_at,
                }
            )
        first = self.paths[0] if self.paths else None
        data["paths"] = {
            "format": "edges-1",
            "edges": edges,
            "rows": rows,
            "graph_uris": list(first.graph_uris) if first else [],
            "type_context_graph_uris": list(first.type_context_graph_uris) if first else [],
        }
        return data
