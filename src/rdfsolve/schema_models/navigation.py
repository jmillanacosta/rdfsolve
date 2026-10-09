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
    # "budget": the time budget was spent; "endpoint_cuts": the endpoint cut the queries of a
    # purpose at a fixed limit, and the search sent no more queries (cuts says which, why, and
    # how many queries were cut and not sent; rdfsolve.sparql_helper.QueryCuts).
    stop_reason: Literal["budget", "endpoint_cuts"] | None = None
    query_count: int = Field(default=0, ge=0)
    failed_queries: int = Field(default=0, ge=0)
    cuts: dict[str, dict[str, Any]] = Field(default_factory=dict)
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


def read_tested_paths(
    path: Any, start_classes: Any = None, max_hops: int | None = None
) -> list[NavigationPath]:
    """Read the tested paths of a saved schema, only those that start at *start_classes*.

    *path* is a schema JSON file (MinedSchema.to_json) whose navigation was tested on the data.
    The paths are kept packed in the file (each step once, each path as step numbers); only the
    paths whose first step starts at one of *start_classes* (class IRIs; all when None) and that
    have at most *max_hops* steps become NavigationPath objects, so a few classes' paths can be
    read without building all of them.
    """
    import json
    from pathlib import Path

    data = json.loads(Path(path).read_text(encoding="utf-8"))
    navigation = (data.get("schema", data) or {}).get("navigation") or {}
    packed = navigation.get("paths")
    if not isinstance(packed, dict):
        return []
    edges = packed.get("edges", [])
    wanted = None if start_classes is None else set(start_classes)
    shared = {
        "evidence": "instance_tested",
        "instance_support": "matched",
        "graph_uris": packed.get("graph_uris", []),
        "type_context_graph_uris": packed.get("type_context_graph_uris", []),
    }
    found = []
    for row in packed.get("rows", []):
        ids = row["edges"]
        if (max_hops is not None and len(ids) > max_hops) or (
            wanted is not None and edges[ids[0]].get("subject_class") not in wanted
        ):
            continue
        found.append(
            NavigationPath.model_validate(
                {
                    **shared,
                    "steps": [edges[i] for i in ids],
                    "matched_sources": row.get("matched"),
                    "source_count": row.get("sources"),
                    "observed_at": row.get("at"),
                }
            )
        )
    return found


def tested_step_support(
    path: Any, start_classes: Any = None
) -> dict[tuple[str, str], tuple[int, int]]:
    """Return, for each first step (class, property) of the tested paths of a saved schema, the
    most matched records of the paths that start with it, and their start records.

    Read from the packed paths without building them (read_tested_paths builds them); only
    paths that start at *start_classes* (all when None) count.
    """
    import json
    from pathlib import Path

    data = json.loads(Path(path).read_text(encoding="utf-8"))
    packed = ((data.get("schema", data) or {}).get("navigation") or {}).get("paths")
    if not isinstance(packed, dict):
        return {}
    edges = packed.get("edges", [])
    wanted = None if start_classes is None else set(start_classes)
    support: dict[tuple[str, str], tuple[int, int]] = {}
    for row in packed.get("rows", []):
        first = edges[row["edges"][0]]
        key = (str(first.get("subject_class")), str(first.get("property_uri")))
        if wanted is not None and key[0] not in wanted:
            continue
        matched, sources = row.get("matched") or 0, row.get("sources") or 0
        seen = support.get(key, (0, 0))
        if sources and matched / sources > (seen[0] / seen[1] if seen[1] else -1.0):
            support[key] = (matched, sources)
    return support


def step_support(paths: Any) -> dict[tuple[str, str], tuple[int, int]]:
    """Return, for each first step (class, property) of tested paths, the most matched records of
    the paths that start with it, and their start records (as tested_step_support, from paths).
    """
    support: dict[tuple[str, str], tuple[int, int]] = {}
    for route in paths:
        if route.instance_support != "matched" or not route.steps:
            continue
        step = route.steps[0]
        key = (step.subject_class, step.property_uri)
        matched, sources = route.matched_sources or 0, route.source_count or 0
        seen = support.get(key, (0, 0))
        if sources and matched / sources > (seen[0] / seen[1] if seen[1] else -1.0):
            support[key] = (matched, sources)
    return support
