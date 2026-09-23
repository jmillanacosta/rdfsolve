"""Deterministic validation plans derived from frozen release artifacts.

The plan is a release artifact in its own right. Execution is deliberately
separate because remote endpoints can drift after a corpus snapshot, whereas
local validation should run against the exact prepared index used for mining.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from rdfsolve.mining.query_builders import _graph_clause, _graph_scope, _type_pattern
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern

from .build import sha256_file
from .model import ReleaseArtifact, ReleaseManifest


class PatternSpotCheckPlan(BaseModel):
    """One exact empirical pattern selected for independent reproduction."""

    check_id: str
    dataset_id: str
    snapshot_id: str
    extraction_mode: str
    schema_artifact_id: str
    target_kind: Literal["remote_endpoint", "frozen_local_index", "unknown"]
    endpoint: str | None = None
    graph_scope: list[str] = Field(default_factory=list)
    type_graph_scope: list[str] = Field(default_factory=list)
    type_context_graph_scope: list[str] = Field(default_factory=list)
    pattern: dict[str, object]
    query: str


class RouteCheckPlan(BaseModel):
    """One stored route-probe query selected for reproduction."""

    check_id: str
    dataset_id: str
    snapshot_id: str
    extraction_mode: str
    schema_artifact_id: str
    target_kind: Literal["remote_endpoint", "frozen_local_index", "unknown"]
    endpoint: str | None = None
    expected_instance_support: str
    expected_source_count: int | None = None
    expected_matched_sources: int | None = None
    query: str


class ScientificValidationPlan(BaseModel):
    """Deterministic spot checks to execute against the corresponding snapshot source."""

    release_id: str
    patterns_per_schema: int
    routes_per_schema: int
    pattern_checks: list[PatternSpotCheckPlan] = Field(default_factory=list)
    route_checks: list[RouteCheckPlan] = Field(default_factory=list)
    skipped_checks: list[str] = Field(default_factory=list)


def _mode_from_path(path: str) -> str:
    name = Path(path).name
    for mode in ("remote", "local", "grouped"):
        if f"_{mode}_schema.json" in name or name.endswith(f"_{mode}.schema.json"):
            return mode
    return "unknown"


def _target_kind(mode: str) -> str:
    if mode == "remote":
        return "remote_endpoint"
    if mode in {"local", "grouped"}:
        return "frozen_local_index"
    return "unknown"


def _stable_id(*parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:24]
    return f"check:{digest}"


def pattern_existence_query(
    pattern: SchemaPattern,
    graph_scope: list[str],
    type_graph_scope: list[str] | None = None,
    type_context_graph_scope: list[str] | None = None,
) -> str:
    """Build a bounded existence query with the same selected-graph semantics as counts."""
    dataset, _, _ = _graph_scope(type_graph_scope or graph_scope, type_context_graph_scope)
    edge_open, edge_close = _graph_clause(graph_scope)
    subject = f"?s a <{pattern.subject_class}> ."
    edge = f"{edge_open} ?s <{pattern.property_uri}> ?o . {edge_close}"
    conditions: list[str] = []
    if pattern.object_class == "Literal":
        conditions.append("FILTER(isLiteral(?o))")
        if pattern.datatype:
            conditions.append(f"FILTER(DATATYPE(?o) = <{pattern.datatype}>)")
    elif pattern.object_class == "Resource":
        conditions.extend(
            [
                "FILTER(isIRI(?o))",
                f"FILTER NOT EXISTS {{ {_type_pattern('?o', '?_anyType', type_context_graph_scope)} }}",
            ]
        )
    elif pattern.object_class == "BlankNode":
        conditions.append("FILTER(isBlank(?o))")
    elif pattern.object_class not in _SENTINEL_OBJECTS:
        conditions.append(
            _type_pattern("?o", f"<{pattern.object_class}>", type_context_graph_scope)
        )
    where = " ".join([subject, edge, *conditions])
    return f"SELECT ?s ?o {dataset} WHERE {{ {where} }} LIMIT 1"


def _canonical_schema_artifacts(manifest: ReleaseManifest) -> list[ReleaseArtifact]:
    return sorted(
        (artifact for artifact in manifest.artifacts if artifact.role == "canonical_schema"),
        key=lambda artifact: artifact.path,
    )


def _sample_patterns(schema: MinedSchema, limit: int) -> list[SchemaPattern]:
    if limit <= 0:
        return []
    return sorted(
        [pattern for pattern in schema.patterns if pattern.evidence_source == "mined"],
        key=lambda p: hashlib.sha256(
            json.dumps(
                [p.subject_class, p.property_uri, p.object_class, p.datatype],
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
    )[:limit]


def build_scientific_validation_plan(
    manifest: ReleaseManifest,
    root: str | Path,
    *,
    patterns_per_schema: int = 3,
    routes_per_schema: int = 3,
) -> ScientificValidationPlan:
    """Create reproducible pattern/route checks without making network requests."""
    root = Path(root)
    datasets = {row.dataset_id: row for row in manifest.datasets}
    pattern_checks: list[PatternSpotCheckPlan] = []
    route_checks: list[RouteCheckPlan] = []
    skipped_checks: list[str] = []
    for artifact in _canonical_schema_artifacts(manifest):
        if not artifact.dataset_id or artifact.dataset_id not in datasets:
            continue
        dataset = datasets[artifact.dataset_id]
        if sha256_file(root / artifact.path) != artifact.sha256:
            raise ValueError(f"Schema differs from release: {artifact.path}")
        schema = MinedSchema.from_json(root / artifact.path)
        mode = _mode_from_path(artifact.path)
        target_kind = _target_kind(mode)
        snapshot = schema.about.snapshot_id or dataset.snapshot_id
        if snapshot is None:
            raise ValueError(f"Extraction snapshot identity missing: {artifact.path}")
        graphs = schema.about.graph_uris or []
        type_graphs = schema.about.type_graph_uris or graphs
        if mode == "grouped" and not schema.about.type_graph_uris:
            skipped_checks.append(f"{artifact.path}: grouped type-lookup scope is missing")
            continue
        endpoint = (schema.about.endpoint or dataset.endpoint) if mode == "remote" else None
        for pattern in _sample_patterns(schema, patterns_per_schema):
            signature = json.dumps(
                [
                    pattern.subject_class,
                    pattern.property_uri,
                    pattern.object_class,
                    pattern.datatype,
                ],
                separators=(",", ":"),
            )
            pattern_checks.append(
                PatternSpotCheckPlan(
                    check_id=_stable_id(snapshot, artifact.artifact_id, signature),
                    dataset_id=dataset.dataset_id,
                    snapshot_id=snapshot,
                    extraction_mode=mode,
                    schema_artifact_id=artifact.artifact_id,
                    target_kind=target_kind,
                    endpoint=endpoint,
                    graph_scope=graphs,
                    type_graph_scope=type_graphs,
                    type_context_graph_scope=schema.about.type_context_graph_uris or [],
                    pattern=pattern.model_dump(mode="json"),
                    query=pattern_existence_query(
                        pattern, graphs, type_graphs, schema.about.type_context_graph_uris
                    ),
                )
            )
        if schema.navigation is None or routes_per_schema <= 0:
            continue
        candidates = [path for path in schema.navigation.paths if path.query]
        candidates.sort(key=lambda path: hashlib.sha256((path.query or "").encode()).hexdigest())
        for path in candidates[:routes_per_schema]:
            route_checks.append(
                RouteCheckPlan(
                    check_id=_stable_id(snapshot, artifact.artifact_id, path.query or ""),
                    dataset_id=dataset.dataset_id,
                    snapshot_id=snapshot,
                    extraction_mode=mode,
                    schema_artifact_id=artifact.artifact_id,
                    target_kind=target_kind,
                    endpoint=endpoint,
                    expected_instance_support=path.instance_support,
                    expected_source_count=path.source_count,
                    expected_matched_sources=path.matched_sources,
                    query=path.query or "",
                )
            )
    return ScientificValidationPlan(
        release_id=manifest.release_id,
        patterns_per_schema=patterns_per_schema,
        routes_per_schema=routes_per_schema,
        pattern_checks=pattern_checks,
        route_checks=route_checks,
        skipped_checks=skipped_checks,
    )


def write_scientific_validation_plan(plan: ScientificValidationPlan, output: str | Path) -> Path:
    """Write the validation plan as JSON and return its path."""
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")
    return path


__all__ = [
    "PatternSpotCheckPlan",
    "RouteCheckPlan",
    "ScientificValidationPlan",
    "build_scientific_validation_plan",
    "pattern_existence_query",
    "write_scientific_validation_plan",
]
