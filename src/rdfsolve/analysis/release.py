"""Analyse retained schema evidence by extraction and access channel."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from itertools import combinations
from pathlib import Path
from typing import Any

from rdfsolve.analysis.connectivity import build_connectivity
from rdfsolve.analysis.io import iter_extractions, read_class_mappings
from rdfsolve.analysis.overlap import jaccard_similarity
from rdfsolve.release.build import sha256_file
from rdfsolve.release.model import ReleaseManifest
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.structural import StructuralPattern

_VIEWS = ("patterns", "raw_patterns", "term_patterns", "structural_patterns")


def _terms(rows: Sequence[SchemaPattern | StructuralPattern]) -> dict[str, set[str]]:
    """Collect vocabulary with class and exact-term bindings kept separate."""
    terms: dict[str, set[str]] = {"classes": set(), "predicates": set(), "terms": set()}
    for row in rows:
        terms["predicates"].add(row.property_uri)
        if isinstance(row, SchemaPattern):
            for value, binding in (
                (row.subject_class, row.subject_binding),
                (row.object_class, row.object_binding),
            ):
                if value not in _SENTINEL_OBJECTS:
                    terms["terms" if binding == "term" else "classes"].add(value)
    return terms


def _view_counts(schema: MinedSchema) -> dict[str, dict[str, int] | None]:
    """Count retained rows and graph-local exact subject shapes."""
    result: dict[str, dict[str, int] | None] = {}
    for view in (*_VIEWS, "collections"):
        rows = getattr(schema, view)
        result[view] = None if rows is None else {"rows": len(rows)}
    if schema.structural_patterns is not None:
        result["structural_patterns"] = {
            "rows": len(schema.structural_patterns),
            "subject_shapes": len(
                {
                    (p.graph_uri, p.subject_kind, tuple(p.subject_properties))
                    for p in schema.structural_patterns
                    if p.shape_semantics == "exact_property_sets"
                }
            ),
        }
    return result


def _compare(
    left: dict[str, Any],
    right: dict[str, Any],
    vocabulary: dict[str, dict[str, dict[str, set[str]] | None]],
) -> list[dict[str, Any]]:
    """Compare observed sets without treating incomplete retrieval as absence."""
    result = []
    for view in _VIEWS:
        a = vocabulary[left["schema_path"]][view]
        b = vocabulary[right["schema_path"]][view]
        recorded = a is not None and b is not None
        complete = recorded and left["completion_state"] == right["completion_state"] == "complete"
        row: dict[str, Any] = {
            "source_dataset": left["dataset_id"],
            "target_dataset": right["dataset_id"],
            "source_mode": left["mode"],
            "target_mode": right["mode"],
            "source_schema": left["schema_path"],
            "target_schema": right["schema_path"],
            "source_snapshot": left["snapshot_id"],
            "target_snapshot": right["snapshot_id"],
            "view": view,
            "completion_states": [left["completion_state"], right["completion_state"]],
            "comparison_state": "not_recorded"
            if not recorded
            else "complete"
            if complete
            else "incomplete",
            "absence_supported": complete,
            "shared_count_basis": "unavailable"
            if not recorded
            else "retained_view"
            if complete
            else "observed_lower_bound",
        }
        for field in ("classes", "predicates", "terms"):
            first = a[field] if a is not None else set()
            second = b[field] if b is not None else set()
            row[f"shared_{field}"] = len(first & second) if recorded else None
            row[f"observed_{field}_jaccard"] = (
                jaccard_similarity(first, second) if recorded else None
            )
        result.append(row)
    return result


def analyze_release(
    directory: str | Path, *, excluded_subject_classes: frozenset[str] = frozenset()
) -> dict[str, Any]:
    """Read a frozen release and retain each attempt in channel-specific analyses."""
    from networkx import node_link_data

    root = Path(directory).resolve()
    manifest = ReleaseManifest.model_validate_json((root / "release.json").read_text())
    artifacts = {artifact.path: artifact for artifact in manifest.artifacts}

    def verified_path(relative: str) -> Path:
        """Resolve an inventoried artifact and verify its hash."""
        artifact = artifacts.get(relative)
        path = (root / relative).resolve()
        if artifact is None or not path.is_relative_to(root):
            raise ValueError(f"Artifact path differs from release: {relative}")
        if sha256_file(path) != artifact.sha256:
            raise ValueError(f"Artifact hash differs from release: {relative}")
        return path

    inventory: list[dict[str, Any]] = []
    schemas: dict[str, MinedSchema] = {}
    for dataset, attempt, schema in iter_extractions(root):
        row: dict[str, Any] = {
            "dataset_id": dataset,
            **attempt.model_dump(),
            "views": None,
            "coverage": None,
        }
        if attempt.report_path:
            report = json.loads(verified_path(attempt.report_path).read_text())
            row["coverage"] = report.get("config", {}).get("structural_coverage")
        if schema is not None:
            if attempt.schema_path is None:
                raise ValueError("Schema extraction has no artifact path")
            if attempt.schema_path in schemas:
                raise ValueError(
                    f"Schema used by multiple extraction records: {attempt.schema_path}"
                )
            row["retained_views"] = _view_counts(schema)
            row["coverage_scope"] = "retained_extraction"
            view = schema.model_copy(deep=True)
            excluded_counts = {}
            for name in ("patterns", "raw_patterns", "term_patterns"):
                patterns = getattr(view, name)
                if patterns is not None:
                    kept = [
                        p
                        for p in patterns
                        if p.subject_binding != "type"
                        or p.subject_class not in excluded_subject_classes
                    ]
                    excluded_counts[name] = len(patterns) - len(kept)
                    setattr(view, name, kept)
            if view.collections is not None:
                kept_collections = [
                    p for p in view.collections if p.subject_class not in excluded_subject_classes
                ]
                excluded_counts["collections"] = len(view.collections) - len(kept_collections)
                view.collections = kept_collections
            row["view_exclusions"] = excluded_counts
            schemas[attempt.schema_path] = view
            row["views"] = _view_counts(view)
        row["coverage_basis"] = (
            "unavailable"
            if row["coverage"] is None
            else "complete_extraction"
            if attempt.completion_state == "complete"
            else "incomplete_extraction"
        )
        inventory.append(row)

    dataset_inventory: list[dict[str, Any]] = []
    for dataset_record in manifest.datasets:
        attempts = [row for row in inventory if row["dataset_id"] == dataset_record.dataset_id]
        dataset_inventory.append(
            {
                "dataset_id": dataset_record.dataset_id,
                "extraction_records": len(attempts),
                "schema_extractions": sum(row["views"] is not None for row in attempts),
                "completion_states": dict(Counter(row["completion_state"] for row in attempts)),
                "channels": sorted({row["mode"] for row in attempts}),
            }
        )

    mappings = []
    for artifact in manifest.artifacts:
        relative = Path(artifact.path)
        if relative.parent == Path("mappings/enriched") and relative.name.endswith(".sssom.tsv"):
            mappings.append(verified_path(artifact.path))

    channels, graphs = {}, {}
    for mode in sorted({row["mode"] for row in inventory}):
        attempts = [row for row in inventory if row["mode"] == mode]
        selected = {
            row["schema_path"]: schemas[row["schema_path"]]
            for row in attempts
            if row["views"] is not None
        }
        edges, imports = [], {}
        for path in mappings:
            found, report = read_class_mappings(path, selected)
            edges.extend(found)
            imports[str(path.relative_to(root))] = report
        graph = build_connectivity(selected, class_mappings=edges)
        metadata = {row["schema_path"]: row for row in attempts}
        for (key, _), data in graph.nodes(data=True):
            data.update(
                dataset=metadata[key]["dataset_id"],
                mode=mode,
                schema_path=key,
                snapshot_id=metadata[key]["snapshot_id"],
                completion_state=metadata[key]["completion_state"],
            )
        graph.graph.update(view="patterns", mode=mode, primary_view="pending_review")
        graphs[mode] = node_link_data(graph)
        channels[mode] = {
            "extraction_attempts": len(attempts),
            "completion_states": dict(Counter(row["completion_state"] for row in attempts)),
            "schema_extractions": len(selected),
            "datasets_with_schemas": len(
                {r["dataset_id"] for r in attempts if r["views"] is not None}
            ),
            "class_nodes": len(graph),
            "schema_edges": sum(d["kind"] == "schema" for _, _, d in graph.edges(data=True)),
            "explicit_mapping_edges": len(edges),
            "mapping_imports": imports,
        }

    vocabulary = {
        key: {
            view: _terms(rows) if (rows := getattr(schema, view)) is not None else None
            for view in _VIEWS
        }
        for key, schema in schemas.items()
    }
    overlaps, comparisons = [], []
    retained = [row for row in inventory if row["views"] is not None]
    for left, right in combinations(retained, 2):
        if left["mode"] == right["mode"]:
            overlaps.extend(_compare(left, right, vocabulary))
        elif left["dataset_id"] == right["dataset_id"]:
            comparisons.extend(_compare(left, right, vocabulary))
    return {
        "paper_statistics": {
            "release_id": manifest.release_id,
            "primary_view": "pending_review",
            "excluded_subject_classes": sorted(excluded_subject_classes),
            "structural_view_scope": "retained_extraction",
            "dataset_unit": "registry_entry",
            "registry_dataset_entries": len(dataset_inventory),
            "service_records": len(manifest.service_records),
            "identity_review_complete": manifest.identity_review_complete,
            "canonical_dataset_count": manifest.canonical_dataset_count,
            "datasets_with_extraction_records": sum(
                row["extraction_records"] > 0 for row in dataset_inventory
            ),
            "datasets_without_extraction_records": sum(
                row["extraction_records"] == 0 for row in dataset_inventory
            ),
            "extraction_attempts": len(inventory),
            "schema_extractions": len(schemas),
            "completion_states": dict(Counter(row["completion_state"] for row in inventory)),
            "channels": channels,
        },
        "dataset_inventory": dataset_inventory,
        "extraction_inventory": inventory,
        "schema_overlaps": overlaps,
        "channel_comparisons": comparisons,
        "class_connectivity": graphs,
    }


def write_release_analysis(result: dict[str, Any], output_dir: str | Path) -> None:
    """Write analysis tables without rebuilding their input manifest."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    for name, value in result.items():
        (output / f"{name}.json").write_text(json.dumps(value, indent=2), encoding="utf-8")
