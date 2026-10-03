"""Derive manuscript/release counts from a canonical release manifest."""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

from .model import ReleaseManifest


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _artifact_paths(manifest: ReleaseManifest, role: str) -> list[str]:
    return sorted(
        artifact.path
        for artifact in manifest.artifacts
        if artifact.role == role and artifact.dataset_id is not None
    )


def _summarize_observed(manifest: ReleaseManifest, root: Path) -> dict[str, Any]:
    pattern_types: Counter[str] = Counter()
    evidence_sources: Counter[str] = Counter()
    patterns = 0
    collections: Counter[str] = Counter()
    retained: Counter[str] = Counter()
    subject_shapes = 0
    datasets: set[str] = set()
    schema_artifacts = 0
    dataset_ids = {a.path: a.dataset_id for a in manifest.artifacts if a.dataset_id is not None}
    with_counts = 0
    with_distinct_subjects = 0
    with_distinct_objects = 0
    for rel in _artifact_paths(manifest, "canonical_schema"):
        raw = _load_json(root / rel)
        schema = raw.get("schema") if isinstance(raw.get("schema"), dict) else raw
        if isinstance(schema, dict):
            for field in ("raw_patterns", "term_patterns", "structural_patterns"):
                collection = schema.get(field)
                if isinstance(collection, list):
                    retained[field] += 1
                    collections[field] += len(collection)
            subject_shapes += len(
                {
                    (
                        row.get("graph_uri"),
                        row.get("subject_kind"),
                        tuple(sorted(set(row.get("subject_properties") or []))),
                    )
                    for row in schema.get("structural_patterns") or []
                    if isinstance(row, dict)
                    and row.get("shape_semantics", "exact_property_sets") == "exact_property_sets"
                }
            )
        rows = schema.get("patterns") if isinstance(schema, dict) else None
        if not isinstance(rows, list):
            continue
        schema_artifacts += 1
        datasets.add(dataset_ids[rel])
        for row in rows:
            if not isinstance(row, dict):
                continue
            patterns += 1
            pattern_types[str(row.get("pattern_type") or "unknown")] += 1
            evidence_sources[str(row.get("evidence_source") or "unknown")] += 1
            if row.get("count") is not None:
                with_counts += 1
            if row.get("distinct_subjects") is not None:
                with_distinct_subjects += 1
            if row.get("distinct_objects") is not None:
                with_distinct_objects += 1
    return {
        "datasets": len(datasets),
        "schema_artifacts": schema_artifacts,
        "patterns": patterns,
        "raw_patterns": collections["raw_patterns"],
        "term_patterns": collections["term_patterns"],
        "structural_patterns": collections["structural_patterns"],
        "structural_subject_shapes": subject_shapes,
        "retained_collections": {
            field: retained[field]
            for field in ("raw_patterns", "term_patterns", "structural_patterns")
        },
        "pattern_types": dict(sorted(pattern_types.items())),
        "evidence_sources": dict(sorted(evidence_sources.items())),
        "patterns_with_counts": with_counts,
        "patterns_with_distinct_subjects": with_distinct_subjects,
        "patterns_with_distinct_objects": with_distinct_objects,
    }


def _summarize_property_usage(manifest: ReleaseManifest, root: Path) -> dict[str, Any]:
    datasets = 0
    records = 0
    class_populations = 0
    populations_available = 0
    support_available = 0
    summary_states: Counter[str] = Counter()
    node_kind_states: Counter[str] = Counter()
    datatype_states: Counter[str] = Counter()
    histogram_states: Counter[str] = Counter()
    for rel in _artifact_paths(manifest, "property_usage_evidence"):
        raw = _load_json(root / rel)
        rows = raw.get("records")
        populations = raw.get("class_populations")
        if not isinstance(rows, list):
            continue
        datasets += 1
        if isinstance(populations, list):
            class_populations += len(populations)
            populations_available += sum(
                1
                for row in populations
                if isinstance(row, dict)
                and row.get("count_status") == "complete"
                and row.get("subject_count") is not None
            )
        for row in rows:
            if not isinstance(row, dict):
                continue
            records += 1
            if (
                row.get("eligible_subjects") not in (None, 0)
                and row.get("subjects_with_property") is not None
            ):
                support_available += 1
            state = row.get("summary_state")
            if isinstance(state, dict):
                summary_states[str(state.get("status") or "unknown")] += 1
            for field, counter in (
                ("node_kind_state", node_kind_states),
                ("datatype_state", datatype_states),
                ("histogram_state", histogram_states),
            ):
                detail = row.get(field)
                if isinstance(detail, dict):
                    counter[str(detail.get("status") or "unknown")] += 1
    return {
        "datasets": datasets,
        "records": records,
        "class_populations": class_populations,
        "class_populations_available": populations_available,
        "records_with_support_fraction": support_available,
        "summary_state": dict(sorted(summary_states.items())),
        "node_kind_state": dict(sorted(node_kind_states.items())),
        "datatype_state": dict(sorted(datatype_states.items())),
        "histogram_state": dict(sorted(histogram_states.items())),
    }


def _summarize_declared(manifest: ReleaseManifest, root: Path) -> dict[str, Any]:
    datasets = 0
    artifacts = 0
    evidence = 0
    errors = 0
    artifact_kinds: Counter[str] = Counter()
    declaration_types: Counter[str] = Counter()
    parse_status: Counter[str] = Counter()
    access_context: Counter[str] = Counter()
    for rel in _artifact_paths(manifest, "declared_artifact_index"):
        raw = _load_json(root / rel)
        rows = raw.get("artifacts")
        declared = raw.get("evidence")
        if not isinstance(rows, list) and not isinstance(declared, list):
            continue
        datasets += 1
        access_context[str(raw.get("access_context") or "unknown")] += 1
        if isinstance(rows, list):
            artifacts += len(rows)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                artifact_kinds[str(row.get("kind") or "unknown")] += 1
                parse_status[str(row.get("parse_status") or "unknown")] += 1
        if isinstance(declared, list):
            evidence += len(declared)
            for row in declared:
                if isinstance(row, dict):
                    declaration_types[str(row.get("declaration_type") or "unknown")] += 1
        raw_errors = raw.get("errors")
        if isinstance(raw_errors, list):
            errors += len(raw_errors)
    return {
        "datasets": datasets,
        "artifacts": artifacts,
        "artifact_kinds": dict(sorted(artifact_kinds.items())),
        "parse_status": dict(sorted(parse_status.items())),
        "evidence_records": evidence,
        "declaration_types": dict(sorted(declaration_types.items())),
        "errors": errors,
        "access_context": dict(sorted(access_context.items())),
    }


def summarize_release(manifest: ReleaseManifest, root: str | Path | None = None) -> dict[str, Any]:
    """Return release-level counts derived from the manifest and frozen artifacts.

    When *root* is supplied, evidence-layer counts are read from artifacts that
    are already enumerated and hashed by ``release.json``.  No live source or
    endpoint access is performed.
    """
    completion = Counter(item.completion_state for item in manifest.datasets)
    extraction_attempts = [
        extraction for item in manifest.datasets for extraction in item.extractions
    ]
    if not extraction_attempts:
        extraction_attempts = []
    modes = Counter(extraction.mode for extraction in extraction_attempts)
    datasets_by_mode = {
        mode: len(
            {
                item.dataset_id
                for item in manifest.datasets
                if any(e.mode == mode for e in item.extractions)
            }
        )
        for mode in sorted(modes)
    }
    artifact_roles = Counter(item.role or "other" for item in manifest.artifacts)
    ontology_candidates = sum(len(item.ontology_usages) for item in manifest.datasets)
    ontology_by_basis = Counter(
        usage.identity_basis for item in manifest.datasets for usage in item.ontology_usages
    )
    ontology_version_match = Counter(
        usage.version_match_status or "not_assessed"
        for item in manifest.datasets
        for usage in item.ontology_usages
        if usage.ontology_artifact_id
    )
    assessed_ontology_usages = sum(
        1
        for item in manifest.datasets
        for usage in item.ontology_usages
        if usage.ontology_artifact_id
    )
    endpoint = sum(bool(item.endpoint) for item in manifest.datasets)
    configured_downloads = sum(bool(item.access_files) for item in manifest.datasets)
    with_download_owl = sum("download_owl" in item.access_files for item in manifest.datasets)
    only_download_owl = sum(
        set(item.access_files) == {"download_owl"} for item in manifest.datasets
    )
    with_non_owl_download = sum(
        any(key != "download_owl" for key in item.access_files) for item in manifest.datasets
    )
    local_ontology_file_candidates = sum(
        item.local_ontology_file_candidate_count for item in manifest.datasets
    )
    ontology_contexts = Counter(
        item.ontology_evidence_context or "none" for item in manifest.datasets
    )
    attempted = [item for item in manifest.datasets if item.report_path is not None]
    attempted_completion = Counter(item.completion_state for item in attempted)
    access_fields = Counter(key for item in manifest.datasets for key in item.access_files)
    summary: dict[str, Any] = {
        "registry_entries": len(manifest.datasets) + len(manifest.service_records),
        "dataset_entries": len(manifest.datasets),
        "service_records": len(manifest.service_records),
        "identity_review_complete": manifest.identity_review_complete,
        "identity_candidate_count": manifest.identity_candidate_count,
        "canonical_dataset_count": manifest.canonical_dataset_count,
        "datasets_with_endpoint": endpoint,
        "datasets_with_configured_downloads": configured_downloads,
        "datasets_with_endpoint_and_configured_downloads": sum(
            bool(item.endpoint and item.access_files) for item in manifest.datasets
        ),
        "datasets_with_download_owl": with_download_owl,
        "datasets_with_only_download_owl": only_download_owl,
        "datasets_with_non_owl_download": with_non_owl_download,
        "local_ontology_file_candidates": local_ontology_file_candidates,
        "ontology_evidence_context": dict(sorted(ontology_contexts.items())),
        "attempted_datasets": len(attempted),
        "extraction_attempts": len(extraction_attempts),
        "datasets_by_extraction_mode": datasets_by_mode,
        "attempted_completion": dict(sorted(attempted_completion.items())),
        "access_file_fields": dict(sorted(access_fields.items())),
        "completion": dict(sorted(completion.items())),
        "extraction_attempts_by_mode": dict(sorted(modes.items())),
        "artifacts": len(manifest.artifacts),
        "artifact_roles": dict(sorted(artifact_roles.items())),
        "ontology_usage_candidates": ontology_candidates,
        "ontology_identity_basis": dict(sorted(ontology_by_basis.items())),
        "assessed_ontology_usages": assessed_ontology_usages,
        "ontology_version_match": dict(sorted(ontology_version_match.items())),
    }
    if root is not None:
        release_root = Path(root)
        summary["observed_evidence"] = _summarize_observed(manifest, release_root)
        summary["property_usage_evidence"] = _summarize_property_usage(manifest, release_root)
        summary["declared_evidence"] = _summarize_declared(manifest, release_root)
    return summary


def _flatten_summary(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    """Flatten nested summary mappings into stable dotted-key rows."""
    rows: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else str(key)
            rows.extend(_flatten_summary(value[key], child))
        return rows
    if isinstance(value, list):
        return [(prefix, json.dumps(value, ensure_ascii=False, sort_keys=True))]
    return [(prefix, "" if value is None else str(value))]


def write_release_summary(summary: dict[str, Any], root: str | Path) -> tuple[Path, Path]:
    """Write canonical JSON and a flat TSV view of one release summary."""
    output = Path(root)
    json_path = output / "summary.json"
    tsv_path = output / "summary.tsv"
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    with tsv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["metric", "value"])
        writer.writerows(_flatten_summary(summary))
    return json_path, tsv_path


__all__ = ["summarize_release", "write_release_summary"]
