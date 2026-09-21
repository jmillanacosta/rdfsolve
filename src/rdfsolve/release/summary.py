"""Derive manuscript/release counts from a canonical release manifest."""

from __future__ import annotations

from collections import Counter
from typing import Any

from .model import ReleaseManifest


def summarize_release(manifest: ReleaseManifest) -> dict[str, Any]:
    """Return release-level counts derived from the manifest."""
    completion = Counter(item.completion_state for item in manifest.datasets)
    modes = Counter(item.extraction_mode or "unknown" for item in manifest.datasets)
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
    return {
        "datasets": len(manifest.datasets),
        "datasets_with_endpoint": endpoint,
        "datasets_with_configured_downloads": configured_downloads,
        # Kept as a compatibility alias for older summaries; a configured
        # download is not necessarily a dataset dump (e.g. download_owl).
        "datasets_with_distribution": configured_downloads,
        "datasets_with_endpoint_and_configured_downloads": sum(
            bool(item.endpoint and item.access_files) for item in manifest.datasets
        ),
        "datasets_with_both": sum(
            bool(item.endpoint and item.access_files) for item in manifest.datasets
        ),
        "datasets_with_download_owl": with_download_owl,
        "datasets_with_only_download_owl": only_download_owl,
        "datasets_with_non_owl_download": with_non_owl_download,
        "local_ontology_file_candidates": local_ontology_file_candidates,
        "ontology_evidence_context": dict(sorted(ontology_contexts.items())),
        "attempted_datasets": len(attempted),
        "attempted_completion": dict(sorted(attempted_completion.items())),
        "access_file_fields": dict(sorted(access_fields.items())),
        "completion": dict(sorted(completion.items())),
        "extraction_mode": dict(sorted(modes.items())),
        "artifacts": len(manifest.artifacts),
        "artifact_roles": dict(sorted(artifact_roles.items())),
        "ontology_usage_candidates": ontology_candidates,
        "ontology_identity_basis": dict(sorted(ontology_by_basis.items())),
        "assessed_ontology_usages": assessed_ontology_usages,
        "ontology_version_match": dict(sorted(ontology_version_match.items())),
    }
