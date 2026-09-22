"""Mechanical validation for a frozen rdfsolve release directory."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, Field
from rdflib import Graph

from .build import sha256_file
from .model import ReleaseManifest


class ValidationIssue(BaseModel):
    """One problem found while validating a release."""

    path: str | None = None
    kind: str
    message: str


class ReleaseValidation(BaseModel):
    """Result of validating a release directory."""

    valid: bool
    checked_artifacts: int
    issues: list[ValidationIssue] = Field(default_factory=list)
    skipped_checks: list[str] = Field(default_factory=list)


def _validate_role_json(path: Path, role: str | None) -> None:
    if role == "canonical_schema":
        from rdfsolve.schema_models.core import MinedSchema

        MinedSchema.from_json(path)
    elif role == "property_usage_evidence":
        from rdfsolve.evidence.observed import PropertyUsageCollection

        PropertyUsageCollection.model_validate_json(path.read_text(encoding="utf-8"))
    elif role == "declared_artifact_index":
        from rdfsolve.evidence.declared_sources import DeclaredArtifactBundle

        DeclaredArtifactBundle.model_validate_json(path.read_text(encoding="utf-8"))
    elif role == "ontology_discovery":
        from rdfsolve.mining.ontology_discovery import OntologyDiscoverySummary

        OntologyDiscoverySummary.model_validate_json(path.read_text(encoding="utf-8"))
    elif role == "ontology_acquisition":
        from rdfsolve.evidence.ontology_acquisition import OntologyAcquisitionPlan

        OntologyAcquisitionPlan.model_validate_json(path.read_text(encoding="utf-8"))


def _validate_sssom(path: Path) -> str | None:
    try:
        from sssom import parse_sssom_table
    except ImportError:
        return "SSSOM parser unavailable in this environment"
    parse_sssom_table(path)
    return None


_RDF_FORMATS = {
    ".ttl": "turtle",
    ".trig": "trig",
    ".nt": "nt",
    ".nq": "nquads",
    ".rdf": "xml",
    ".owl": "xml",
    ".jsonld": "json-ld",
}


def validate_release(
    manifest: ReleaseManifest, root: str | Path, *, parse_rdf: bool = True
) -> ReleaseValidation:
    """Check artifact references, sizes, hashes and RDF syntax of a release."""
    root = Path(root)
    issues: list[ValidationIssue] = []
    skipped_checks: list[str] = []
    ids = {artifact.artifact_id for artifact in manifest.artifacts}
    for dataset in manifest.datasets:
        for ref in dataset.artifacts:
            if ref not in ids:
                issues.append(
                    ValidationIssue(
                        kind="dangling_artifact", message=f"{dataset.dataset_id}: {ref}"
                    )
                )
        for usage in dataset.ontology_usages:
            if usage.ontology_artifact_id and usage.ontology_artifact_id not in ids:
                issues.append(
                    ValidationIssue(
                        kind="dangling_ontology_artifact",
                        message=(
                            f"{dataset.dataset_id}: {usage.namespace}: {usage.ontology_artifact_id}"
                        ),
                    )
                )
            for ontology_ref in usage.artifact_refs:
                if ontology_ref.artifact_id and ontology_ref.artifact_id not in ids:
                    issues.append(
                        ValidationIssue(
                            kind="dangling_ontology_artifact",
                            message=(
                                f"{dataset.dataset_id}: {usage.namespace}: {ontology_ref.artifact_id}"
                            ),
                        )
                    )

    for artifact in manifest.artifacts:
        path = root / artifact.path
        if not path.exists():
            issues.append(
                ValidationIssue(path=artifact.path, kind="missing", message="File does not exist")
            )
            continue
        if path.stat().st_size != artifact.byte_size:
            issues.append(
                ValidationIssue(
                    path=artifact.path, kind="size", message="Byte size differs from manifest"
                )
            )
        if sha256_file(path) != artifact.sha256:
            issues.append(
                ValidationIssue(
                    path=artifact.path, kind="checksum", message="SHA-256 differs from manifest"
                )
            )
        if artifact.media_type == "application/json" or path.suffix.lower() == ".json":
            try:
                _validate_role_json(path, artifact.role)
            except Exception as error:
                issues.append(
                    ValidationIssue(path=artifact.path, kind="json_model", message=str(error))
                )
        if path.name.endswith(".sssom.tsv"):
            try:
                skipped = _validate_sssom(path)
            except Exception as error:
                issues.append(ValidationIssue(path=artifact.path, kind="sssom", message=str(error)))
            else:
                if skipped:
                    skipped_checks.append(f"{artifact.path}: {skipped}")
        if parse_rdf:
            suffix = path.suffix.lower()
            rdf_format = _RDF_FORMATS.get(suffix)
            # JSON-LD schema exports can require contexts/dependencies not
            # available offline; only parse when they declare RDF JSON-LD media.
            if rdf_format and artifact.media_type in {
                "text/turtle",
                "application/trig",
                "application/n-triples",
                "application/n-quads",
                "application/rdf+xml",
            }:
                try:
                    Graph().parse(path, format=rdf_format)
                except Exception as error:
                    issues.append(
                        ValidationIssue(path=artifact.path, kind="rdf_parse", message=str(error))
                    )
    return ReleaseValidation(
        valid=not issues,
        checked_artifacts=len(manifest.artifacts),
        issues=issues,
        skipped_checks=skipped_checks,
    )


__all__ = ["ReleaseValidation", "ValidationIssue", "validate_release"]
