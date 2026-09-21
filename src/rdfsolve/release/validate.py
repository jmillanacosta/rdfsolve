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


def _validate_role_json(role: str | None, path: Path) -> str | None:
    """Validate scientific JSON artifacts against the model used by the release.

    Imports are intentionally local so release validation stays independent of
    optional conversion stacks unless the corresponding artifact is present.
    """
    if role is None or path.suffix.lower() != ".json":
        return None
    try:
        payload = path.read_text(encoding="utf-8")
        if role == "property_usage_evidence":
            from rdfsolve.evidence.observed import PropertyUsageCollection

            PropertyUsageCollection.model_validate_json(payload)
        elif role == "declared_artifact_index":
            from rdfsolve.evidence.declared_sources import DeclaredArtifactBundle

            DeclaredArtifactBundle.model_validate_json(payload)
        elif role == "ontology_discovery":
            from rdfsolve.mining.ontology_discovery import OntologyDiscoverySummary

            OntologyDiscoverySummary.model_validate_json(payload)
        elif role == "ontology_acquisition":
            from rdfsolve.evidence.ontology_acquisition import OntologyAcquisitionPlan

            OntologyAcquisitionPlan.model_validate_json(payload)
        elif role == "canonical_schema":
            # Avoid requiring optional exporters merely to validate the frozen
            # evidence record.  The canonical wrapper must contain a schema
            # object and that object must contain a pattern list when present.
            raw = json.loads(payload)
            if not isinstance(raw, dict):
                raise ValueError("canonical schema JSON must be an object")
            schema = raw.get("schema", raw)
            if not isinstance(schema, dict):
                raise ValueError("canonical schema payload must be an object")
            patterns = schema.get("patterns", [])
            if not isinstance(patterns, list):
                raise ValueError("canonical schema patterns must be a list")
    except Exception as error:
        return f"{type(error).__name__}: {error}"
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
        model_error = _validate_role_json(artifact.role, path)
        if model_error is not None:
            issues.append(
                ValidationIssue(path=artifact.path, kind="evidence_model", message=model_error)
            )
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
        valid=not issues, checked_artifacts=len(manifest.artifacts), issues=issues
    )


__all__ = ["ReleaseValidation", "ValidationIssue", "validate_release"]
