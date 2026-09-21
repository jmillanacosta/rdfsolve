"""Mechanical validation for a frozen rdfsolve release directory."""

from __future__ import annotations

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
