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
    if role is not None:
        # The outputs of a per-graph part are validated as the outputs they are.
        role = role.removeprefix("graph_part_")
    if role == "scientific_validation_results":
        from rdfsolve.release.scientific_execution import ScientificValidationResults

        ScientificValidationResults.model_validate_json(path.read_text(encoding="utf-8"))
    elif role == "scientific_validation_plan":
        from rdfsolve.release.scientific_validation import ScientificValidationPlan

        ScientificValidationPlan.model_validate_json(path.read_text(encoding="utf-8"))
    elif role == "canonical_schema":
        _validate_schema_streamed(path)
    elif role == "property_usage_evidence":
        from rdfsolve.evidence.observed import PropertyUsageCollection

        PropertyUsageCollection.model_validate_json(path.read_text(encoding="utf-8"))
    elif role == "declared_artifact_index":
        from rdfsolve.evidence.declared_sources import DeclaredArtifactBundle

        DeclaredArtifactBundle.model_validate_json(path.read_text(encoding="utf-8"))
    elif role == "ontology_discovery":
        from rdfsolve.ontology.discovery import OntologyDiscoverySummary

        OntologyDiscoverySummary.model_validate_json(path.read_text(encoding="utf-8"))
    elif role == "ontology_acquisition":
        from rdfsolve.ontology.sources import OntologyAcquisitionPlan

        OntologyAcquisitionPlan.model_validate_json(path.read_text(encoding="utf-8"))


# The list sections of a schema validated row by row rather than read whole.
_ROW_SECTIONS = ("structural_patterns", "raw_patterns", "term_patterns")


def _validate_schema_streamed(path: Path) -> None:
    """Validate a schema file in the memory of its other sections and one row: the schema is
    read without its row sections and without the rows of its detail files
    (schema_models.layout), then each row is validated on its own with the checks the whole
    model applies to it; the rows of each detail file are counted against the schema's record
    of them, and tested paths are checked as packed rows (their steps once, each path as step
    numbers). Raises on the first error.
    """
    from rdfsolve.schema_models.core import MinedSchema, iter_section_rows
    from rdfsolve.schema_models.layout import PATHS_SECTION, packed_paths
    from rdfsolve.schema_models.pattern import SchemaPattern
    from rdfsolve.schema_models.structural import StructuralPattern

    schema = MinedSchema.from_json(path, leave_out=_ROW_SECTIONS, details=False)
    details = schema.details or {}
    pattern_fields = set(SchemaPattern.model_fields)
    counted = dict.fromkeys(_ROW_SECTIONS, 0)
    for name, row in iter_section_rows(path, _ROW_SECTIONS):
        if row is None:
            continue
        counted[name] += 1
        if name == "structural_patterns":
            StructuralPattern.model_validate(row)
            continue
        if set(row) - pattern_fields:
            raise ValueError(f"{name}: unknown canonical pattern fields")
        pattern = SchemaPattern.model_validate(row)
        bindings = (pattern.subject_binding, pattern.object_binding)
        if name == "raw_patterns" and "term" in bindings:
            raise ValueError("Exact term bindings belong in term_patterns")
        if name == "term_patterns" and bindings == ("type", "type"):
            raise ValueError("term_patterns requires an exact term binding")
    for name, detail in details.items():
        if name in counted and counted[name] != detail.rows:
            raise ValueError(
                f"{name}: {counted[name]} rows in {detail.file}, {detail.rows} recorded"
            )
    if PATHS_SECTION in details:
        packed = packed_paths(path)
        if packed is None:
            raise ValueError(f"{PATHS_SECTION}: no paths in {details[PATHS_SECTION].file}")
        for edge in packed.edges:
            SchemaPattern.model_validate(edge)
        rows = 0
        for path_row in packed.rows():
            rows += 1
            steps = path_row["edges"]
            if not 2 <= len(steps) <= 6 or not all(0 <= i < len(packed.edges) for i in steps):
                raise ValueError(f"{PATHS_SECTION}: a path of steps {steps} is not a path")
            for key in ("matched", "sources"):
                if path_row.get(key) is not None and path_row[key] < 0:
                    raise ValueError(f"{PATHS_SECTION}: negative {key}")
        if rows != details[PATHS_SECTION].rows:
            raise ValueError(
                f"{PATHS_SECTION}: {rows} rows in {details[PATHS_SECTION].file}, "
                f"{details[PATHS_SECTION].rows} recorded"
            )


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


# The streaming parser's format of each format name above.
_STREAMED_FORMATS = {
    "turtle": "TURTLE",
    "trig": "TRIG",
    "nt": "N_TRIPLES",
    "nquads": "N_QUADS",
    "xml": "RDF_XML",
}


def _parse_streamed(path: Path, rdf_format: str) -> None:
    """Parse an RDF file statement by statement, holding none of them: an artifact's syntax is
    checked in the memory of one statement, whatever its size. Raises on the first error.
    """
    import pyoxigraph

    name = _STREAMED_FORMATS.get(rdf_format)
    if name is None:
        Graph().parse(path, format=rdf_format)
        return
    # Relative IRIs resolve against the file, as rdflib resolves them.
    base = path.resolve().as_uri()
    for _ in pyoxigraph.parse(
        path=str(path), format=getattr(pyoxigraph.RdfFormat, name), base_iri=base
    ):
        pass


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
                    _parse_streamed(path, rdf_format)
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
