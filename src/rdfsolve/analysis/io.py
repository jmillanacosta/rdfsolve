"""Load canonical schemas and project explicit SSSOM class mappings onto datasets."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from rdfsolve.analysis.schema import extract_class_set
from rdfsolve.mappings.models.core import MappingEdge
from rdfsolve.release.model import ExtractionReleaseRecord
from rdfsolve.schema_models.core import MinedSchema


def iter_extractions(
    directory: str | Path, *, extraction_mode: str | None = None
) -> Iterator[tuple[str, ExtractionReleaseRecord, MinedSchema | None]]:
    """Yield each attempt and its hash-verified schema when present."""
    from rdfsolve.release.build import sha256_file
    from rdfsolve.release.model import ReleaseManifest

    root = Path(directory).resolve()
    manifest = ReleaseManifest.model_validate_json((root / "release.json").read_text())
    artifacts = {artifact.path: artifact for artifact in manifest.artifacts}
    for dataset in manifest.datasets:
        if any(
            a.dataset_id == dataset.dataset_id and a.role == "canonical_schema"
            for a in manifest.artifacts
        ) and not any(e.schema_path for e in dataset.extractions):
            raise ValueError(f"Rebuild release extraction records for {dataset.dataset_id}")
        for extraction in dataset.extractions:
            if extraction_mode is not None and extraction.mode != extraction_mode:
                continue
            relative = extraction.schema_path
            if relative is None:
                yield dataset.dataset_id, extraction, None
                continue
            artifact = artifacts.get(relative)
            path = (root / relative).resolve()
            if not path.is_relative_to(root):
                raise ValueError(f"Schema path leaves release: {relative}")
            if artifact is None or artifact.artifact_id != extraction.schema_artifact_id:
                raise ValueError(f"Missing extraction artifact: {relative}")
            if sha256_file(path) != artifact.sha256:
                raise ValueError(f"Schema hash differs from release: {relative}")
            schema = MinedSchema.from_json(path)
            if (
                schema.about.dataset_name != dataset.dataset_id
                or schema.about.snapshot_id != extraction.snapshot_id
            ):
                raise ValueError(f"Schema identity differs from extraction: {relative}")
            yield dataset.dataset_id, extraction, schema


def load_schemas(
    directory: str | Path, *, extraction_mode: str | None = None
) -> dict[str, MinedSchema]:
    """Load one hash-verified extraction per dataset from a release manifest."""
    schemas: dict[str, MinedSchema] = {}
    for dataset, _, schema in iter_extractions(directory, extraction_mode=extraction_mode):
        if schema is None:
            continue
        if dataset in schemas:
            raise ValueError(f"Select one extraction for {dataset}")
        schemas[dataset] = schema
    return schemas


def read_class_mappings(
    path: str | Path, schemas: dict[str, MinedSchema]
) -> tuple[list[MappingEdge], dict[str, int]]:
    """Expand file CURIEs and retain each class mapping in every applicable dataset pair.

    The input must contain class mappings. Entity mappings require type indexing
    and derive_class_mappings instead. Unrepresented classes are counted separately.
    """
    from rdfsolve.mappings.sssom import project_mappings

    return project_mappings(
        path, {name: extract_class_set(schema) for name, schema in schemas.items()}
    )
