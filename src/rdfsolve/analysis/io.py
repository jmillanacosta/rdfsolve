"""Load canonical schemas and project explicit SSSOM class mappings onto datasets."""

from __future__ import annotations

from pathlib import Path

from rdfsolve.analysis.schema import extract_class_set
from rdfsolve.mappings.models.core import MappingEdge
from rdfsolve.schema_models.core import MinedSchema


def load_schemas(directory: str | Path) -> dict[str, MinedSchema]:
    """Load canonical schema snapshots, rejecting duplicate dataset names."""
    schemas = {}
    for path in sorted(
        set(Path(directory).rglob("*_schema.json")) | set(Path(directory).rglob("*.schema.json"))
    ):
        schema = MinedSchema.from_json(path)
        name = schema.about.dataset_name
        if not name or name in schemas:
            raise ValueError(f"Choose one named schema snapshot per dataset: {path}")
        schemas[name] = schema
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
