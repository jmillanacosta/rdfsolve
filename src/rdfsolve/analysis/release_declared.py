"""Build observed-versus-declared comparison data from a frozen release only.

No endpoint access is performed here.  Canonical observed schemas and normalized
provider-declared evidence are read exclusively from artifacts enumerated by
``release.json``.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from pydantic import BaseModel, Field

from rdfsolve.analysis.declared_comparison import (
    EvidenceComparison,
    compare_observed_with_declared_shacl,
)
from rdfsolve.evidence.declared_sources import DeclaredArtifactBundle
from rdfsolve.release.model import ReleaseManifest
from rdfsolve.schema_models.core import MinedSchema


class ReleaseDeclaredComparison(BaseModel):
    """Observed-declared comparisons for the datasets of one release."""

    release_id: str
    comparisons: list[EvidenceComparison] = Field(default_factory=list)
    compared_datasets: list[str] = Field(default_factory=list)
    skipped: dict[str, str] = Field(default_factory=dict)


def _artifacts_for_dataset(
    manifest: ReleaseManifest,
    dataset_id: str,
    role: str,
) -> list[str]:
    return sorted(
        artifact.path
        for artifact in manifest.artifacts
        if artifact.dataset_id == dataset_id and artifact.role == role
    )


def build_release_declared_comparison(
    manifest: ReleaseManifest,
    release_root: str | Path,
) -> ReleaseDeclaredComparison:
    """Compare observed schemas with normalized SHACL declarations in one release.

    A dataset is compared only when exactly one canonical schema and exactly one
    declared-artifact index are present.  Ambiguity is reported rather than
    silently choosing one of several artifacts.
    """
    root = Path(release_root)
    out = ReleaseDeclaredComparison(release_id=manifest.release_id)
    for dataset in sorted(manifest.datasets, key=lambda row: row.dataset_id):
        schemas = _artifacts_for_dataset(manifest, dataset.dataset_id, "canonical_schema")
        declared_indexes = _artifacts_for_dataset(
            manifest, dataset.dataset_id, "declared_artifact_index"
        )
        if not schemas or not declared_indexes:
            continue
        if len(schemas) != 1:
            out.skipped[dataset.dataset_id] = f"expected one canonical schema, found {len(schemas)}"
            continue
        if len(declared_indexes) != 1:
            out.skipped[dataset.dataset_id] = (
                f"expected one declared-artifact index, found {len(declared_indexes)}"
            )
            continue
        try:
            schema = MinedSchema.from_json(root / schemas[0])
            bundle = DeclaredArtifactBundle.model_validate_json(
                (root / declared_indexes[0]).read_text(encoding="utf-8")
            )
        except Exception as exc:
            out.skipped[dataset.dataset_id] = f"load failed: {type(exc).__name__}: {exc}"
            continue
        rows = compare_observed_with_declared_shacl(
            dataset_id=dataset.dataset_id,
            patterns=schema.patterns,
            declared=bundle.evidence,
        )
        out.comparisons.extend(rows)
        out.compared_datasets.append(dataset.dataset_id)
    return out


def write_release_declared_comparison(
    result: ReleaseDeclaredComparison,
    output_dir: str | Path,
) -> tuple[Path, Path]:
    """Write canonical JSON plus a flat TSV analysis view."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "observed_declared.json"
    json_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")

    tsv_path = output / "observed_declared.tsv"
    with tsv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "dataset_id",
                "subject_class",
                "property_uri",
                "dimension",
                "relation",
                "observed_count",
                "declared_count",
                "observed_values",
                "declared_values",
            ],
            delimiter="\t",
        )
        writer.writeheader()
        for row in result.comparisons:
            writer.writerow(
                {
                    "dataset_id": row.dataset_id,
                    "subject_class": row.subject_class,
                    "property_uri": row.property_uri,
                    "dimension": row.dimension,
                    "relation": row.relation,
                    "observed_count": row.observed_count,
                    "declared_count": row.declared_count,
                    "observed_values": json.dumps(row.observed_values, ensure_ascii=False),
                    "declared_values": json.dumps(row.declared_values, ensure_ascii=False),
                }
            )
    return json_path, tsv_path


__all__ = [
    "ReleaseDeclaredComparison",
    "build_release_declared_comparison",
    "write_release_declared_comparison",
]
