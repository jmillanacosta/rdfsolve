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
    """Compare each channel's observed schema with its normalized SHACL declarations.

    A channel is a file stem shared by ``*_schema.json`` and
    ``*_declared_artifacts.json`` (for example ``name_local``). A side without its
    partner is reported in ``skipped`` rather than silently omitted.
    """
    root = Path(release_root)
    out = ReleaseDeclaredComparison(release_id=manifest.release_id)
    for dataset in sorted(manifest.datasets, key=lambda row: row.dataset_id):
        name = dataset.dataset_id
        schemas = {
            Path(p).name.removesuffix("_schema.json"): p
            for p in _artifacts_for_dataset(manifest, name, "canonical_schema")
        }
        declared = {
            Path(p).name.removesuffix("_declared_artifacts.json"): p
            for p in _artifacts_for_dataset(manifest, name, "declared_artifact_index")
        }
        if not declared:
            continue
        for channel in sorted(schemas.keys() | declared.keys()):
            key = f"{name}/{channel}"
            if channel not in declared:
                out.skipped[key] = "mined schema without declared artifacts"
                continue
            if channel not in schemas:
                out.skipped[key] = "declared artifacts without a mined schema"
                continue
            try:
                schema = MinedSchema.from_json(root / schemas[channel])
                bundle = DeclaredArtifactBundle.model_validate_json(
                    (root / declared[channel]).read_text(encoding="utf-8")
                )
            except Exception as exc:
                out.skipped[key] = f"load failed: {type(exc).__name__}: {exc}"
                continue
            rows = compare_observed_with_declared_shacl(
                dataset_id=name, patterns=schema.patterns, declared=bundle.evidence
            )
            out.comparisons.extend(row.model_copy(update={"channel": channel}) for row in rows)
            if name not in out.compared_datasets:
                out.compared_datasets.append(name)
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
                "channel",
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
                    "channel": row.channel,
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
