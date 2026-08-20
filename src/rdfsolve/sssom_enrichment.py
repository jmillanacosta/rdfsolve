"""Enrich external SSSOM files with RDFSolve VoID dataset URIs.

Downloads external SSSOM mapping sets (e.g., OLS mappings) and enriches them
by adding subject_source/object_source fields pointing to our VoID dataset URIs
where the mapped classes appear in our datasets.
"""

from __future__ import annotations

import logging
import tarfile
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import yaml

log = logging.getLogger(__name__)
from sssom import Mapping, MappingSetDataFrame, parse_sssom_table, write_tsv

from rdfsolve.sssom_generator import create_sssom_mappings

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema


def extract_classes_from_schema(schema: MinedSchema) -> set[str]:
    """Extract all class URIs from a mined schema.

    Args:
        schema: MinedSchema with patterns

    Returns:
        Set of class URIs used in the schema
    """
    classes: set[str] = set()
    for pat in schema.patterns:
        if pat.subject_class:
            classes.add(pat.subject_class)
        if pat.object_class:
            classes.add(pat.object_class)
    return classes


def download_sssom_source(
    url: str,
    output_dir: Path,
    name: str,
) -> list[Path]:
    """Download and extract SSSOM files from a URL.

    Supports .tgz, .tar.gz, .zip archives and plain .sssom.tsv files.

    Args:
        url: URL to download from
        output_dir: Directory to extract files to
        name: Source name for logging

    Returns:
        List of paths to extracted .sssom.tsv files
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    sssom_files: list[Path] = []

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        # Download file
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as response:
            response.raise_for_status()
            for chunk in response.iter_bytes():
                tmp.write(chunk)
        tmp_path = Path(tmp.name)

    try:
        if url.endswith((".tgz", ".tar.gz")):
            # Extract tarball
            with tarfile.open(tmp_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.name.endswith(".sssom.tsv"):
                        tar.extract(member, output_dir)
                        sssom_files.append(output_dir / member.name)

        elif url.endswith(".zip"):
            # Extract zip
            with zipfile.ZipFile(tmp_path, "r") as zf:
                for member_name in zf.namelist():
                    if member_name.endswith(".sssom.tsv"):
                        zf.extract(member_name, output_dir)
                        sssom_files.append(output_dir / member_name)

        elif url.endswith(".sssom.tsv"):
            # Plain SSSOM file
            dest = output_dir / f"{name}.sssom.tsv"
            dest.write_bytes(tmp_path.read_bytes())
            sssom_files.append(dest)

    finally:
        tmp_path.unlink()

    return sssom_files


def enrich_sssom_file(
    sssom_file: Path,
    class_to_dataset: dict[str, str],
    dataset_void_uris: dict[str, str],
    creator_id: str | None = None,
    creator_label: str | None = None,
) -> MappingSetDataFrame | None:
    """Enrich a single SSSOM file with our VoID dataset URIs.

    For each mapping, identifies which of our datasets contain those classes
    and adds subject_source/object_source fields.

    Args:
        sssom_file: Path to SSSOM TSV file
        class_to_dataset: Mapping of class URI -> dataset name
        dataset_void_uris: Mapping of dataset name -> VoID URI
        creator_id: Optional creator ORCID
        creator_label: Optional creator name

    Returns:
        Enriched MappingSetDataFrame, or None if no mappings were enriched
    """
    # Parse external SSSOM
    msdf = parse_sssom_table(sssom_file)

    enriched_mappings: list[Mapping] = []

    for _, row in msdf.df.iterrows():
        subject_id = str(row.get("subject_id", ""))
        object_id = str(row.get("object_id", ""))

        # Find which of our datasets contain these classes
        subj_ds = class_to_dataset.get(subject_id)
        obj_ds = class_to_dataset.get(object_id)

        # Only include if at least one class is in our datasets
        if subj_ds or obj_ds:
            mapping = Mapping(
                subject_id=subject_id,
                predicate_id=str(row.get("predicate_id", "")),
                object_id=object_id,
                mapping_justification=str(row.get("mapping_justification", "")),
                confidence=float(row["confidence"]) if row.get("confidence") else None,
                subject_source=dataset_void_uris.get(subj_ds) if subj_ds else None,
                object_source=dataset_void_uris.get(obj_ds) if obj_ds else None,
                comment=f"Enriched from {sssom_file.name}",
            )
            enriched_mappings.append(mapping)

    if not enriched_mappings:
        return None

    # Create enriched mapping set
    return create_sssom_mappings(
        mappings=enriched_mappings,
        mapping_set_id=f"https://w3id.org/rdfsolve/mappings/enriched-{sssom_file.stem}",
        mapping_set_version=str(datetime.now(UTC).date()),
        creator_id=creator_id,
        creator_label=creator_label,
    )


def enrich_external_sssom_sources(
    sssom_sources_file: Path,
    schemas: list[tuple[str, MinedSchema]],
    dataset_void_uris: dict[str, str],
    output_dir: Path,
    creator_id: str | None = None,
    creator_label: str | None = None,
) -> dict[str, int]:
    """Download and enrich external SSSOM files from sources config.

    Args:
        sssom_sources_file: Path to sssom_sources.yaml config file
        schemas: List of (dataset_name, MinedSchema) tuples
        dataset_void_uris: Mapping of dataset name -> VoID URI
        output_dir: Directory for output files
        creator_id: Optional creator ORCID
        creator_label: Optional creator name

    Returns:
        Dict mapping source name -> number of enriched mappings
    """
    # Build class-to-dataset index from all schemas
    class_to_dataset: dict[str, str] = {}
    for ds_name, schema in schemas:
        for cls_uri in extract_classes_from_schema(schema):
            class_to_dataset[cls_uri] = ds_name

    # Load SSSOM sources config
    sources = yaml.safe_load(sssom_sources_file.read_text())

    # Create output directories
    downloads_dir = output_dir / "sssom_downloads"
    enriched_dir = output_dir / "mappings" / "enriched"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    enriched_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, int] = {}

    for source in sources:
        name = source["name"]
        url = source["url"]
        source_type = source.get("type", "class_mappings")

        # Skip property mappings for now (different handling needed)
        if source_type == "property_mappings":
            continue

        try:
            # Download and extract
            sssom_files = download_sssom_source(url, downloads_dir / name, name)

            total_enriched = 0
            for sssom_file in sssom_files:
                enriched_msdf = enrich_sssom_file(
                    sssom_file,
                    class_to_dataset,
                    dataset_void_uris,
                    creator_id=creator_id,
                    creator_label=creator_label,
                )

                if enriched_msdf is not None:
                    # Export enriched file
                    output_path = enriched_dir / f"enriched-{sssom_file.stem}.sssom.tsv"
                    write_tsv(enriched_msdf, output_path, embedded_mode=True)
                    total_enriched += len(enriched_msdf.df)

            results[name] = total_enriched

        except Exception as e:
            results[name] = -1  # Error indicator
            log.warning(f"Error processing {name}: {e}")

    return results
