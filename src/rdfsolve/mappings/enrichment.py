"""Select external SSSOM assertions relevant to the mined classes."""

from __future__ import annotations

import logging
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import yaml

log = logging.getLogger(__name__)
from sssom import MappingSetDataFrame, parse_sssom_table, write_tsv

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema


def extract_classes_from_schema(schema: MinedSchema) -> set[str]:
    """Extract all class URIs from a mined schema.

    Args:
        schema: MinedSchema with patterns

    Returns:
        Set of class URIs used in the schema
    """
    return set(schema.get_classes())


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
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as response:
            response.raise_for_status()
            for chunk in response.iter_bytes():
                tmp.write(chunk)
        tmp_path = Path(tmp.name)

    try:
        if url.endswith((".tgz", ".tar.gz")):
            with tarfile.open(tmp_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.name.endswith(".sssom.tsv"):
                        tar.extract(member, output_dir, filter="data")
                        sssom_files.append(output_dir / member.name)

        elif url.endswith(".zip"):
            with zipfile.ZipFile(tmp_path, "r") as zf:
                for member_name in zf.namelist():
                    if member_name.endswith(".sssom.tsv"):
                        zf.extract(member_name, output_dir)
                        sssom_files.append(output_dir / member_name)

        elif url.endswith(".sssom.tsv"):
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
    """Retain mappings touching observed classes and preserve their provenance."""
    msdf = parse_sssom_table(sssom_file)

    def local(value):
        prefix, separator, identifier = str(value).partition(":")
        iri = (
            msdf.prefix_map[prefix] + identifier
            if separator and prefix in msdf.prefix_map
            else value
        )
        return iri in class_to_dataset

    selected = msdf.df["subject_id"].map(local) | msdf.df["object_id"].map(local)
    if not selected.any():
        return None
    msdf.df = msdf.df.loc[selected].copy()
    return msdf


def enrich_external_sssom_sources(
    sssom_sources_file: Path,
    schemas: list[tuple[str, MinedSchema]],
    dataset_void_uris: dict[str, str],
    output_dir: Path,
    creator_id: str | None = None,
    creator_label: str | None = None,
) -> dict[str, int]:
    """Download and select external SSSOM assertions from source configuration.

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
    class_to_dataset: dict[str, str] = {}
    for ds_name, schema in schemas:
        for cls_uri in extract_classes_from_schema(schema):
            class_to_dataset[cls_uri] = ds_name

    sources = yaml.safe_load(sssom_sources_file.read_text())

    downloads_dir = output_dir / "sssom_downloads"
    enriched_dir = output_dir / "mappings" / "enriched"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    enriched_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, int] = {}

    for source in sources:
        name = source["name"]
        url = source["url"]
        source_type = source.get("type", "class_mappings")

        if source_type == "property_mappings":
            continue

        try:
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
                    output_path = enriched_dir / f"enriched-{sssom_file.stem}.sssom.tsv"
                    write_tsv(enriched_msdf, output_path, embedded_mode=True)
                    total_enriched += len(enriched_msdf.df)

            results[name] = total_enriched

        except Exception as e:
            results[name] = -1  # Error indicator
            log.warning(f"Error processing {name}: {e}")

    return results
