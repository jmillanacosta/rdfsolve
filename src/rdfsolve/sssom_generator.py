"""SSSOM mapping set generation with TSV and RDF export.

Generates SSSOM (Simple Standard for Sharing Ontological Mappings) files
documenting class-to-class mappings between datasets using the official
sssom library.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sssom import Mapping, MappingSetDataFrame, write_rdf, write_tsv

if TYPE_CHECKING:
    from collections.abc import Sequence


def create_sssom_mappings(
    mappings: Sequence[Mapping],
    mapping_set_id: str,
    mapping_set_version: str | None = None,
    subject_source: str | None = None,
    object_source: str | None = None,
    creator_id: str | None = None,
    creator_label: str | None = None,
    license_uri: str = "https://creativecommons.org/publicdomain/zero/1.0/",
    mapping_provider: str | None = None,
    mapping_tool: str = "rdfsolve",
    mapping_tool_version: str = "0.3.0",
) -> MappingSetDataFrame:
    """Create SSSOM MappingSetDataFrame from individual mappings.

    Args:
        mappings: List of sssom.Mapping objects
        mapping_set_id: Canonical URI for this mapping set
        mapping_set_version: Version identifier (defaults to today's date)
        subject_source: VoID dataset URI for subject classes
        object_source: VoID dataset URI for object classes
        creator_id: Tool/person that created mappings
        license_uri: License URI
        mapping_provider: Source that provided the mapping
        mapping_tool: Tool used to generate mappings
        mapping_tool_version: Version of the tool

    Returns:
        MappingSetDataFrame ready for serialization
    """
    if mapping_set_version is None:
        mapping_set_version = str(datetime.now(UTC).date())

    if creator_id is None:
        creator_id = "https://orcid.org/0000-0001-5608-781X"

    if creator_label is None:
        creator_label = "Javier Millan Acosta"

    if mapping_provider is None:
        mapping_provider = "https://github.com/jmillanacosta/rdfsolve"

    # Create metadata dict for the mapping set
    metadata = {
        "mapping_set_id": mapping_set_id,
        "mapping_set_version": mapping_set_version,
        "creator_id": creator_id,
        "creator_label": creator_label,
        "license": license_uri,
        "mapping_date": str(datetime.now(UTC).date()),
        "mapping_provider": mapping_provider,
        "mapping_tool": mapping_tool,
        "mapping_tool_version": mapping_tool_version,
    }

    # Add subject/object source if provided
    if subject_source:
        metadata["subject_source"] = subject_source
    if object_source:
        metadata["object_source"] = object_source

    # Create MappingSetDataFrame from mappings
    msdf = MappingSetDataFrame.from_mappings(mappings=list(mappings), metadata=metadata)

    # Add custom prefixes for RDFSolve URIs
    msdf.prefix_map.update(
        {
            "rdfsolve": "https://rdfsolve.bigcat-bioinformatics.nl/",
            "orcid": "https://orcid.org/",
        }
    )

    # Clean prefix map to only include prefixes actually used in the mapping set
    msdf.clean_prefix_map()

    return msdf


def write_sssom_tsv(msdf: MappingSetDataFrame, output_path: Path) -> None:
    """Write SSSOM MappingSetDataFrame to TSV file.

    Args:
        msdf: MappingSetDataFrame to write
        output_path: Path to output TSV file
    """
    write_tsv(msdf, output_path, embedded_mode=True)


def write_sssom_rdf(msdf: MappingSetDataFrame, output_path: Path, format: str = "turtle") -> None:
    """Write SSSOM MappingSetDataFrame to RDF file.

    Args:
        msdf: MappingSetDataFrame to write
        output_path: Path to output RDF file
        format: RDF serialization format (turtle, xml, nt, etc.)
    """
    write_rdf(msdf, output_path, serialisation=format)
