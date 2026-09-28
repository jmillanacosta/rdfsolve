"""SSSOM mapping set generation with TSV and RDF export.

Generates SSSOM (Simple Standard for Sharing Ontological Mappings) files
documenting class-to-class mappings between datasets using the official
sssom library.
"""

from __future__ import annotations

from datetime import datetime, timezone
from itertools import product
from math import isnan
from pathlib import Path
from typing import TYPE_CHECKING

from sssom import Mapping, MappingSetDataFrame, write_tsv

from rdfsolve.config import get_base_uri
from rdfsolve.mappings.models.core import MappingEdge

if TYPE_CHECKING:
    from collections.abc import Mapping as MappingType
    from collections.abc import Sequence

    from curies import Converter

    from rdfsolve.mappings.signatures import LinkEvidence
    from rdfsolve.schema_models.core import MinedSchema


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
    converter: Converter | None = None,
) -> MappingSetDataFrame:
    """Create SSSOM MappingSetDataFrame from individual mappings.

    Args:
        mappings: List of sssom.Mapping objects
        mapping_set_id: Canonical URI for this mapping set
        mapping_set_version: Version identifier (defaults to today's date)
        subject_source: VoID dataset URI for subject classes
        object_source: VoID dataset URI for object classes
        creator_id: Person or agent that created the mappings (left out when not given)
        creator_label: Name of the creator (left out when not given)
        license_uri: License URI
        mapping_provider: Source that provided the mapping
        mapping_tool: Tool used to generate mappings
        mapping_tool_version: Version of the tool
        converter: Prefixes of the CURIEs in the mappings, added to the SSSOM built-in ones

    Returns:
        MappingSetDataFrame ready for serialization
    """
    if mapping_set_version is None:
        mapping_set_version = str(datetime.now(timezone.utc).date())

    if mapping_provider is None:
        mapping_provider = "https://github.com/jmillanacosta/rdfsolve"

    # Create metadata dict for the mapping set
    metadata = {
        "mapping_set_id": mapping_set_id,
        "mapping_set_version": mapping_set_version,
        "license": license_uri,
        "mapping_date": str(datetime.now(timezone.utc).date()),
        "mapping_provider": mapping_provider,
        "mapping_tool": mapping_tool,
        "mapping_tool_version": mapping_tool_version,
    }

    if creator_id:
        metadata["creator_id"] = creator_id
    if creator_label:
        metadata["creator_label"] = creator_label
    # Add subject/object source if provided
    if subject_source:
        metadata["subject_source"] = subject_source
    if object_source:
        metadata["object_source"] = object_source

    # Create MappingSetDataFrame from mappings
    msdf = MappingSetDataFrame.from_mappings(
        mappings=list(mappings), converter=converter, metadata=metadata
    )

    # Add custom prefixes for RDFSolve URIs
    msdf.converter.add_prefix("rdfsolve", get_base_uri(), merge=True)
    msdf.converter.add_prefix("orcid", "https://orcid.org/", merge=True)

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


def project_mappings(
    path: str | Path, identities: dict[str, set[str]]
) -> tuple[list[MappingEdge], dict[str, int]]:
    """Expand SSSOM identifiers and project assertions onto matching dataset inventories."""
    from sssom.parsers import parse_sssom_table

    table = parse_sssom_table(Path(path))

    def expand(value: object) -> str:
        """Expand a declared SSSOM CURIE to an absolute IRI."""
        if not isinstance(value, str) or not value:
            raise ValueError("SSSOM identifiers and predicates must be nonempty")
        prefix, separator, local = value.partition(":")
        if separator and prefix in table.prefix_map:
            return table.prefix_map[prefix] + local
        if value.startswith(("http://", "https://", "urn:")):
            return value
        raise ValueError(f"SSSOM prefix is not declared: {value}")

    edges: list[MappingEdge] = []
    unmatched = 0
    for row in table.df.to_dict("records"):
        source, target = expand(row["subject_id"]), expand(row["object_id"])
        predicate = expand(row["predicate_id"])
        left = [name for name, terms in identities.items() if source in terms]
        right = [name for name, terms in identities.items() if target in terms]
        if not left or not right:
            unmatched += 1
            continue
        confidence = row.get("confidence")
        if confidence is not None and isnan(float(confidence)):
            confidence = None
        for a, b in product(left, right):
            edges.append(
                MappingEdge(
                    source_class=source,
                    target_class=target,
                    predicate=predicate,
                    source_dataset=a,
                    target_dataset=b,
                    confidence=confidence,
                    mapping_justification=expand(row["mapping_justification"]),
                    mapping_source=str(table.metadata.get("mapping_set_id") or Path(path).name),
                )
            )
    return edges, {
        "rows": len(table.df),
        "unrepresented_rows": unmatched,
        "projected_edges": len(edges),
    }


def links_to_sssom(
    links: Sequence[LinkEvidence],
    schemas: MappingType[str, MinedSchema],
    mapping_set_id: str,
    *,
    min_share: float = 0.5,
    **metadata: str,
) -> MappingSetDataFrame:
    """Return the verified links with a share of at least *min_share* as a mapping set.

    A link maps its source class to its target class with skos:relatedMatch: the values of
    the source property (subject_match_field) name the target subjects (a join) or the same
    entities as the target property (object_match_field). similarity_score is the share of
    sampled identifiers that the target has; other holds the sample and the target forms (the
    rewrite that applies the link) as JSON. IRIs are written as CURIEs with the prefixes of
    the schemas; a namespace without a prefix gets a new name. Other keyword arguments go to
    create_sssom_mappings (for example creator_id).
    """
    import json

    from curies import Converter

    from rdfsolve._uri import prefix_map
    from rdfsolve.config import mint

    kept = [e for e in links if e.share is not None and e.share >= min_share]
    retained: dict[str, str] = {"rdfsolve": get_base_uri()}
    for schema in schemas.values():
        for prefix, namespace in schema.get_prefixes().items():
            if prefix not in retained and namespace not in retained.values():
                retained[prefix] = namespace
    iris = [mint("dataset", name) for name in schemas]
    for e in kept:
        link = e.link
        iris += [
            link.source_class,
            link.property,
            link.target_class or "",
            link.target_property or "",
        ]
    converter = Converter.from_prefix_map(prefix_map([i for i in iris if i], retained))

    def curie(iri: str | None) -> str | None:
        """Return the CURIE of an IRI; every namespace has a prefix, so none is left long."""
        return converter.compress(iri, strict=True) if iri else None

    rows = []
    for e in kept:
        link = e.link
        rows.append(
            Mapping(
                subject_id=curie(link.source_class),
                subject_type="owl class",
                subject_source=curie(mint("dataset", link.source)),
                subject_match_field=[curie(link.property)],
                predicate_id="skos:relatedMatch",
                object_id=curie(link.target_class),
                object_type="owl class",
                object_source=curie(mint("dataset", link.target)),
                object_match_field=[curie(link.target_property)] if link.target_property else None,
                mapping_justification="semapv:InstanceBasedMatching",
                similarity_score=round(e.share or 0.0, 4),
                similarity_measure="share of sampled source identifiers found in the target",
                comment=f"{link.kind} on {link.identifier_type}: {e.found} of {e.sampled} sampled",
                other=json.dumps(
                    {
                        "kind": link.kind,
                        "identifier_type": link.identifier_type,
                        "sampled": e.sampled,
                        "found": e.found,
                        "population": e.population,
                        "complete": e.complete,
                        "interval": e.interval(),
                        "target_forms": e.target_forms,
                    },
                    sort_keys=True,
                ),
            )
        )
    return create_sssom_mappings(rows, mapping_set_id, converter=converter, **metadata)
