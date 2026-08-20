"""Discover class-to-class mappings from schema patterns.

Analyzes mined schemas to find cross-dataset class relationships and
generates SSSOM mapping sets documenting interoperability points.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sssom import Mapping, MappingSetDataFrame

from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.sssom_generator import create_sssom_mappings

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema

# SemapV vocabulary for mapping justification
SEMAPV_STRUCTURAL = "https://w3id.org/semapv/vocab/StructuralMatching"

# Metadata properties to exclude from mapping discovery
_METADATA_PROPERTIES = {
    "http://rdfs.org/ns/void#property",
    "http://rdfs.org/ns/void#class",
    "http://rdfs.org/ns/void#propertyPartition",
    "http://rdfs.org/ns/void#classPartition",
    "http://rdfs.org/ns/void#vocabulary",  # VoID vocabulary usage metadata
    "http://ldf.fi/void-ext#subjectClass",
    "http://ldf.fi/void-ext#objectClass",
    "http://ldf.fi/void-ext#datatype",  # VoID-ext datatype metadata
    "http://ldf.fi/void-ext#datatypePartition",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
    "http://www.w3.org/2000/01/rdf-schema#subClassOf",
    "http://www.w3.org/2000/01/rdf-schema#domain",
    "http://www.w3.org/2000/01/rdf-schema#range",
}


def _is_metadata_property(prop_uri: str) -> bool:
    """Check if property is metadata/structural (should skip for mappings)."""
    if prop_uri in _METADATA_PROPERTIES:
        return True
    # Skip internal schema partition URIs
    return "rdfsolve" in prop_uri and "/schema#" in prop_uri


def _is_metadata_class(class_uri: str) -> bool:
    """Check if class URI is metadata/structural (should skip for mappings)."""
    # Skip rdfsolve dataset URIs
    if "rdfsolve" in class_uri and "/dataset/" in class_uri:
        return True
    # Skip rdfsolve schema partition URIs
    if "rdfsolve" in class_uri and "/schema#" in class_uri:
        return True
    # Skip namespace URIs (ending with / or #)
    if class_uri.endswith(("/", "#")):
        return True

    # Skip common metadata vocabulary namespaces
    metadata_namespaces = (
        "http://purl.org/dc/elements/1.1/",
        "http://purl.org/dc/terms/",
        "http://purl.org/pav/",
        "http://xmlns.com/foaf/0.1/",
        "http://rdfs.org/ns/void#",
        "http://www.w3.org/ns/dcat#",
        "http://www.w3.org/2001/XMLSchema#",  # XSD datatypes
        "http://www.w3.org/2000/01/rdf-schema#",  # RDFS
        "http://www.w3.org/2002/07/owl#",  # OWL
    )

    return any(class_uri.startswith(ns) for ns in metadata_namespaces)


def discover_schema_pattern_mappings(
    schemas: list[tuple[str, MinedSchema]],
    dataset_void_uris: dict[str, str],
    creator_id: str | None = None,
    creator_label: str | None = None,
) -> dict[tuple[str, str], MappingSetDataFrame]:
    """Discover cross-dataset class mappings from schema patterns.

    Analyzes patterns where classes from one dataset appear in another
    dataset's schema patterns, creating SSSOM mapping sets per dataset pair.

    Args:
        schemas: List of (dataset_name, MinedSchema) tuples
        dataset_void_uris: Mapping of dataset_name -> VoID dataset URI

    Returns:
        Dict keyed by (dataset1, dataset2) pairs containing MappingSetDataFrame
    """
    # Map class URI -> datasets using it
    class_to_datasets: dict[str, set[str]] = {}

    for ds_name, schema in schemas:
        for pat in schema.patterns:
            if (
                pat.subject_class
                and pat.subject_class not in _SENTINEL_OBJECTS
                and not _is_metadata_class(pat.subject_class)
            ):
                class_to_datasets.setdefault(pat.subject_class, set()).add(ds_name)
            if (
                pat.object_class
                and pat.object_class not in _SENTINEL_OBJECTS
                and not _is_metadata_class(pat.object_class)
            ):
                class_to_datasets.setdefault(pat.object_class, set()).add(ds_name)

    # Build SSSOM mapping sets per dataset pair
    mapping_sets: dict[tuple[str, str], list[Mapping]] = {}

    for ds_name, schema in schemas:
        for pat in schema.patterns:
            # Skip metadata properties
            if _is_metadata_property(pat.property_uri):
                continue

            # Skip patterns without proper classes
            if not pat.subject_class or pat.subject_class in _SENTINEL_OBJECTS:
                continue
            if not pat.object_class or pat.object_class in _SENTINEL_OBJECTS:
                continue

            # Skip metadata classes
            if _is_metadata_class(pat.subject_class) or _is_metadata_class(pat.object_class):
                continue

            # Skip if subject and object are same class (no mapping value)
            if pat.subject_class == pat.object_class:
                continue

            # Find other datasets using this object class
            obj_datasets = class_to_datasets.get(pat.object_class, set())
            cross_dataset = obj_datasets - {ds_name}

            if not cross_dataset:
                continue

            for target_ds in sorted(cross_dataset):
                ds1, ds2 = sorted([ds_name, target_ds])
                pair_key = (ds1, ds2)

                if pair_key not in mapping_sets:
                    mapping_sets[pair_key] = []

                # Use skos:relatedMatch for schema pattern co-occurrence
                # (classes co-occur in patterns but may not be semantically identical)
                predicate = "http://www.w3.org/2004/02/skos/core#relatedMatch"

                # Avoid duplicate mappings
                existing = [
                    m
                    for m in mapping_sets[pair_key]
                    if m.subject_id == pat.subject_class and m.object_id == pat.object_class
                ]

                if not existing:
                    mapping = Mapping(
                        subject_id=pat.subject_class,
                        predicate_id=predicate,
                        object_id=pat.object_class,
                        mapping_justification=SEMAPV_STRUCTURAL,
                        confidence=0.75,
                        comment=f"Co-occurs in schema patterns via {pat.property_uri}",
                    )
                    mapping_sets[pair_key].append(mapping)

    # Convert to MappingSetDataFrame objects
    result: dict[tuple[str, str], MappingSetDataFrame] = {}
    for pair_key, mappings in mapping_sets.items():
        mapping_set_id = (
            f"https://w3id.org/rdfsolve/mappings/"
            f"{pair_key[0]}-{pair_key[1]}-schema-patterns.sssom.tsv"
        )

        msdf = create_sssom_mappings(
            mappings=mappings,
            mapping_set_id=mapping_set_id,
            mapping_set_version=str(datetime.now(UTC).date()),
            subject_source=dataset_void_uris.get(pair_key[0]),
            object_source=dataset_void_uris.get(pair_key[1]),
            creator_id=creator_id,
            creator_label=creator_label,
        )
        result[pair_key] = msdf

    return result
