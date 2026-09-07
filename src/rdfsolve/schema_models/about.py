"""Dataset identity and mining provenance."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt

from rdfsolve.schema_models._constants import _BASE_URI


class AboutMetadata(BaseModel):
    """Provenance metadata attached to every schema export.

    Contains identity, source, provenance, quality, and validation
    information critical for versioned schema management and
    downstream typed-client generation.
    """

    # Identity
    schema_id: str | None = Field(
        None,
        description="Unique identifier (UUID or content hash)",
    )
    schema_version: str = Field(
        default="",
        description="Source release identifier or dated mining snapshot; not the JSON format version",
    )

    # Source
    dataset_name: str | None = Field(
        None,
        description="Human-readable dataset name",
    )
    endpoint: str | None = Field(
        None,
        description="SPARQL endpoint URL",
    )
    graph_uris: list[str] | None = Field(
        None,
        description="Named graph URIs queried",
    )
    discovered_graphs: list[dict[str, Any]] | None = Field(
        None,
        description="All named graphs discovered via discover_all_graphs() with counts",
    )
    ontology_graph_uris: list[str] | None = Field(
        None,
        description="URIs of graphs identified as ontologies (.owl extension)",
    )

    # Provenance
    generated_by: str = Field(
        default="unknown",
        description="Tool and version string",
    )
    generated_at: str = Field(
        default="",
        description="ISO-8601 timestamp (UTC)",
    )
    strategy: str = Field(
        "unknown",
        description="Mining strategy used (e.g. 'qlever_oneshot', 'sparql_paginated', 'void')",
    )

    # Data Versioning (critical for typed-client)
    source_version: str | None = Field(
        None,
        description="Version of the source data if known (e.g. '2024_01', 'v3.2')",
    )
    source_version_iri: str | None = Field(
        None,
        description="Dataset version IRI (owl:versionIRI)",
    )
    source_issued: str | None = Field(
        None,
        description="Publication date of source dataset (ISO-8601, dcat:issued)",
    )
    source_modified: str | None = Field(
        None,
        description="Last-Modified timestamp of source data (ISO-8601)",
    )
    source_license: str | None = Field(
        None,
        description="License URI for the source data",
    )
    source_publisher: str | None = Field(
        None,
        description="Publisher URI or name (dcterms:publisher)",
    )
    source_creator: list[str] | None = Field(
        None,
        description="Creator URIs (dcterms:creator)",
    )
    homepage: str | None = Field(
        None,
        description="Dataset homepage URI (foaf:homepage)",
    )
    title: str | None = Field(
        None,
        description="Dataset title (dcterms:title, overrides dataset_name if present)",
    )
    description: str | None = Field(
        None,
        description="Dataset description (dcterms:description)",
    )

    # Tool Versions
    rdfsolve_version: str | None = Field(
        None,
        description="rdfsolve version string",
    )
    qlever_version: dict[str, str] | None = Field(
        None,
        description=(
            "QLever build info fetched from the endpoint's "
            '?cmd=stats: {"git_hash_server": str, "git_hash_index": str}'
        ),
    )

    # Timing
    started_at: str | None = Field(
        None,
        description="ISO-8601 timestamp when mining started",
    )
    finished_at: str | None = Field(
        None,
        description="ISO-8601 timestamp when mining finished",
    )
    total_duration_s: float | None = Field(
        None,
        ge=0,
        description="Total wall-clock seconds",
    )

    # Statistics
    class_entity_counts: dict[str, NonNegativeInt] = Field(default_factory=dict)
    pattern_count: int = Field(
        0,
        ge=0,
        description="Number of schema patterns",
    )
    class_count: int = Field(
        0,
        ge=0,
        description="Number of distinct classes (declared + used types)",
    )
    declared_class_count: int = Field(
        0,
        ge=0,
        description=(
            "Number of formally declared classes (owl:Class or rdfs:Class). "
            "These have explicit class definitions in the dataset."
        ),
    )
    used_type_count: int = Field(
        0,
        ge=0,
        description=(
            "Number of URIs used as rdf:type values but NOT declared as classes. "
            "Common in LOD: external ontology terms used as types without importing definitions."
        ),
    )
    property_count: int = Field(
        0,
        ge=0,
        description="Number of distinct properties",
    )
    triple_count_estimate: int | None = Field(
        None,
        ge=0,
        description="Estimated total triples in source data",
    )
    distinct_subject_count: int | None = Field(
        None,
        ge=0,
        description="COUNT(DISTINCT ?s) across all patterns",
    )
    distinct_predicate_count: int | None = Field(
        None,
        ge=0,
        description="COUNT(DISTINCT ?p) across all patterns",
    )
    document_count: int | None = Field(
        None,
        ge=0,
        description="Number of RDF documents in the dataset (for local/downloaded datasets, void:documents)",
    )

    # Quality Metrics
    coverage_score: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Fraction of data covered by schema patterns (0.0-1.0)",
    )
    confidence_score: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Overall schema confidence score (0.0-1.0)",
    )

    # Validation
    validation_status: Literal["unvalidated", "auto_validated", "manual_validated"] = Field(
        default="unvalidated",
        description="Validation state of this schema",
    )
    validation_errors: list[str] = Field(
        default_factory=list,
        description="List of validation error messages",
    )

    # Authors
    authors: list[dict[str, str]] | None = Field(
        None,
        description='List of {"name": str, "orcid": str} dicts',
    )

    # Canonical URIs (auto-populated from dataset_name)
    schema_uri: str | None = Field(
        None,
        description="Canonical URI where this schema is served",
    )
    void_uri: str | None = Field(
        None,
        description="Canonical URI where the VoID catalog is served",
    )
    report_uri: str | None = Field(
        None,
        description="Canonical URI where the run report is served",
    )
    linkml_uri: str | None = Field(
        None,
        description="Canonical URI where the LinkML schema is served",
    )

    model_config = ConfigDict(extra="allow")

    @staticmethod
    def build(
        endpoint: str | None = None,
        dataset_name: str | None = None,
        graph_uris: list[str] | None = None,
        pattern_count: int = 0,
        class_count: int = 0,
        declared_class_count: int = 0,
        used_type_count: int = 0,
        property_count: int = 0,
        strategy: str = "unknown",
        started_at: str | None = None,
        finished_at: str | None = None,
        total_duration_s: float | None = None,
        authors: list[dict[str, str]] | None = None,
        qlever_version: dict[str, str] | None = None,
        # Version fields
        schema_version: str | None = None,
        source_version: str | None = None,
        source_version_iri: str | None = None,
        source_issued: str | None = None,
        source_modified: str | None = None,
        source_license: str | None = None,
        source_publisher: str | None = None,
        source_creator: list[str] | None = None,
        homepage: str | None = None,
        title: str | None = None,
        description: str | None = None,
        triple_count_estimate: int | None = None,
        distinct_subject_count: int | None = None,
        distinct_predicate_count: int | None = None,
        document_count: int | None = None,
        coverage_score: float | None = None,
        confidence_score: float | None = None,
    ) -> AboutMetadata:
        """Create metadata with auto-populated version + timestamp."""
        from urllib.parse import quote
        from uuid import uuid4

        from rdfsolve.version import VERSION

        def _uri(suffix: str) -> str | None:
            if not dataset_name:
                return None
            encoded_name = quote(dataset_name, safe="")
            return f"{_BASE_URI}/api/{suffix}/{encoded_name}"

        generated_at = finished_at or datetime.now(timezone.utc).isoformat()
        version = (
            source_version_iri
            or source_version
            or source_modified
            or source_issued
            or schema_version
            or f"snapshot:{generated_at}"
        )
        return AboutMetadata(
            # Identity
            schema_id=str(uuid4()),
            schema_version=version,
            # Source
            dataset_name=dataset_name,
            endpoint=endpoint,
            graph_uris=graph_uris,
            # Provenance
            generated_by=f"rdfsolve {VERSION}",
            generated_at=generated_at,
            strategy=strategy,
            # Data versioning
            source_version=source_version,
            source_version_iri=source_version_iri,
            source_issued=source_issued,
            source_modified=source_modified,
            source_license=source_license,
            source_publisher=source_publisher,
            source_creator=source_creator,
            homepage=homepage,
            title=title,
            description=description,
            # Tool versions
            rdfsolve_version=VERSION,
            qlever_version=qlever_version,
            # Timing
            started_at=started_at,
            finished_at=finished_at,
            total_duration_s=total_duration_s,
            # Statistics
            pattern_count=pattern_count,
            class_count=class_count,
            declared_class_count=declared_class_count,
            used_type_count=used_type_count,
            property_count=property_count,
            triple_count_estimate=triple_count_estimate,
            distinct_subject_count=distinct_subject_count,
            distinct_predicate_count=distinct_predicate_count,
            document_count=document_count,
            # Quality
            coverage_score=coverage_score,
            confidence_score=confidence_score,
            # Authors
            authors=authors,
            # Canonical URIs
            schema_uri=_uri("schemas"),
            void_uri=_uri("void"),
            report_uri=_uri("reports"),
            linkml_uri=_uri("linkml"),
        )
