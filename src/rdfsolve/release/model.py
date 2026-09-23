"""Canonical models for an rdfsolve evidence-corpus release.

The manifest is deliberately plain JSON/Pydantic first.  RDF projections are
secondary views of the same release inventory and must not become the only
place where completion or provenance semantics are represented.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

CompletionState = Literal["complete", "partial", "failed", "unfinished", "skipped", "unknown"]


class ReleaseArtifact(BaseModel):
    """One file of a release, identified by its content hash."""

    artifact_id: str
    path: str
    sha256: str
    byte_size: int = Field(ge=0)
    media_type: str | None = None
    dataset_id: str | None = None
    role: str | None = None
    generated_by: str | None = None
    derived_from: list[str] = Field(default_factory=list)


class OntologyReleaseRef(BaseModel):
    """An archived ontology artifact referenced by a release."""

    ontology_id: str | None = None
    artifact_id: str | None = None
    source_url: str | None = None
    source_graph: str | None = None
    version_iris: list[str] = Field(default_factory=list)
    version_values: list[str] = Field(default_factory=list)
    retrieved_at: str | None = None
    sha256: str | None = None


class OntologyUsageReleaseRecord(BaseModel):
    """Ontology usage of one namespace in one dataset of a release."""

    namespace: str
    ontology_id: str | None = None
    identity_basis: str = "unresolved"
    observed_class_count: int = 0
    observed_property_count: int = 0
    graph_evidence_count: int = 0
    reference_source_count: int = 0
    ontology_artifact_id: str | None = None
    artifact_relation: str | None = None
    version_match_status: str | None = None
    resolved_class_count: int | None = None
    unresolved_class_count: int | None = None
    resolved_property_count: int | None = None
    unresolved_property_count: int | None = None
    artifact_refs: list[OntologyReleaseRef] = Field(default_factory=list)


class ExtractionReleaseRecord(BaseModel):
    """One mining attempt retained for a dataset snapshot."""

    mode: str
    completion_state: CompletionState = "unknown"
    report_path: str | None = None
    snapshot_id: str | None = None
    schema_path: str | None = None
    schema_artifact_id: str | None = None
    graph_scope: list[str] = Field(default_factory=list)
    type_context_graph_scope: list[str] = Field(default_factory=list)
    ontology_graph_scope: list[str] = Field(default_factory=list)
    endpoint: str | None = None
    retrieved_at: str | None = None


class DatasetReleaseRecord(BaseModel):
    """One dataset snapshot of a release and its artifacts."""

    dataset_id: str
    snapshot_id: str | None = None
    source_version: str | None = None
    source_version_iri: str | None = None
    retrieved_at: str | None = None
    endpoint: str | None = None
    distributions: list[str] = Field(default_factory=list)
    access_files: dict[str, list[str]] = Field(default_factory=dict)
    graph_scope: list[str] = Field(default_factory=list)
    graph_sources: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    extraction_mode: str | None = None
    completion_state: CompletionState = "unknown"
    report_path: str | None = None
    extractions: list[ExtractionReleaseRecord] = Field(default_factory=list)
    artifacts: list[str] = Field(default_factory=list)
    ontology_evidence_context: str | None = None
    local_ontology_file_candidate_count: int = 0
    ontology_usages: list[OntologyUsageReleaseRecord] = Field(default_factory=list)


class ReleaseManifest(BaseModel):
    """Inventory and provenance of one frozen release."""

    release_id: str
    base_uri: str = "https://w3id.org/rdfsolve/"
    issued: datetime
    rdfsolve_version: str | None = None
    code_commit: str | None = None
    run_root: str
    source_registry_artifact: str | None = None
    environment_artifact: str | None = None
    pipeline_config_artifacts: list[str] = Field(default_factory=list)
    identity_overrides_artifact: str | None = None
    identity_review_complete: bool | None = None
    identity_candidate_count: int | None = None
    canonical_dataset_count: int | None = None
    identity_review_error: str | None = None
    ontology_registry_artifact: str | None = None
    service_records: list[str] = Field(default_factory=list)
    datasets: list[DatasetReleaseRecord] = Field(default_factory=list)
    artifacts: list[ReleaseArtifact] = Field(default_factory=list)

    def artifact_by_id(self) -> dict[str, ReleaseArtifact]:
        """Return the artifacts keyed by artifact_id."""
        return {item.artifact_id: item for item in self.artifacts}
