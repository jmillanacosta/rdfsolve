"""Canonical models for an rdfsolve evidence-corpus release.

The manifest is deliberately plain JSON/Pydantic first.  RDF projections are
secondary views of the same release inventory and must not become the only
place where completion or provenance semantics are represented.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from rdfsolve.models.source_model import DatasetKind

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
    # Declared identities: whether some statements are flagged by the identity checks
    identity_check: Literal["flagged_statements", "no_flagged_statements"] | None = None
    identity_check_note: str | None = None


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


class GraphPartExtraction(BaseModel):
    """The schema of one data graph of a source from one extraction of the source."""

    mode: str
    # edge_graph_split, mined_with_graph_settings, void_scoped_to_graph or not_in_void
    derivation: str
    completion_state: CompletionState = "unknown"
    schema_path: str | None = None
    schema_artifact_id: str | None = None
    snapshot_id: str | None = None


class GraphPartReleaseRecord(BaseModel):
    """One data graph of a source mined across several graphs, with its own schemas.

    ``registry_entry`` names the registry entry that is this graph of the source (a graph
    scope), whose own record points back with ``graph_part_of``.
    """

    graph_uri: str
    name: str
    registry_entry: str | None = None
    classes_as_data: bool = False
    membership_properties: list[str] = Field(default_factory=list)
    own_settings: bool = False
    extractions: list[GraphPartExtraction] = Field(default_factory=list)
    # The pin of the source's inputs (the index is the source's), and the pinned files that
    # hold this graph (graph_sources), by their path in it
    input_manifest_artifact: str | None = None
    input_paths: list[str] = Field(default_factory=list)


class InputDownloadRecord(BaseModel):
    """One downloaded file that a local index was built from, pinned when it was downloaded."""

    url: str
    final_url: str | None = None
    path: str | None = None
    sha256: str | None = None
    byte_size: int | None = Field(default=None, ge=0)
    last_modified: str | None = None
    etag: str | None = None
    release_version: str | None = None
    release_metalink: str | None = None
    # The file compared with the size and hashes of its publisher's metalink
    publisher_check: Literal["match", "mismatch", "unchecked"] | None = None
    # Its path in the run's input archive (ReleaseManifest.input_archive), when it was archived
    archive_path: str | None = None


class InputArchiveRecord(BaseModel):
    """Where the downloaded inputs of a run are kept (rdfsolve.release.input_archive)."""

    packaging: str
    # The bag's path or, once deposited, its URL or DOI
    location: str
    created: str | None = None
    # SHA-256 of the bag's manifest-sha256.txt, which lists each file's pinned SHA-256
    manifest_sha256: str
    file_count: int = Field(ge=0)
    byte_size: int = Field(ge=0)
    record_artifact: str | None = None


class DatasetReleaseRecord(BaseModel):
    """One dataset snapshot of a release and its artifacts."""

    dataset_id: str
    dataset_kind: DatasetKind = "unknown"
    snapshot_id: str | None = None
    source_version: str | None = None
    source_version_iri: str | None = None
    retrieved_at: str | None = None
    endpoint: str | None = None
    distributions: list[str] = Field(default_factory=list)
    access_files: dict[str, list[str]] = Field(default_factory=dict)
    graph_scope: list[str] = Field(default_factory=list)
    graph_sources: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    # Graphs whose local inputs are a sample of the endpoint's graph, with how they were sampled.
    sampled_graphs: dict[str, str] = Field(default_factory=dict)
    # The pin of the files a local index was built from (<dataset>_inputs.json): every file
    # with its SHA-256 is in the artifact; the downloads are listed here.
    input_manifest_artifact: str | None = None
    input_manifest_path: str | None = None
    inputs_recorded: Literal["before_index", "after_index"] | None = None
    input_file_count: int | None = None
    input_byte_size: int | None = None
    input_release_versions: list[str] = Field(default_factory=list)
    input_downloads: list[InputDownloadRecord] = Field(default_factory=list)
    extraction_mode: str | None = None
    completion_state: CompletionState = "unknown"
    report_path: str | None = None
    extractions: list[ExtractionReleaseRecord] = Field(default_factory=list)
    artifacts: list[str] = Field(default_factory=list)
    ontology_evidence_context: str | None = None
    local_ontology_file_candidate_count: int = 0
    ontology_usages: list[OntologyUsageReleaseRecord] = Field(default_factory=list)
    # The schemas of each data graph of a source mined across several graphs (rdfsolve.graph_parts)
    graph_parts: list[GraphPartReleaseRecord] = Field(default_factory=list)
    # The source whose per-graph schema of this entry's graph is this entry's schema
    graph_part_of: str | None = None


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
    # The copy of the downloaded inputs of the local indexes, when one was made
    input_archive: InputArchiveRecord | None = None
    datasets: list[DatasetReleaseRecord] = Field(default_factory=list)
    artifacts: list[ReleaseArtifact] = Field(default_factory=list)

    def artifact_by_id(self) -> dict[str, ReleaseArtifact]:
        """Return the artifacts keyed by artifact_id."""
        return {item.artifact_id: item for item in self.artifacts}
