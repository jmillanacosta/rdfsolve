"""Pydantic models for data source entries with validation and serialization."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from typing_extensions import Self

__all__ = ["DatasetKind", "PublicationRef", "SourceModel", "SourcesRegistry", "SparqlExamples"]

DatasetKind = Literal["instance", "ontology", "unknown"]


class PublicationRef(BaseModel):
    """A literature reference attached to a bioregistry resource.

    Attributes
    ----------
    pubmed:
        PubMed identifier string (or None).
    doi:
        Digital Object Identifier string (or None).
    pmc:
        PubMed Central identifier string (or None).
    title:
        Article title string (or None).
    """

    pubmed: str | None = None
    doi: str | None = None
    pmc: str | None = None
    title: str | None = None


class SparqlExamples(BaseModel):
    """Locations of published query examples, separate from the data graph."""

    model_config = {"extra": "forbid"}
    shacl_graph_in_endpoint: list[str] = Field(default_factory=list)
    shacl_dumps: list[str] = Field(default_factory=list)
    link_to_repository: str = ""

    @field_validator("shacl_graph_in_endpoint", "shacl_dumps", mode="before")
    @classmethod
    def locations(cls, value: Any) -> Any:
        """Accept one location or a list without changing their order."""
        return [value] if isinstance(value, str) else value or []

    @model_validator(mode="after")
    def validate_locations(self) -> Self:
        """Validate graph identities and explicit HTTP repository links."""
        from rdfsolve.schema_models.paths import absolute_iri

        for iri in self.shacl_graph_in_endpoint:
            absolute_iri(iri)
        if any(not item.strip() for item in self.shacl_dumps):
            raise ValueError("Example dump locations must be nonempty")
        if self.link_to_repository:
            absolute_iri(self.link_to_repository)
            if not self.link_to_repository.startswith(("https://", "http://")):
                raise ValueError("Use an HTTP repository URL")
        return self


class SourceModel(BaseModel):
    """Validated model for a single data-source entry from ``sources.yaml``.

    All fields correspond directly to keys in the YAML mapping.
    Bioregistry-derived fields are optional and populated by
    :func:`~rdfsolve.sources.enrich_source_with_bioregistry`.

    Attributes
    ----------
    name:
        Unique source identifier.
    source_role:
        Dataset or access service.
    dataset_kind:
        Curated instance or ontology resource classification; unknown until reviewed.
    skip_mining:
        Exclude this entry from pipeline mining.
    endpoint:
        SPARQL endpoint URL.
    void_iri:
        Optional VoID dataset IRI.
    graph_uris:
        Named graphs that hold data edges.
    type_context_graph_uris:
        Extra named graphs for subject and object types.
    ontology_graph_uris:
        Named graphs for ontology interpretation and extraction.
    graph_sources:
        Download fields keyed by the named graph that receives their triples.
    chunk_size:
        Mining chunk size (None = default).
    class_batch_size:
        Class batch size (None = default).
    class_chunk_size:
        Class chunk size (None = default).
    timeout:
        Per-request timeout seconds (None = default).
    delay:
        Inter-request delay seconds (None = default).
    counts:
        Whether to mine instance counts.
    unsafe_paging:
        Use offset paging even on endpoints that don't support it well.
    notes:
        Free-text notes about the source.
    local_provider:
        Optional Bioregistry provider code for prefix resolution.
    download_ttl:
        Optional list of TTL download URLs for local loading.
    bioregistry_prefix:
        Canonical Bioregistry prefix.
    bioregistry_name:
        Human-readable dataset name from Bioregistry.
    bioregistry_description:
        Dataset description from Bioregistry.
    bioregistry_homepage:
        Dataset homepage URL.
    bioregistry_license:
        SPDX license identifier or URL.
    bioregistry_domain:
        Dataset domain (e.g. ``"chemical"``, ``"biology"``).
    keywords:
        Keyword tags from Bioregistry.
    bioregistry_publications:
        Literature references from Bioregistry.
    bioregistry_uri_prefix:
        Canonical URI prefix for entity IRIs.
    bioregistry_uri_prefixes:
        All known URI prefixes for entity IRIs.
    bioregistry_synonyms:
        Alternative prefix names / synonyms.
    bioregistry_mappings:
        Cross-reference mappings to other registries.
    bioregistry_logo:
        URL of the dataset logo image.
    bioregistry_extra_providers:
        Additional provider entries from Bioregistry.
    kg_registry_id:
        Resource identifier in the KG-Registry.
    in_kamdar:
        Whether the resource is in the Kamdar et al. LSLOD analysis.
    terminology_nomenclature:
        Topic tags for terminology and nomenclature resources.
    """

    name: str
    aliases: list[str] = Field(default_factory=list)
    source_role: Literal["dataset", "service"] = "dataset"
    dataset_kind: DatasetKind = "unknown"
    skip_mining: bool = False
    endpoint: str = ""
    sparql_examples: SparqlExamples | None = None
    dataset_metadata: dict[str, Any] | None = None
    metadata_graph_uris: list[str] | None = None
    enrichment: dict[str, Any] | None = None
    void_graphs: list[str] | None = None
    void_schema: list[str] | None = None
    void_default_graph: bool | None = None
    has_void: bool | None = None
    has_void_partitions: bool | None = None
    has_void_patterns: bool | None = None
    void_iri: str = ""
    graph_uris: list[str] = Field(default_factory=list)
    type_context_graph_uris: list[str] = Field(default_factory=list)
    ontology_graph_uris: list[str] = Field(default_factory=list)
    graph_sources: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    skip_remote: bool = False
    chunk_size: int | None = None
    class_batch_size: int | None = None
    class_chunk_size: int | None = None
    timeout: float | None = None
    delay: float | None = None
    counts: bool = False
    unsafe_paging: bool = False
    notes: str = ""
    local_provider: str = ""
    download_ttl: list[str] = Field(default_factory=list)

    # Endpoint metadata (populated by probe/discovery scripts)
    sparql_engine: str = ""
    sparql_strategy: str = ""
    supports_graph: bool | None = None
    endpoint_down: bool = False
    endpoint_status: str = "unknown"  # "up", "down", "timeout", "rate_limited", "unknown"
    last_checked: str = ""  # ISO timestamp
    last_success: str = ""  # ISO timestamp
    last_error: str = ""  # Last error message
    failure_count: int = 0  # Consecutive failures
    avg_response_time: float | None = None  # Seconds

    bioregistry_prefix: str = ""
    bioregistry_name: str = ""
    bioregistry_description: str = ""
    bioregistry_homepage: str = ""
    bioregistry_license: str = ""
    bioregistry_domain: str = ""
    keywords: list[str] = Field(default_factory=list)
    bioregistry_publications: list[PublicationRef] = Field(default_factory=list)
    bioregistry_uri_prefix: str = ""
    bioregistry_uri_prefixes: list[str] = Field(default_factory=list)
    bioregistry_synonyms: list[str] = Field(default_factory=list)
    bioregistry_mappings: dict[str, str] = Field(default_factory=dict)
    bioregistry_logo: str = ""
    bioregistry_extra_providers: list[dict[str, str | None]] = Field(default_factory=list)
    bioregistry_repository: str = ""
    bioregistry_owl_download: str = ""
    bioregistry_rdf_download: str = ""
    bioregistry_obo_download: str = ""
    bioregistry_enriched_at: str = ""
    bioregistry_package_version: str = ""

    kg_registry_id: str = ""
    in_kamdar: bool = False
    terminology_nomenclature: list[str] = Field(default_factory=list)

    # Keep registry fields without a typed attribute, such as download_* URL lists.
    model_config = {"populate_by_name": True, "extra": "allow"}

    @model_validator(mode="after")
    def validate_graph_sources(self) -> Self:
        """Require an input mapping for every selected local graph."""
        if not self.graph_sources:
            return self
        from rdfsolve.schema_models.paths import absolute_iri

        scopes = set(self.graph_uris + self.type_context_graph_uris + self.ontology_graph_uris)
        if not self.graph_uris or scopes != set(self.graph_sources):
            raise ValueError("graph_sources must match the data, type and ontology graph scopes")
        if self.download_ttl or any(
            value
            for key, value in (self.model_extra or {}).items()
            if key.startswith("download_") or key == "local_tar_url"
        ):
            raise ValueError("Place all downloads inside graph_sources")
        for graph, fields in self.graph_sources.items():
            absolute_iri(graph)
            if not fields:
                raise ValueError(f"No inputs for {graph}")
            for key, urls in fields.items():
                if key not in {
                    "download_ttl",
                    "download_nt",
                    "download_rdf",
                    "download_rdfxml",
                    "download_owl",
                }:
                    raise ValueError(f"Unsupported graph input format: {key}")
                if not urls or any(not url.strip() for url in urls):
                    raise ValueError(f"Empty input locations for {graph}")
        return self

    @property
    def mining_enabled(self) -> bool:
        """Return whether this entry permits pipeline mining."""
        return self.source_role == "dataset" and not self.skip_mining

    @field_validator(
        "aliases",
        "graph_uris",
        "type_context_graph_uris",
        "ontology_graph_uris",
        "download_ttl",
        "bioregistry_uri_prefixes",
        "bioregistry_synonyms",
        "keywords",
        "terminology_nomenclature",
        mode="before",
    )
    @classmethod
    def _coerce_list(cls, v: Any) -> list[Any]:
        """Ensure list fields are always lists (None -> [])."""
        if v is None:
            return []
        if isinstance(v, str):
            return [v]
        return list(v)

    @field_validator("bioregistry_publications", mode="before")
    @classmethod
    def _coerce_publications(cls, v: Any) -> list[Any]:
        """Coerce publication entries; None -> []."""
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return []

    @field_validator("bioregistry_mappings", mode="before")
    @classmethod
    def _coerce_mappings(cls, v: Any) -> dict[str, str]:
        """Coerce mappings; None -> {}."""
        if v is None:
            return {}
        if isinstance(v, dict):
            return {str(k): str(val) for k, val in v.items()}
        return {}

    @field_validator("bioregistry_extra_providers", mode="before")
    @classmethod
    def _coerce_extra_providers(cls, v: Any) -> list[dict[str, str | None]]:
        """Coerce extra_providers; None -> []."""
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return []

    @model_validator(mode="before")
    @classmethod
    def _stringify_none_strings(cls, data: Any) -> Any:
        """Replace None for string fields with empty string."""
        if not isinstance(data, dict):
            return data
        str_fields = {
            "endpoint",
            "void_iri",
            "notes",
            "local_provider",
            "sparql_engine",
            "sparql_strategy",
            "endpoint_status",
            "last_checked",
            "last_success",
            "last_error",
            "bioregistry_prefix",
            "bioregistry_name",
            "bioregistry_description",
            "bioregistry_homepage",
            "bioregistry_license",
            "bioregistry_domain",
            "bioregistry_uri_prefix",
            "bioregistry_logo",
            "bioregistry_repository",
            "bioregistry_owl_download",
            "bioregistry_rdf_download",
            "bioregistry_obo_download",
            "bioregistry_enriched_at",
            "bioregistry_package_version",
            "kg_registry_id",
        }
        for field_name in str_fields:
            if data.get(field_name) is None:
                data[field_name] = ""
        return data


class SourcesRegistry(BaseModel):
    """Container for a list of :class:`SourceModel` instances.

    Attributes
    ----------
    sources:
        All validated source entries.
    """

    sources: list[SourceModel] = Field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: str | Path) -> SourcesRegistry:
        """Load and validate all source entries from a YAML file.

        Parameters
        ----------
        path:
            Path to ``sources.yaml`` (a YAML list of source dicts).

        Returns
        -------
        SourcesRegistry
            Validated registry.

        Raises
        ------
        FileNotFoundError
            If *path* does not exist.
        ValueError
            If the YAML root is not a list.
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"sources YAML not found: {p}")
        with p.open(encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        if not isinstance(raw, list):
            raise ValueError(f"Expected a YAML list in {p}, got {type(raw).__name__}")
        from rdfsolve.source_metadata import with_source_metadata

        entries = [SourceModel.model_validate(item) for item in with_source_metadata(raw, p)]
        return cls(sources=entries)

    def by_name(self, name: str) -> SourceModel | None:
        """Return the source with the given *name*, or None.

        Parameters
        ----------
        name:
            Source name (primary key).

        Returns
        -------
        SourceModel or None
        """
        for s in self.sources:
            if s.name == name:
                return s
        return None

    def filter_by_domain(self, domain: str) -> list[SourceModel]:
        """Return all sources whose ``bioregistry_domain`` equals *domain*.

        Parameters
        ----------
        domain:
            Domain string (e.g. ``"chemical"``).

        Returns
        -------
        list[SourceModel]
        """
        return [s for s in self.sources if s.bioregistry_domain == domain]
