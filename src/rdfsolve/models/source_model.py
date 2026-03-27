"""Pydantic model for a single rdfsolve data-source entry.

Mirrors the :class:`~rdfsolve.sources.SourceEntry` TypedDict but adds
validation, coercion, and serialisation helpers.  Used throughout the
backend to validate data read from ``sources.yaml`` before it is stored
in the database and returned from API routes.

Typical usage::

    from rdfsolve.models.source_model import SourceModel, SourcesRegistry

    models = SourcesRegistry.from_yaml("data/sources.yaml")
    first = models.sources[0]
    print(first.name, first.bioregistry_domain)

    d = first.model_dump()  # round-trip to plain dict
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

__all__ = ["PublicationRef", "SourceModel", "SourcesRegistry"]


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


class SourceModel(BaseModel):
    """Validated model for a single data-source entry from ``sources.yaml``.

    All fields correspond directly to keys in the YAML mapping.
    Bioregistry-derived fields are optional and populated by
    :func:`~rdfsolve.sources.enrich_source_with_bioregistry`.

    Attributes
    ----------
    name:
        Unique source identifier (primary key in the ``sources`` DB table).
    endpoint:
        SPARQL endpoint URL.
    void_iri:
        Optional VoID dataset IRI.
    graph_uris:
        Named graph URIs to restrict queries.
    use_graph:
        Whether to use a GRAPH clause in SPARQL queries.
    two_phase:
        Use two-phase mining (default True).
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
    bioregistry_keywords:
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
    """

    name: str
    endpoint: str = ""
    void_iri: str = ""
    graph_uris: list[str] = Field(default_factory=list)
    use_graph: bool = False
    two_phase: bool = True
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

    bioregistry_prefix: str = ""
    bioregistry_name: str = ""
    bioregistry_description: str = ""
    bioregistry_homepage: str = ""
    bioregistry_license: str = ""
    bioregistry_domain: str = ""
    bioregistry_keywords: list[str] = Field(default_factory=list)
    bioregistry_publications: list[PublicationRef] = Field(default_factory=list)
    bioregistry_uri_prefix: str = ""
    bioregistry_uri_prefixes: list[str] = Field(default_factory=list)
    bioregistry_synonyms: list[str] = Field(default_factory=list)
    bioregistry_mappings: dict[str, str] = Field(default_factory=dict)
    bioregistry_logo: str = ""
    bioregistry_extra_providers: list[dict[str, str | None]] = Field(default_factory=list)

    model_config = {"populate_by_name": True, "extra": "ignore"}

    @field_validator(
        "graph_uris",
        "download_ttl",
        "bioregistry_uri_prefixes",
        "bioregistry_synonyms",
        "bioregistry_keywords",
        mode="before",
    )
    @classmethod
    def _coerce_list(cls, v: Any) -> list[Any]:
        """Ensure list fields are always lists (None → [])."""
        if v is None:
            return []
        if isinstance(v, str):
            return [v]
        return list(v)

    @field_validator("bioregistry_publications", mode="before")
    @classmethod
    def _coerce_publications(cls, v: Any) -> list[Any]:
        """Coerce publication entries; None → []."""
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return []

    @field_validator("bioregistry_mappings", mode="before")
    @classmethod
    def _coerce_mappings(cls, v: Any) -> dict[str, str]:
        """Coerce mappings; None → {}."""
        if v is None:
            return {}
        if isinstance(v, dict):
            return {str(k): str(val) for k, val in v.items()}
        return {}

    @field_validator("bioregistry_extra_providers", mode="before")
    @classmethod
    def _coerce_extra_providers(cls, v: Any) -> list[dict[str, str | None]]:
        """Coerce extra_providers; None → []."""
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
            "bioregistry_prefix",
            "bioregistry_name",
            "bioregistry_description",
            "bioregistry_homepage",
            "bioregistry_license",
            "bioregistry_domain",
            "bioregistry_uri_prefix",
            "bioregistry_logo",
        }
        for field_name in str_fields:
            if data.get(field_name) is None:
                data[field_name] = ""
        return data

    def to_db_dict(self) -> dict[str, Any]:
        """Return a plain dict suitable for :meth:`~rdfsolve.backend.database.Database.save_source`.

        Publications are serialised as list-of-dicts (not Pydantic objects).

        Returns
        -------
        dict[str, Any]
            Dict with all fields, ready for database persistence.
        """
        d = self.model_dump()
        d["bioregistry_publications"] = [p.model_dump() for p in self.bioregistry_publications]
        return d


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
        entries = [SourceModel.model_validate(item) for item in raw]
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
