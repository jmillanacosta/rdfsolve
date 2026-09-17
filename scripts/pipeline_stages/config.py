"""Config operations for the pipeline command."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field

from rdfsolve.models.source_model import SourceModel
from rdfsolve.schema_models._constants import SUGGESTED_SERVICE_GRAPHS

log = logging.getLogger(__name__)


class SourceMode(Enum):
    """How to mine a source."""

    REMOTE = "remote"  # Query SPARQL endpoint directly
    LOCAL = "local"  # Download + QLever index + mine
    BOTH = "both"  # Has both endpoint and local_provider
    UNKNOWN = "unknown"


class Source(SourceModel):
    """Validated source with pipeline download locations."""

    download_urls: list[str] = Field(default_factory=list)
    download_fields: dict[str, Any] = Field(default_factory=dict)
    local_tar_url: str | None = None

    @property
    def mode(self):
        local = bool(self.local_provider or self.download_urls or self.local_tar_url)
        if self.endpoint:
            return SourceMode.BOTH if local else SourceMode.REMOTE
        return SourceMode.LOCAL if local else SourceMode.UNKNOWN

    @classmethod
    def from_dict(cls, value):
        settings = SourceModel.model_validate(value)
        fields = {k: v for k, v in value.items() if k.startswith("download_") and v}
        urls = [url for v in fields.values() for url in ([v] if isinstance(v, str) else v)]
        return cls(
            **settings.model_dump(),
            download_urls=urls,
            download_fields=fields,
            local_tar_url=value.get("local_tar_url"),
        )

    def qlever_entry(self):
        """Keep download formats and selected graphs at the QLever boundary."""
        return {
            "name": self.name,
            **self.download_fields,
            "local_tar_url": self.local_tar_url,
            "graph_uris": self.graph_uris,
        }


@dataclass
class PipelineConfig:
    """Pipeline configuration."""

    base_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parents[3])
    repo_dir: Path | None = None
    data_dir: Path | None = None
    output_dir: Path | None = None
    results_dir: Path | None = None
    log_dir: Path | None = None

    sources_file: Path | None = None
    sssom_sources_file: Path | None = None
    sources: list[Source] = field(default_factory=list)

    get_graphs_from_store: bool = False
    graph_store_urls: dict[str, str] = field(default_factory=dict)

    timeout: float | None = None  # Override the source timeout only when set.
    delay: float = 1.0
    chunk_size: int = 10000
    class_batch_size: int = 15
    class_chunk_size: int | None = None
    max_response_bytes: int = 64 * 1024 * 1024
    benchmark: bool = True
    enrich: bool = True
    examples_per_pattern: int = 1
    trim_descriptions: int | None = None
    navigation_hops: int = 5
    navigation_min_hops: int = 3
    navigation_limit: int = 100
    navigation_probes: int = 0
    void_base_url: str = "https://rdfsolve.bigcat-bioinformatics.nl"

    qlever_image: str = "docker://docker.io/adfreiburg/qlever:latest"
    base_port: int = 7019
    qlever_startup_timeout: int = 600  # Seconds to load the index.
    no_download: bool = False
    no_index: bool = False

    skip_remote: bool = False
    skip_local: bool = False
    skip_mining: bool = False
    skip_mappings: bool = False
    skip_seeding: bool = False
    skip_inference: bool = False
    skip_completed: bool = False  # Skip sources with existing output files

    extract_ontology: bool = False
    ontology_scope: str = "schema"
    ontology_as_data: bool = False
    extract_metadata: bool = False

    parallelism: int = 4
    exclude_graph_prefixes: tuple[str, ...] = SUGGESTED_SERVICE_GRAPHS

    output_suffix: str = ""

    output_formats: list[str] = field(default_factory=lambda: ["json-ld", "void"])

    endpoint_status_file: Path | None = None
    download_status_file: Path | None = None

    def __post_init__(self):
        if self.repo_dir is None:
            self.repo_dir = self.base_dir / "rdfsolve-2"
        if self.data_dir is None:
            self.data_dir = self.base_dir / "data"
        if self.output_dir is None:
            self.output_dir = self.base_dir / "output"
        if self.results_dir is None:
            self.results_dir = self.base_dir / "results"
        if self.log_dir is None:
            self.log_dir = self.base_dir / "logs"
        if self.sources_file is None:
            self.sources_file = self.repo_dir / "data" / "sources.yaml"
        if self.sssom_sources_file is None:
            self.sssom_sources_file = self.repo_dir / "data" / "sssom_sources.yaml"

    def load_sources(
        self,
        names: list[str] | None = None,
        skip_providers: list[str] | None = None,
    ) -> list[Source]:
        """Load sources from YAML file.

        Parameters
        ----------
        names
            If provided, only load sources with these names.
        skip_providers
            If provided, skip sources with these local_provider values.
            E.g., ["idsm"] to skip all IDSM-hosted sources.
        """
        if not self.sources_file.exists():
            raise FileNotFoundError(f"Sources file not found: {self.sources_file}")

        with open(self.sources_file) as f:
            raw = yaml.safe_load(f) or []

        sources = [Source.from_dict(d) for d in raw]

        if names:
            missing = sorted(set(names) - {s.name for s in sources})
            if missing:
                raise ValueError(f"Unknown source names: {missing}")
            sources = [s for s in sources if s.name in names]

        if skip_providers:
            skip_set = {p.lower() for p in skip_providers}
            before = len(sources)
            sources = [
                s
                for s in sources
                if not (s.local_provider and s.local_provider.lower() in skip_set)
            ]
            skipped = before - len(sources)
            if skipped:
                log.info(f"Skipped {skipped} sources from providers: {skip_providers}")

        from collections import Counter

        duplicates = sorted(
            name for name, count in Counter(s.name for s in sources).items() if count > 1
        )
        if duplicates:
            raise ValueError(
                f"Duplicate source names would overwrite outputs: {duplicates}. Select a registry with unique names."
            )

        sources = self._filter_by_health_checks(sources)

        self.sources = sources
        return sources

    def _filter_by_health_checks(self, sources: list[Source]) -> list[Source]:
        filtered = sources

        if self.endpoint_status_file and self.endpoint_status_file.exists():
            with open(self.endpoint_status_file) as f:
                status = json.load(f)
            down = {name for name, data in status["endpoints"].items() if data["status"] != "up"}
            before = len(filtered)
            filtered = [s for s in filtered if s.name not in down]
            skipped = before - len(filtered)
            if skipped:
                log.info(f"Skipped {skipped} sources with down endpoints")

        if self.download_status_file and self.download_status_file.exists():
            with open(self.download_status_file) as f:
                status = json.load(f)
            broken = {
                name
                for name, data in status["downloads"].items()
                if data["status"] not in ("accessible", "redirect")
            }
            before = len(filtered)
            filtered = [s for s in filtered if s.name not in broken]
            skipped = before - len(filtered)
            if skipped:
                log.info(f"Skipped {skipped} sources with broken downloads")

        return filtered

    def get_remote_sources(self) -> list[Source]:
        """Get sources that can be mined remotely."""
        return [s for s in self.sources if s.mode in (SourceMode.REMOTE, SourceMode.BOTH)]

    def get_local_sources(self) -> list[Source]:
        """Get sources that need local mining."""
        return [s for s in self.sources if s.mode in (SourceMode.LOCAL, SourceMode.BOTH)]
