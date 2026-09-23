"""Config operations for the pipeline command."""

from __future__ import annotations

import importlib.metadata
import json
import logging
import shutil
import subprocess
from dataclasses import dataclass, field, fields
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
    download_error: str = ""

    @property
    def mode(self):
        local = bool(self.graph_sources or self.local_provider or self.download_urls or self.local_tar_url)
        if self.endpoint:
            return SourceMode.BOTH if local else SourceMode.REMOTE
        return SourceMode.LOCAL if local else SourceMode.UNKNOWN

    @classmethod
    def from_dict(cls, value):
        settings = SourceModel.model_validate(value)
        fields = {k: v for k, v in value.items() if k.startswith("download_") and v}
        urls = [url for v in fields.values() for url in ([v] if isinstance(v, str) else v)]
        return cls(
            **settings.model_dump(exclude=set(settings.model_extra or {})),
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
            "graph_sources": self.graph_sources,
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
    ontology_term_budget: int = 300
    discover_ontology_graphs: bool = False
    ontology_discovery_max_graphs: int = 500
    extract_metadata: bool = False
    collect_property_usage_evidence: bool = False
    collect_property_value_profiles: bool = False
    collect_property_value_histograms: bool = False
    collect_declared_artifacts: bool = False

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

        excluded = {
            s.name: "service record" if s.source_role == "service" else "skip_mining"
            for s in sources
            if not s.mining_enabled
        }
        if names and excluded:
            raise ValueError(f"Sources not eligible for mining: {excluded}")
        if excluded:
            log.info("Excluded sources from mining: %s", excluded)
        sources = self._filter_by_health_checks([s for s in sources if s.mining_enabled])

        self.sources = sources
        return sources

    def _filter_by_health_checks(self, sources: list[Source]) -> list[Source]:
        """Attach health observations to each selected access channel."""
        if self.endpoint_status_file and self.endpoint_status_file.exists():
            status = json.loads(self.endpoint_status_file.read_text())
            for source in sources:
                observed = status["endpoints"].get(source.name)
                if observed and observed["status"] != "up":
                    source.endpoint_down = True
                    source.failure_count = max(source.failure_count, 3)
        if self.download_status_file and self.download_status_file.exists():
            status = json.loads(self.download_status_file.read_text())
            for source in sources:
                observed = status["downloads"].get(source.name)
                if observed and observed["status"] not in ("accessible", "redirect"):
                    source.download_error = f"Download health: {observed['status']}"
        return sources

    def effective_config_dict(self) -> dict[str, Any]:
        """Return the effective pipeline configuration in YAML-safe form.

        Loaded source objects are represented by their selected registry names;
        the frozen ``sources.yaml`` remains the authoritative source registry.
        """

        def clean(value):
            if isinstance(value, Path):
                return str(value)
            if isinstance(value, Enum):
                return value.value
            if isinstance(value, tuple):
                return [clean(item) for item in value]
            if isinstance(value, list):
                return [clean(item) for item in value]
            if isinstance(value, dict):
                return {str(key): clean(item) for key, item in value.items()}
            return value

        result: dict[str, Any] = {}
        for item in fields(self):
            if item.name == "sources":
                continue
            result[item.name] = clean(getattr(self, item.name))
        result["selected_sources"] = [source.name for source in self.sources]
        return result

    def archive_run_inputs(self) -> dict[str, str]:
        """Freeze the exact registry/configuration inputs used by this run.

        This is intentionally pipeline infrastructure rather than CLI logic so
        direct ``scripts/pipeline.py`` runs and wrappers produce the same
        provenance files. Existing wrapper-written files are preserved.
        """
        assert self.output_dir is not None
        output = self.output_dir
        output.mkdir(parents=True, exist_ok=True)
        written: dict[str, str] = {}

        def copy_if_present(source: Path | None, name: str) -> None:
            if source is None or not source.exists():
                return
            target = output / name
            if source.resolve() != target.resolve():
                shutil.copy2(source, target)
            written[name] = str(target)

        copy_if_present(self.sources_file, "sources.yaml")
        copy_if_present(self.sssom_sources_file, "sssom_sources.yaml")
        copy_if_present(self.endpoint_status_file, "endpoint_status.json")
        copy_if_present(self.download_status_file, "download_status.json")
        if self.sources_file is not None:
            copy_if_present(
                self.sources_file.with_name("identity_overrides.yaml"), "identity_overrides.yaml"
            )

        config_path = output / "pipeline_config.yaml"
        config_path.write_text(
            yaml.safe_dump(self.effective_config_dict(), sort_keys=True),
            encoding="utf-8",
        )
        written["pipeline_config.yaml"] = str(config_path)

        commit_path = output / "code_commit.txt"
        if not commit_path.exists() and self.repo_dir is not None:
            try:
                commit = subprocess.run(
                    ["git", "-C", str(self.repo_dir), "rev-parse", "HEAD"],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout.strip()
            except (OSError, subprocess.SubprocessError):
                commit = ""
            if commit:
                commit_path.write_text(commit + "\n", encoding="utf-8")
        if commit_path.exists():
            written["code_commit.txt"] = str(commit_path)

        environment_path = output / "environment.txt"
        if not environment_path.exists():
            # Read installed distributions directly; uv environments have no pip.
            frozen = sorted(
                {f"{dist.name}=={dist.version}" for dist in importlib.metadata.distributions()},
                key=str.lower,
            )
            if frozen:
                environment_path.write_text("\n".join(frozen) + "\n", encoding="utf-8")
        if environment_path.exists():
            written["environment.txt"] = str(environment_path)
        return written

    def get_remote_sources(self) -> list[Source]:
        """Get sources to mine remotely, minus those a local index covers."""
        return [
            s
            for s in self.sources
            if s.mining_enabled
            and s.mode in (SourceMode.REMOTE, SourceMode.BOTH)
            and not s.skip_remote
        ]

    def get_local_sources(self) -> list[Source]:
        """Get sources that need local mining."""
        return [
            s
            for s in self.sources
            if s.mining_enabled and s.mode in (SourceMode.LOCAL, SourceMode.BOTH)
        ]
