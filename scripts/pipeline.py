#!/usr/bin/env python
"""RDFSolve Analysis Pipeline - Comprehensive pipeline for LOD cloud analysis.

This pipeline handles:
- Remote mining: Query SPARQL endpoints directly
- Local mining: Download RDF dumps, index with QLever, mine locally
- Grouped mining: Combine related sources (e.g., pubchem.ftp.*) in one QLever
- LSLOD Cloud: Combine ALL local sources for connectivity analysis
- Mapping generation: SSSOM, SeMRA, instance matching, class derivation
- Inference: Expand mappings using SeMRA
- Graph building: Create connectivity graphs
- Export: Generate paper statistics and figures

Usage:
    # Full pipeline (all sources)
    python scripts/pipeline.py

    # Remote mining only
    python scripts/pipeline.py --remote-only

    # Local mining only (individual sources)
    python scripts/pipeline.py --local-only

    # Grouped mining (related sources together)
    python scripts/pipeline.py --grouped-only

    # LSLOD Cloud (all local sources combined)
    python scripts/pipeline.py --lslod-cloud-only

    # Specific sources
    python scripts/pipeline.py --sources wikipathways aopwikirdf chebi

    # Skip stages
    python scripts/pipeline.py --skip-mining --skip-mappings

For SLURM submission, use the separate shell scripts:
    sbatch scripts/slurm_remote.sh        # Remote mining only
    sbatch scripts/slurm_local.sh         # Local mining only (individual)
    sbatch scripts/slurm_grouped.sh       # Grouped mining
    sbatch scripts/slurm_lslod_cloud.sh   # LSLOD Cloud mining
    sbatch scripts/slurm_full.sh          # Full pipeline
    sbatch scripts/slurm_mappings.sh      # Mappings + inference
    sbatch scripts/slurm_analysis.sh      # Analysis only
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from rdfsolve.qlever import QleverConfig, build_provider_qleverfile, build_qleverfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# Configuration


class SourceMode(Enum):
    """How to mine a source."""

    REMOTE = "remote"  # Query SPARQL endpoint directly
    LOCAL = "local"  # Download + QLever index + mine
    BOTH = "both"  # Has both endpoint and local_provider
    UNKNOWN = "unknown"


@dataclass
class Source:
    """A data source from sources.yaml."""

    name: str
    endpoint: str | None = None
    local_provider: str | None = None
    download_urls: list[str] = field(default_factory=list)
    download_fields: dict[str, Any] = field(default_factory=dict)
    local_tar_url: str | None = None
    graph_uris: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    bioregistry_prefix: str | None = None

    # Health tracking fields
    endpoint_status: str = "unknown"
    endpoint_down: bool = False
    last_checked: str = ""
    last_success: str = ""
    last_error: str = ""
    failure_count: int = 0
    avg_response_time: float | None = None
    delay: float | None = None
    timeout: float | None = None
    sparql_engine: str = ""
    sparql_strategy: str = ""

    @property
    def mode(self) -> SourceMode:
        if self.endpoint and (self.local_provider or self.download_urls or self.local_tar_url):
            return SourceMode.BOTH
        elif self.endpoint:
            return SourceMode.REMOTE
        elif self.local_provider or self.download_urls or self.local_tar_url:
            return SourceMode.LOCAL
        return SourceMode.UNKNOWN

    @classmethod
    def from_dict(cls, d: dict) -> Source:
        from rdfsolve.models.source_model import SourceModel

        settings = SourceModel.model_validate(d)
        download_fields = {k: v for k, v in d.items() if k.startswith("download_") and v}
        download_urls = []
        for urls in download_fields.values():
            if isinstance(urls, str):
                urls = [urls]
            download_urls.extend(urls)

        return cls(
            name=d.get("name", ""),
            endpoint=d.get("endpoint"),
            local_provider=d.get("local_provider"),
            download_urls=download_urls,
            download_fields=download_fields,
            local_tar_url=d.get("local_tar_url"),
            graph_uris=settings.graph_uris,
            keywords=d.get("keywords", []),
            bioregistry_prefix=d.get("bioregistry_prefix"),
            endpoint_status=d.get("endpoint_status", "unknown"),
            endpoint_down=d.get("endpoint_down", False),
            last_checked=d.get("last_checked", ""),
            last_success=d.get("last_success", ""),
            last_error=d.get("last_error", ""),
            failure_count=d.get("failure_count", 0),
            avg_response_time=d.get("avg_response_time"),
            delay=d.get("delay"),
            timeout=settings.timeout,
            sparql_engine=settings.sparql_engine,
            sparql_strategy=settings.sparql_strategy,
        )

    def qlever_entry(self) -> dict[str, Any]:
        """Keep download formats and tar scope at the builder boundary."""
        return {
            "name": self.name,
            **self.download_fields,
            "local_tar_url": self.local_tar_url,
            "graph_uris": self.graph_uris,
        }


@dataclass
class PipelineConfig:
    """Pipeline configuration."""

    # Directories
    base_dir: Path = field(default_factory=lambda: Path("/home/javier.millanacosta/rdfsolve"))
    repo_dir: Path | None = None
    data_dir: Path | None = None
    output_dir: Path | None = None
    results_dir: Path | None = None
    log_dir: Path | None = None

    # Sources
    sources_file: Path | None = None
    sssom_sources_file: Path | None = None
    sources: list[Source] = field(default_factory=list)

    # Mining settings
    timeout: float | None = None  # Override the source timeout only when set.
    delay: float = 1.0
    chunk_size: int = 50000
    class_batch_size: int = 50
    benchmark: bool = True
    enrich: bool = True
    examples_per_pattern: int = 2
    void_base_url: str = "https://rdfsolve.bigcat-bioinformatics.nl"

    # QLever settings (for local mining)
    qlever_image: str = "docker://docker.io/adfreiburg/qlever:latest"
    base_port: int = 7019
    qlever_startup_timeout: int = 600  # Seconds to load the index.
    no_download: bool = False
    no_index: bool = False

    # Stage control
    skip_remote: bool = False
    skip_local: bool = False
    skip_mining: bool = False
    skip_mappings: bool = False
    skip_seeding: bool = False
    skip_inference: bool = False
    skip_completed: bool = False  # Skip sources with existing output files

    # Ontology/metadata extraction
    extract_ontology: bool = False
    ontology_scope: str = "schema"
    ontology_as_data: bool = False
    extract_metadata: bool = False

    # Parallelism
    parallelism: int = 4

    # Output naming
    output_suffix: str = ""

    # Output formats
    output_formats: list[str] = field(default_factory=lambda: ["json-ld", "void"])

    # Health check files
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
        duplicates = sorted(name for name, count in Counter(s.name for s in sources).items() if count > 1)
        if duplicates:
            raise ValueError(f"Duplicate source names would overwrite outputs: {duplicates}. Select a registry with unique names.")

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
                name for name, data in status["downloads"].items()
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


# Pipeline Stages


class Stage:
    """Base class for pipeline stages."""

    name: str = "base"

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.start_time: float | None = None
        self.end_time: float | None = None
        self.results: dict[str, Any] = {}
        self._servers: dict[int, subprocess.Popen] = {}

    def run(self) -> dict[str, Any]:
        """Execute the stage."""
        log.info("=" * 70)
        log.info(f"STAGE: {self.name.upper()}")
        log.info("=" * 70)

        self.start_time = time.time()
        try:
            self.results = self._execute()
            failed = bool(self.results.get("failed"))
            produced = any(self.results.get(key) for key in
                           ("mined", "groups_mined", "indexed_individually"))
            skipped = bool(self.results.get("skipped"))
            state = ("partial" if produced else "failed") if failed else (
                "partial" if skipped and produced else "skipped" if skipped else "complete"
            )
            self.results["state"] = state
            self.results["success"] = state == "complete"
        except Exception as e:
            log.exception(f"Stage {self.name} failed: {e}")
            self.results = {"success": False, "state": "failed", "error": str(e)}
        finally:
            self.end_time = time.time()
            elapsed = self.end_time - self.start_time
            self.results["elapsed_seconds"] = elapsed
            log.info(f"Stage {self.name}: {self.results.get('state', 'interrupted')} in {elapsed:.1f}s")

        return self.results

    def _execute(self) -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    def _require_complete(miner: Any) -> None:
        report = miner.last_report
        if report is None or report.completion_state != "complete":
            reason = report.abort_reason if report is not None else "No mining report"
            raise RuntimeError(f"Mining incomplete: {reason or 'see the source report'}")

    def _save_schema_outputs(
        self,
        schema: Any,
        output_dir: Path,
        name: str,
        suffix: str,
    ) -> None:
        """Save schema in requested output formats.

        Args:
            schema: MinedSchema object to save
            output_dir: Directory to save outputs
            name: Source name
            suffix: Output file suffix
        """
        formats = self.config.output_formats

        # Keep a complete internal record, regardless of export formats.
        path = output_dir / f"{name}{suffix}_schema.json"
        path.write_text(json.dumps(schema.to_dict(), indent=2), encoding="utf-8")

        # JSON-LD format
        if "json-ld" in formats:
            path = output_dir / f"{name}{suffix}_schema.jsonld"
            path.write_text(json.dumps(schema.to_jsonld(), indent=2), encoding="utf-8")

        # VoID format
        if "void" in formats:
            path = output_dir / f"{name}{suffix}_void.ttl"
            try:
                void_graph = schema.to_void_graph(base_url=self.config.void_base_url)
                if void_graph:
                    void_ttl = void_graph.serialize(format="turtle")
                    path.write_text(void_ttl, encoding="utf-8")
            except Exception as e:
                raise RuntimeError(f"[{name}] Could not generate VoID: {e}") from e

        # SHACL format
        if "shacl" in formats:
            path = output_dir / f"{name}{suffix}_shacl.ttl"
            try:
                shacl_ttl = schema.to_shacl()
                path.write_text(shacl_ttl, encoding="utf-8")
            except Exception as e:
                raise RuntimeError(f"[{name}] Could not generate SHACL: {e}") from e

        # Pydantic format (Python code)
        if "pydantic" in formats:
            path = output_dir / f"{name}{suffix}_schema.py"
            try:
                pydantic_code = schema.to_pydantic(schema_name=name)
                path.write_text(pydantic_code, encoding="utf-8")
            except Exception as e:
                raise RuntimeError(f"[{name}] Could not generate Pydantic: {e}") from e


class RemoteMiningStage(Stage):
    """Mine schemas from remote SPARQL endpoints with health checking and rate limiting.

    Mines different hosts concurrently, but endpoints on the same host sequentially.
    """

    name = "remote_mining"

    def _execute(self) -> dict[str, Any]:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from urllib.parse import urlparse

        sources = self.config.get_remote_sources()
        log.info(f"Mining {len(sources)} remote endpoints")

        # Group sources by host for concurrent mining
        host_groups: dict[str, list[Source]] = {}
        for source in sources:
            if not source.endpoint:
                continue
            host = urlparse(source.endpoint).netloc
            if host not in host_groups:
                host_groups[host] = []
            host_groups[host].append(source)

        log.info(f"Grouped into {len(host_groups)} hosts for concurrent mining")

        # Thread-safe results collection
        from threading import Lock
        results = {"mined": [], "failed": [], "skipped": []}
        results_lock = Lock()

        def mine_host_sources(host: str, host_sources: list[Source]) -> None:
            """Mine all sources on a single host sequentially."""
            for source in host_sources:
                result = self._mine_single_source(source)
                with results_lock:
                    if result["status"] == "mined":
                        results["mined"].append(result["data"])
                    elif result["status"] == "failed":
                        results["failed"].append(result["data"])
                    else:
                        results["skipped"].append(result["data"])

        if not host_groups:
            return {"mined": [], "failed": [], "skipped": ["No remote sources selected"]}

        # Mine hosts concurrently
        max_workers = min(self.config.parallelism or 8, len(host_groups))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(mine_host_sources, host, host_sources): host
                for host, host_sources in host_groups.items()
            }
            for future in as_completed(futures):
                host = futures[future]
                try:
                    future.result()
                except Exception as e:
                    log.error(f"Host {host} mining failed: {e}")
                    results["failed"].append({"host": host, "error": str(e)})

        # Log summary
        log.info(
            f"Mining complete: {len(results['mined'])} succeeded, "
            f"{len(results['failed'])} failed, {len(results['skipped'])} skipped"
        )

        return results

    def _mine_single_source(self, source: Source) -> dict[str, Any]:
        """Mine a single source. Returns dict with status and data."""
        from rdfsolve import SchemaMiner
        from rdfsolve.endpoint_health import (
            check_endpoint_health,
            get_polite_delay,
            update_endpoint_status,
        )
        from datetime import datetime, timezone

        log.info(f"[{source.name}] Starting...")

        if not source.endpoint:
            return {"status": "skipped", "data": source.name}

        # Skip endpoints known to be down
        if source.endpoint_down and source.failure_count >= 3:
            log.warning(f"[{source.name}] Skipping: endpoint marked as down")
            return {"status": "skipped", "data": source.name}

        # Health check
        needs_health_check = True
        if source.last_checked:
            try:
                last_check = datetime.fromisoformat(source.last_checked)
                age = (datetime.now(timezone.utc) - last_check).total_seconds()
                if age < 3600:
                    needs_health_check = False
            except ValueError:
                pass

        if needs_health_check:
            health = check_endpoint_health(source.endpoint, timeout=30)
            update_endpoint_status(source, health)
            if health.status != "up":
                log.warning(f"[{source.name}] Skipping: endpoint is {health.status}")
                return {"status": "skipped", "data": source.name}

        # Check if output already exists
        output_dir = self.config.output_dir
        source_output_dir = output_dir / source.name
        source_output_dir.mkdir(parents=True, exist_ok=True)
        suffix = self.config.output_suffix
        schema_path = source_output_dir / f"{source.name}{suffix}_schema.jsonld"

        if self.config.skip_completed and schema_path.exists():
            log.info(f"[{source.name}] Skipping: output already exists")
            return {"status": "skipped", "data": source.name}

        # Mine
        polite_delay = get_polite_delay(source)
        try:
            report_path = source_output_dir / f"{source.name}{suffix}_report.json"
            miner = SchemaMiner(
                endpoint_url=source.endpoint,
                source_name=source.name,
                graph_uris=source.graph_uris or None,
                timeout=(self.config.timeout if self.config.timeout is not None
                         else source.timeout if source.timeout is not None else 300.0),
                delay=polite_delay,
                sparql_engine=source.sparql_engine,
                sparql_strategy=source.sparql_strategy,
                chunk_size=self.config.chunk_size,
                class_batch_size=self.config.class_batch_size,
            enrich=self.config.enrich,
            examples_per_pattern=self.config.examples_per_pattern,
                report_path=str(report_path),
            )

            # Use mine_with_ontology if ontology extraction enabled
            if self.config.extract_ontology or self.config.extract_metadata:
                from rdfsolve.mining import mine_with_ontology
                result = mine_with_ontology(
                    miner,
                    extract_ontology=self.config.extract_ontology,
                    ontology_scope=self.config.ontology_scope,
                    ontology_as_data=self.config.ontology_as_data,
                    extract_metadata=self.config.extract_metadata,
                    dataset_name=source.name,
                )
                schema = result.data_schema
                # Export ontology and metadata if extracted
                if result.ontology:
                    ontology_path = source_output_dir / f"{source.name}{suffix}_ontology.ttl"
                    try:
                        ontology_graph = result.ontology.to_rdf_graph()
                        if ontology_graph:
                            schema.annotate_rdf(ontology_graph, include_examples=False)
                            ont_ttl = ontology_graph.serialize(format="turtle")
                            ontology_path.write_text(ont_ttl, encoding="utf-8")
                    except Exception as e:
                        raise RuntimeError(f"[{source.name}] Could not generate ontology.ttl: {e}") from e
                if result.metadata:
                    metadata_path = source_output_dir / f"{source.name}{suffix}_metadata.ttl"
                    try:
                        metadata_graph = result.metadata.to_rdf_graph()
                        if metadata_graph:
                            meta_ttl = metadata_graph.serialize(format="turtle")
                            metadata_path.write_text(meta_ttl, encoding="utf-8")
                    except Exception as e:
                        raise RuntimeError(f"[{source.name}] Could not generate metadata.ttl: {e}") from e
            else:
                schema = miner.mine(dataset_name=source.name)

            source.endpoint_status = "up"
            source.last_success = datetime.now(timezone.utc).isoformat()
            source.failure_count = 0
            source.endpoint_down = False

            # Save schema in requested formats
            self._save_schema_outputs(schema, source_output_dir, source.name, suffix)
            self._require_complete(miner)

            if miner.last_report:
                report = {
                    "name": source.name,
                    "endpoint": source.endpoint,
                    "classes": miner.last_report.class_count,
                    "properties": miner.last_report.property_count,
                    "patterns": miner.last_report.pattern_count,
                    "queries_sent": miner.last_report.total_queries_sent,
                    "queries_failed": miner.last_report.total_queries_failed,
                }
                log.info(
                    f"[{source.name}] -> {report['classes']} classes, "
                    f"{report['properties']} props, {report['queries_sent']} queries"
                )
                return {"status": "mined", "data": report}
            else:
                return {"status": "mined", "data": {"name": source.name, "endpoint": source.endpoint}}

        except Exception as e:
            source.failure_count += 1
            source.last_error = str(e)[:500]
            if source.failure_count >= 3:
                source.endpoint_down = True

            log.error(f"[{source.name}] -> FAILED: {e}")
            return {"status": "failed", "data": {"name": source.name, "error": str(e)}}


class LocalMiningStage(Stage):
    """Mine schemas from local RDF dumps using QLever."""

    name = "local_mining"

    def _execute(self) -> dict[str, Any]:
        sources = self.config.get_local_sources()
        log.info(f"Processing {len(sources)} local sources")

        results = {"indexed": [], "mined": [], "failed": [], "skipped": []}

        # Ensure QLever image exists
        self._ensure_qlever_image()

        qlever_workdir = self.config.data_dir / "qlever_workdirs"
        qlever_workdir.mkdir(parents=True, exist_ok=True)

        port = self.config.base_port

        for i, source in enumerate(sources, 1):
            log.info(f"[{i}/{len(sources)}] {source.name}")

            workdir = qlever_workdir / source.name
            workdir.mkdir(parents=True, exist_ok=True)

            # Skip if output already exists
            suffix = self.config.output_suffix
            source_output_dir = self.config.output_dir / source.name
            schema_path = source_output_dir / f"{source.name}{suffix}_schema.jsonld"
            if self.config.skip_completed and schema_path.exists():
                log.info(f"  Skipping: output already exists")
                results["skipped"].append(source.name)
                continue

            try:
                qleverfile = workdir / "Qleverfile"
                has_index = self._has_qlever_index(workdir, source.name)
                if not qleverfile.exists() and not has_index:
                    self._prepare_qleverfile(workdir, source, port)

                index_done = workdir / ".index.done"
                if not has_index:
                    if self.config.no_index:
                        raise FileNotFoundError(f"No cached index in {workdir}; prepare it before mining")
                    log.info("  Executing Qleverfile (download + index)...")
                    try:
                        self._execute_qleverfile(workdir, source)
                        index_done.touch()
                        results["indexed"].append(source.name)
                    except Exception as idx_err:
                        log.error(f"  -> Failed: {idx_err}")
                        log.warning(f"  -> Skipping {source.name}")
                        results["failed"].append(
                            {"name": source.name, "error": f"Qleverfile execution failed: {idx_err}"}
                        )
                        continue

                # Start QLever server
                log.info(f"  Starting QLever on port {port}...")
                server_pid = self._qlever_start(workdir, source.name, port)

                if server_pid:
                    try:
                        # Mine schema
                        log.info("  Mining schema...")
                        self._mine_local(source, port)
                        results["mined"].append(source.name)
                    finally:
                        # Stop server
                        self._qlever_stop(server_pid)
                else:
                    log.warning(f"  -> Server failed to start, skipping {source.name}")
                    results["failed"].append(
                        {"name": source.name, "error": "Server failed to start"}
                    )

                port += 1

            except Exception as e:
                log.error(f"  -> FAILED: {e}")
                results["failed"].append({"name": source.name, "error": str(e)})

        return results

    def _check_data_exists(self, workdir: Path, source: Source) -> bool:
        """Check if data files already exist."""
        for ext in ["*.ttl", "*.nt", "*.nq", "*.ttl.gz", "*.nt.gz"]:
            if list(workdir.glob(ext)):
                return True
        return False

    def _has_qlever_index(self, workdir: Path, source_name: str) -> bool:
        """Reuse existing indices; do not overwrite partial index files."""
        from rdfsolve.qlever.lifecycle import index_name

        source_name = index_name(workdir, source_name)
        index_spo = workdir / f"{source_name}.index.spo"
        if index_spo.is_file():
            return True
        if any(workdir.glob(f"{source_name}.index.*")):
            raise ValueError(f"Incomplete index in {workdir}; inspect it before rebuilding")
        return False

    def _prepare_qleverfile(self, workdir: Path, source: Source, port: int):
        """Generate Qleverfile for source."""
        qleverfile_path = workdir / "Qleverfile"
        if qleverfile_path.exists():
            return
        entry = source.qlever_entry()

        cfg = QleverConfig(
            memory_for_queries="30G",
            timeout="600s",
            parser_buffer_size="2GB",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_content = build_qleverfile(
            entry, self.config.data_dir, port, runtime="singularity", cfg=cfg, workdir=workdir
        )

        qleverfile_path = workdir / "Qleverfile"
        with qleverfile_path.open("x", encoding="utf-8") as stream:
            stream.write(qleverfile_content)
        log.info(f"    Generated Qleverfile")

    def _execute_qleverfile(self, workdir: Path, source: Source, skip_download: bool = False):
        """Execute Qleverfile data download and indexing."""
        qleverfile_path = workdir / "Qleverfile"
        if not qleverfile_path.exists():
            raise ValueError(f"Qleverfile not found in {workdir}")

        import configparser
        config = configparser.ConfigParser(interpolation=None)
        config.read(qleverfile_path)

        rdf_dir = workdir / "rdf"
        rdf_dir.mkdir(exist_ok=True)

        input_files_pattern = config.get("index", "INPUT_FILES")
        rdf_format = config.get("data", "FORMAT")
        settings_json = config.get("index", "SETTINGS_JSON")

        input_files = self._qlever_input_files(workdir, input_files_pattern)

        if not input_files and not skip_download and not self.config.no_download:
            get_data_cmd = config.get("data", "GET_DATA_CMD")
            log.info(f"    Downloading data...")
            subprocess.run(["bash", "-c", get_data_cmd], check=True, cwd=workdir)

            input_files = self._qlever_input_files(workdir, input_files_pattern)

        if not input_files:
            raise ValueError(f"No files found matching {input_files_pattern}")

        settings_path = workdir / f"{source.name}.settings.json"
        settings_path.write_text(settings_json)

        file_flags = []
        for f in input_files:
            file_flags.extend(["-f", str(f)])

        image_path = self.config.data_dir / "qlever.sif"
        cmd = [
            "singularity",
            "exec",
            "--bind",
            f"{workdir}:{workdir}",
            str(image_path),
            "qlever-index",
            "-i",
            config.get("data", "NAME", fallback=source.name),
            "-s",
            str(settings_path),
            "-F",
            rdf_format,
            *file_flags,
            "-p",
            config.get("index", "PARALLEL_PARSING"),
        ]

        log.info(f"    Indexing {len(input_files)} files...")
        subprocess.run(cmd, cwd=workdir, check=True)

    @staticmethod
    def _qlever_input_files(workdir: Path, patterns: str) -> list[Path]:
        """Resolve each input glob; support legacy files outside rdf/."""
        import glob
        import shlex

        files: set[Path] = set()
        for pattern in shlex.split(patterns):
            matches = [Path(p) for p in glob.glob(str(workdir / pattern))]
            if not matches and pattern.startswith("rdf/"):
                matches = [Path(p) for p in glob.glob(str(workdir / pattern[4:]))]
            files.update(p.resolve() for p in matches if p.is_file())
        return sorted(files)


    def _mine_local(self, source: Source, port: int):
        """Mine schema from local QLever instance."""
        from rdfsolve import SchemaMiner

        endpoint = f"http://localhost:{port}"

        output_dir = self.config.output_dir / source.name
        output_dir.mkdir(parents=True, exist_ok=True)

        suffix = self.config.output_suffix
        report_path = output_dir / f"{source.name}{suffix}_report.json"

        # Local queries use QLever, not the remote endpoint's transport hints.
        miner = SchemaMiner(
            endpoint_url=endpoint,
            source_name=source.name,
            timeout=self.config.timeout if self.config.timeout is not None else 86400.0,
            delay=self.config.delay,
            sparql_engine="qlever",
            chunk_size=self.config.chunk_size,
            class_batch_size=self.config.class_batch_size,
            enrich=self.config.enrich,
            examples_per_pattern=self.config.examples_per_pattern,
            report_path=str(report_path),
        )

        # Use mine_with_ontology if ontology extraction enabled
        if self.config.extract_ontology or self.config.extract_metadata:
            from rdfsolve.mining import mine_with_ontology
            result = mine_with_ontology(
                miner,
                extract_ontology=self.config.extract_ontology,
                ontology_scope=self.config.ontology_scope,
                    ontology_as_data=self.config.ontology_as_data,
                extract_metadata=self.config.extract_metadata,
                dataset_name=source.name,
            )
            schema = result.data_schema
            # Export ontology and metadata if extracted
            if result.ontology:
                ontology_path = output_dir / f"{source.name}{suffix}_ontology.ttl"
                try:
                    ontology_graph = result.ontology.to_rdf_graph()
                    if ontology_graph:
                        schema.annotate_rdf(ontology_graph, include_examples=False)
                        ont_ttl = ontology_graph.serialize(format="turtle")
                        ontology_path.write_text(ont_ttl, encoding="utf-8")
                except Exception as e:
                    raise RuntimeError(f"  Could not generate ontology.ttl: {e}") from e
            if result.metadata:
                metadata_path = output_dir / f"{source.name}{suffix}_metadata.ttl"
                try:
                    metadata_graph = result.metadata.to_rdf_graph()
                    if metadata_graph:
                        meta_ttl = metadata_graph.serialize(format="turtle")
                        metadata_path.write_text(meta_ttl, encoding="utf-8")
                except Exception as e:
                    raise RuntimeError(f"  Could not generate metadata.ttl: {e}") from e
        else:
            schema = miner.mine(dataset_name=source.name)

        self._save_schema_outputs(schema, output_dir, source.name, suffix)
        self._require_complete(miner)


    def _ensure_qlever_image(self):
        """Require the prepared image; do not pull a new engine during mining."""
        image = self.config.data_dir / "qlever.sif"
        if not image.is_file():
            raise FileNotFoundError(f"Prepare the QLever image before mining: {image}")

    def _qlever_start(self, workdir: Path, name: str, port: int) -> int:
        from rdfsolve.qlever.lifecycle import start_server

        process = start_server(self.config.data_dir / "qlever.sif", workdir, name, port,
                               startup_timeout=self.config.qlever_startup_timeout)
        self._servers[process.pid] = process
        return process.pid

    def _qlever_stop(self, pid: int):
        from rdfsolve.qlever.lifecycle import stop_server

        process = self._servers.pop(pid, None)
        if process is not None:
            stop_server(process)


class GroupedMiningStage(LocalMiningStage):
    """Mine schemas from grouped local sources.

    Loads multiple related sources into ONE QLever instance with named graphs.
    Inherits from LocalMiningStage to reuse download/indexing methods.
    """

    name = "grouped_mining"

    def _execute(self) -> dict[str, Any]:
        sources = self.config.get_local_sources()
        groups = self._identify_groups(sources)
        log.info(f"Identified {len(groups)} source groups for combined mining")

        results = {"groups_mined": [], "failed": [], "skipped": [], "indexed_individually": []}
        grouped_names = {source.name for members in groups.values() for source in members}
        sources_with_existing_index: list[tuple[Source, Path]] = []
        for source in sources:
            if source.name not in grouped_names:
                workdir = self.config.data_dir / "qlever_workdirs" / source.name
                if self._has_qlever_index(workdir, source.name):
                    sources_with_existing_index.append((source, workdir))
                else:
                    results["failed"].append({"name": source.name,
                        "error": f"No cached individual index in {workdir}; use --local-only to prepare it"})
        if not sources:
            results["skipped"].append("No local sources selected")
        self._ensure_qlever_image()

        qlever_workdir = self.config.data_dir / "qlever_groups"
        qlever_workdir.mkdir(parents=True, exist_ok=True)
        port = self.config.base_port + 1000

        for group_name, group_sources in groups.items():
            log.info(f"[Group: {group_name}] {len(group_sources)} sources")

            workdir = qlever_workdir / group_name
            workdir.mkdir(parents=True, exist_ok=True)

            # Skip if output already exists
            output_dir = self.config.output_dir / f"grouped_{group_name}"
            schema_path = output_dir / f"{group_name}_schema.jsonld"
            if self.config.skip_completed and schema_path.exists():
                log.info(f"  Skipping: output already exists")
                results["skipped"].append(group_name)
                continue

            try:
                # Existing grouped indices do not need the original downloads.
                if self._has_qlever_index(workdir, group_name):
                    server_pid = self._qlever_start(workdir, group_name, port)
                    if not server_pid:
                        raise RuntimeError(f"Server failed to start for {group_name}")
                    try:
                        self._mine_grouped(group_name, group_sources, port)
                        results["groups_mined"].append(group_name)
                    finally:
                        self._qlever_stop(server_pid)
                    port += 1
                    continue
                if self.config.no_index:
                    for source in group_sources:
                        source_workdir = self.config.data_dir / "qlever_workdirs" / source.name
                        if not self._has_qlever_index(source_workdir, source.name):
                            results["failed"].append({"name": source.name,
                                "error": f"No grouped or individual index for {source.name}"})
                        else:
                            sources_with_existing_index.append((source, source_workdir))
                    continue
                source_data = []
                for source in group_sources:
                    source_workdir = self.config.data_dir / "qlever_workdirs" / source.name
                    source_workdir.mkdir(parents=True, exist_ok=True)

                    # Check if workdir has RDF files
                    has_files = any(
                        list(source_workdir.glob(ext))
                        for ext in ["*.ttl", "*.nt", "*.nq", "rdf/*.ttl", "rdf/*.nt", "rdf/*.nq"]
                    )

                    # Download if no files exist and source has download URLs
                    if not has_files and self._has_qlever_index(source_workdir, source.name):
                        sources_with_existing_index.append((source, source_workdir))
                        continue
                    if not has_files and not self.config.no_download and (
                        source.download_urls or source.local_tar_url
                        or (source_workdir / "Qleverfile").exists()
                    ):
                        log.info(f"  -> Downloading data for {source.name}...")
                        try:
                            qleverfile = source_workdir / "Qleverfile"
                            if not qleverfile.exists():
                                self._prepare_qleverfile(source_workdir, source, self.config.base_port)
                            self._execute_qleverfile(source_workdir, source)
                            # Re-check for files
                            has_files = any(
                                list(source_workdir.glob(ext))
                                for ext in ["*.ttl", "*.nt", "*.nq", "rdf/*.ttl", "rdf/*.nt", "rdf/*.nq"]
                            )
                        except Exception as dl_err:
                            log.warning(f"  -> Download failed for {source.name}: {dl_err}")

                    if not has_files:
                        # Check if there's a pre-existing QLever index we can use
                        if self._has_qlever_index(source_workdir, source.name):
                            log.info(f"  -> No RDF files but found existing QLever index for {source.name}")
                            sources_with_existing_index.append((source, source_workdir))
                        else:
                            log.warning(f"  -> No RDF files in {source.name} workdir, skipping")
                        continue
                    source_data.append((source, source_workdir))

                if not source_data:
                    log.warning(f"  -> No data found for group {group_name}, skipping")
                    results["skipped"].append(group_name)
                    continue

                qleverfile = workdir / "Qleverfile"
                if not qleverfile.exists():
                    self._prepare_group_qleverfile(workdir, group_name, group_sources, port)

                index_done = workdir / ".index.done"
                if not index_done.exists():
                    log.info(f"  Indexing {len(source_data)} sources...")
                    try:
                        self._execute_group_qleverfile(workdir, group_name, source_data)
                        index_done.touch()
                    except Exception as idx_err:
                        log.error(f"  -> Indexing failed: {idx_err}")
                        results["failed"].append(
                            {"group": group_name, "error": f"Index failed: {idx_err}"}
                        )
                        continue

                log.info(f"  Starting QLever on port {port}...")
                server_pid = self._qlever_start(workdir, group_name, port)

                if server_pid:
                    try:
                        log.info("  Mining combined schema...")
                        self._mine_grouped(group_name, group_sources, port)
                        results["groups_mined"].append(group_name)
                    finally:
                        self._qlever_stop(server_pid)
                else:
                    log.warning(f"  -> Server failed to start for {group_name}")
                    results["failed"].append(
                        {"group": group_name, "error": "Server failed to start"}
                    )

                port += 1

            except Exception as e:
                log.error(f"  -> FAILED: {e}")
                results["failed"].append({"group": group_name, "error": str(e)})

        # Mine sources with existing indices individually (fallback for failed downloads)
        if sources_with_existing_index:
            log.info(f"\n=== Mining {len(sources_with_existing_index)} sources with existing indices ===")
            individual_port = port + 100  # Use different port range

            for source, workdir in sources_with_existing_index:
                # Skip if output already exists
                suffix = self.config.output_suffix
                source_output_dir = self.config.output_dir / source.name
                schema_path = source_output_dir / f"{source.name}{suffix}_schema.jsonld"
                if self.config.skip_completed and schema_path.exists():
                    log.info(f"[{source.name}] Skipping: output already exists")
                    results["skipped"].append(source.name)
                    continue

                log.info(f"[{source.name}] Mining from existing index...")
                try:
                    server_pid = self._qlever_start(workdir, source.name, individual_port)
                    if server_pid:
                        try:
                            self._mine_local(source, individual_port)
                            results["indexed_individually"].append(source.name)
                            log.info(f"[{source.name}] -> Mined successfully")
                        finally:
                            self._qlever_stop(server_pid)
                    else:
                        log.warning(f"[{source.name}] -> Server failed to start")
                        results["failed"].append(
                            {"name": source.name, "error": "Server failed to start for existing index"}
                        )
                    individual_port += 1
                except Exception as e:
                    log.error(f"[{source.name}] -> FAILED: {e}")
                    results["failed"].append({"name": source.name, "error": str(e)})

        return results

    def _identify_groups(self, sources: list[Source]) -> dict[str, list[Source]]:
        """Identify groups of sources that can be mined together locally.

        Only sources with download_urls or local_provider are included -
        endpoint-only sources cannot be grouped for local mining.
        """
        from urllib.parse import urlparse

        groups: dict[str, list[Source]] = {}

        for source in sources:
            # Skip endpoint-only sources - they can't be locally indexed
            if not source.download_urls and not source.local_provider:
                continue

            group_name = None

            if source.download_urls:
                first_url = source.download_urls[0] if isinstance(source.download_urls, list) else source.download_urls
                hostname = urlparse(first_url).hostname
                if hostname:
                    # Only group known multi-file providers
                    if "pubchem" in hostname or "pubchem" in source.name:
                        group_name = "pubchem.ftp"
                    elif "bio2rdf" in hostname or "bio2rdf" in source.name:
                        group_name = "bio2rdf"
                    elif "rdfportal" in hostname or "rdfportal" in source.name:
                        group_name = "rdfportal"
                    elif "dbcls" in hostname or "dbcls" in source.name:
                        group_name = "dbcls"
                    # Don't group other hosts (e.g., raw.githubusercontent.com)
                    # They should be mined individually via LocalMiningStage

            if not group_name and source.local_provider:
                group_name = source.local_provider

            # Skip sources that don't belong to a known group
            if not group_name:
                continue

            if group_name not in groups:
                groups[group_name] = []
            groups[group_name].append(source)

        return groups

    def _prepare_group_qleverfile(
        self, workdir: Path, group_name: str, group_sources: list[Source], port: int
    ):
        """Generate provider Qleverfile for grouped sources."""
        qleverfile_path = workdir / "Qleverfile"
        if qleverfile_path.exists():
            return
        members = [source.qlever_entry() for source in group_sources]

        cfg = QleverConfig(
            memory_for_queries="80G",
            timeout="1200s",
            parser_buffer_size="4GB",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_content = build_provider_qleverfile(
            group_name, members, self.config.data_dir, port, runtime="singularity", cfg=cfg, workdir=workdir
        )

        qleverfile_path = workdir / "Qleverfile"
        with qleverfile_path.open("x", encoding="utf-8") as stream:
            stream.write(qleverfile_content)
        log.info(f"  Generated provider Qleverfile")

    def _execute_group_qleverfile(
        self, workdir: Path, group_name: str, source_data: list[tuple[Source, Path]]
    ):
        """Execute group Qleverfile indexing using existing data."""
        import configparser
        qleverfile_path = workdir / "Qleverfile"
        config = configparser.ConfigParser()
        config.read(qleverfile_path)

        rdf_format = config.get("data", "FORMAT")
        settings_json = config.get("index", "SETTINGS_JSON")

        settings_path = workdir / f"{group_name}.settings.json"
        settings_path.write_text(settings_json)

        input_files = []
        for source, source_workdir in source_data:
            for ext in ["*.ttl", "*.nt", "*.nq"]:
                for f in source_workdir.glob(ext):
                    graph_uri = f"http://rdfsolve.org/graph/{source.name}"
                    input_files.append((f, graph_uri))

        if not input_files:
            raise ValueError(f"No input files found for group {group_name}")

        file_flags = []
        for file_path, graph_uri in input_files:
            file_flags.extend(["-f", str(file_path), "-g", graph_uri])

        image_path = self.config.data_dir / "qlever.sif"
        cmd = [
            "singularity",
            "exec",
            "--bind",
            f"{self.config.data_dir}:{self.config.data_dir}",
            str(image_path),
            "qlever-index",
            "-i",
            group_name,
            "-s",
            str(settings_path),
            "-F",
            rdf_format,
            *file_flags,
            "-p",
            config.get("index", "PARALLEL_PARSING"),
        ]

        log.info(f"  Indexing {len(input_files)} files from {len(source_data)} sources...")
        subprocess.run(cmd, cwd=workdir, check=True)


    def _mine_grouped(self, group_name: str, sources: list[Source], port: int):
        from rdfsolve import SchemaMiner

        endpoint = f"http://localhost:{port}"
        graph_uris = [f"http://rdfsolve.org/graph/{s.name}" for s in sources]

        # Create output directory for this group
        output_dir = self.config.output_dir / f"grouped_{group_name}"
        output_dir.mkdir(parents=True, exist_ok=True)

        report_path = output_dir / f"{group_name}_report.json"

        miner = SchemaMiner(
            endpoint_url=endpoint,
            source_name=group_name,
            graph_uris=graph_uris,
            timeout=self.config.timeout if self.config.timeout is not None else 86400.0,
            delay=self.config.delay,
            sparql_engine="qlever",
            chunk_size=self.config.chunk_size,
            class_batch_size=self.config.class_batch_size,
            enrich=self.config.enrich,
            examples_per_pattern=self.config.examples_per_pattern,
            report_path=str(report_path),
        )

        # Use mine_with_ontology if ontology extraction enabled
        if self.config.extract_ontology or self.config.extract_metadata:
            from rdfsolve.mining import mine_with_ontology
            result = mine_with_ontology(
                miner,
                extract_ontology=self.config.extract_ontology,
                ontology_scope=self.config.ontology_scope,
                    ontology_as_data=self.config.ontology_as_data,
                extract_metadata=self.config.extract_metadata,
                dataset_name=group_name,
            )
            schema = result.data_schema
            # Export ontology and metadata if extracted
            if result.ontology:
                ontology_path = output_dir / f"{group_name}_ontology.ttl"
                try:
                    ontology_graph = result.ontology.to_rdf_graph()
                    if ontology_graph:
                        schema.annotate_rdf(ontology_graph, include_examples=False)
                        ont_ttl = ontology_graph.serialize(format="turtle")
                        ontology_path.write_text(ont_ttl, encoding="utf-8")
                except Exception as e:
                    raise RuntimeError(f"  Could not generate ontology.ttl: {e}") from e
            if result.metadata:
                metadata_path = output_dir / f"{group_name}_metadata.ttl"
                try:
                    metadata_graph = result.metadata.to_rdf_graph()
                    if metadata_graph:
                        meta_ttl = metadata_graph.serialize(format="turtle")
                        metadata_path.write_text(meta_ttl, encoding="utf-8")
                except Exception as e:
                    raise RuntimeError(f"  Could not generate metadata.ttl: {e}") from e
        else:
            schema = miner.mine(dataset_name=group_name)

        self._save_schema_outputs(schema, output_dir, group_name, self.config.output_suffix)
        self._require_complete(miner)
        schema_path = output_dir / f"{group_name}_schema.json"
        log.info(f"  -> Saved grouped schema to {schema_path}")

class LsLodCloudStage(LocalMiningStage):
    """Mine the complete Local Semantic LOD Cloud.

    Combines ALL local sources into one mega-QLever instance.
    """

    name = "lslod_cloud"

    def _execute(self) -> dict[str, Any]:
        sources = self.config.get_local_sources()
        log.info(f"Building LSLOD Cloud from {len(sources)} local sources")

        results = {"sources_included": 0, "sources_skipped": 0, "mining_success": False}
        self._ensure_qlever_image()

        workdir = self.config.data_dir / "lslod_cloud"
        workdir.mkdir(parents=True, exist_ok=True)

        try:
            source_data = []
            for source in sources:
                source_workdir = self.config.data_dir / "qlever_workdirs" / source.name
                if not source_workdir.exists():
                    log.warning(f"  -> Workdir for {source.name} not found, skipping")
                    results["sources_skipped"] += 1
                    continue
                # Check if workdir actually has RDF files
                has_files = any(
                    list(source_workdir.glob(ext))
                    for ext in ["*.ttl", "*.nt", "*.nq"]
                )
                if not has_files:
                    log.warning(f"  -> No RDF files in {source.name} workdir, skipping")
                    results["sources_skipped"] += 1
                    continue
                source_data.append((source, source_workdir))

            if not source_data:
                log.error("  -> No source data found, cannot build LSLOD Cloud")
                return results

            results["sources_included"] = len(source_data)
            log.info(f"  Including {len(source_data)} sources in LSLOD Cloud")

            port = self.config.base_port + 2000
            qleverfile = workdir / "Qleverfile"
            if not qleverfile.exists():
                self._prepare_cloud_qleverfile(workdir, sources, port)

            index_done = workdir / ".index.done"
            if not index_done.exists():
                log.info(f"  Indexing all {len(source_data)} sources...")
                try:
                    self._execute_cloud_qleverfile(workdir, source_data)
                    index_done.touch()
                except Exception as idx_err:
                    log.error(f"  -> Indexing failed: {idx_err}")
                    return results

            log.info(f"  Starting LSLOD Cloud QLever on port {port}...")
            server_pid = self._qlever_start(workdir, "lslod_cloud", port)

            if server_pid:
                try:
                    log.info("  Mining LSLOD Cloud schema...")
                    self._mine_cloud(source_data, port)
                    results["mining_success"] = True
                finally:
                    self._qlever_stop(server_pid)
            else:
                log.error("  -> Server failed to start for LSLOD Cloud")

        except Exception as e:
            log.error(f"  -> FAILED: {e}")
            import traceback

            log.error(traceback.format_exc())

        return results

    def _prepare_cloud_qleverfile(
        self, workdir: Path, all_sources: list[Source], port: int
    ):
        """Generate Qleverfile for LSLOD Cloud."""
        qleverfile_path = workdir / "Qleverfile"
        if qleverfile_path.exists():
            return
        members = [source.qlever_entry() for source in all_sources]

        cfg = QleverConfig(
            memory_for_queries="250G",
            timeout="3600s",
            parser_buffer_size="8GB",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_content = build_provider_qleverfile(
            "lslod_cloud", members, self.config.data_dir, port, runtime="singularity", cfg=cfg, workdir=workdir
        )

        qleverfile_path = workdir / "Qleverfile"
        with qleverfile_path.open("x", encoding="utf-8") as stream:
            stream.write(qleverfile_content)
        log.info("  Generated LSLOD Cloud Qleverfile")

    def _execute_cloud_qleverfile(self, workdir: Path, source_data: list[tuple[Source, Path]]):
        """Execute LSLOD Cloud indexing using existing data."""
        import configparser
        qleverfile_path = workdir / "Qleverfile"
        config = configparser.ConfigParser()
        config.read(qleverfile_path)

        rdf_format = config.get("data", "FORMAT")
        settings_json = config.get("index", "SETTINGS_JSON")

        settings_path = workdir / "lslod_cloud.settings.json"
        settings_path.write_text(settings_json)

        input_files = []
        for source, source_workdir in source_data:
            for ext in ["*.ttl", "*.nt", "*.nq"]:
                for f in source_workdir.glob(ext):
                    graph_uri = f"http://rdfsolve.org/graph/{source.name}"
                    input_files.append((f, graph_uri))

        if not input_files:
            raise ValueError("No input files found for LSLOD Cloud")

        log.info(f"  Found {len(input_files)} RDF files across {len(source_data)} sources")

        file_flags = []
        for file_path, graph_uri in input_files:
            file_flags.extend(["-f", str(file_path), "-g", graph_uri])

        image_path = self.config.data_dir / "qlever.sif"
        cmd = [
            "singularity",
            "exec",
            "--bind",
            f"{self.config.data_dir}:{self.config.data_dir}",
            str(image_path),
            "qlever-index",
            "-i",
            "lslod_cloud",
            "-s",
            str(settings_path),
            "-F",
            rdf_format,
            *file_flags,
            "-p",
            config.get("index", "PARALLEL_PARSING"),
        ]

        subprocess.run(cmd, cwd=workdir, check=True)


    def _mine_cloud(self, source_data: list[tuple[Source, Path]], port: int):
        """Mine schema from the complete LSLOD Cloud."""
        from rdfsolve import SchemaMiner

        endpoint = f"http://localhost:{port}"

        graph_uris = [f"http://rdfsolve.org/graph/{s.name}" for s, _ in source_data]
        sources_map = {f"http://rdfsolve.org/graph/{s.name}": s for s, _ in source_data}

        log.info(f"  Mining with {len(graph_uris)} named graphs")

        output_dir = self.config.output_dir / "lslod_cloud"
        output_dir.mkdir(parents=True, exist_ok=True)

        report_path = output_dir / "lslod_cloud_report.json"

        miner = SchemaMiner(
            endpoint_url=endpoint,
            source_name="lslod_cloud",
            graph_uris=graph_uris,
            timeout=self.config.timeout if self.config.timeout is not None else 86400.0,
            delay=self.config.delay,
            sparql_engine="qlever",
            chunk_size=self.config.chunk_size,
            class_batch_size=self.config.class_batch_size,
            enrich=self.config.enrich,
            examples_per_pattern=self.config.examples_per_pattern,
            report_path=str(report_path),
        )

        schema = miner.mine(dataset_name="lslod_cloud")

        self._save_schema_outputs(schema, output_dir, "lslod_cloud", self.config.output_suffix)
        self._require_complete(miner)
        schema_path = output_dir / "lslod_cloud_schema.json"
        log.info(f"  -> Saved LSLOD Cloud schema to {schema_path}")

        log.info("  Generating SSSOM class mappings...")
        self._generate_sssom_mappings(source_data, output_dir)

    def _generate_sssom_mappings(self, source_data: list[tuple[Source, Path]], output_dir: Path) -> None:
        """Generate SSSOM class mapping files for cross-dataset interoperability."""
        from rdfsolve.mapping_discovery import discover_schema_pattern_mappings
        from rdfsolve.schema_models.core import MinedSchema
        from rdfsolve.sssom_generator import write_sssom_rdf, write_sssom_tsv

        # Load schemas and build VoID URI map
        schemas = []
        dataset_void_uris = {}
        for source, workdir in source_data:
            schema_path = self.config.output_dir / source.name / f"{source.name}_schema.json"
            if schema_path.exists():
                schema = MinedSchema.from_json(schema_path)
                schemas.append((source.name, schema))
                dataset_void_uris[source.name] = f"https://rdfsolve.bigcat-bioinformatics.nl/dataset/{source.name}"

        # Discover schema pattern mappings
        sssom_sets = discover_schema_pattern_mappings(
            schemas,
            dataset_void_uris,
            creator_id="https://orcid.org/0000-0001-5608-781X",
            creator_label="Javier Millan Acosta",
        )

        if not sssom_sets:
            log.info("  -> No cross-dataset mappings found")
            return

        # Export SSSOM files
        mappings_dir = output_dir / "mappings"
        mappings_dir.mkdir(exist_ok=True)

        total_mappings = 0
        for (ds1, ds2), msdf in sssom_sets.items():
            # TSV export (primary SSSOM format)
            tsv_path = mappings_dir / f"{ds1}-{ds2}-schema-patterns.sssom.tsv"
            write_sssom_tsv(msdf, tsv_path)

            # RDF export (Turtle format) - best effort, requires strict CURIE compliance
            rdf_path = mappings_dir / f"{ds1}-{ds2}-schema-patterns.sssom.ttl"
            try:
                write_sssom_rdf(msdf, rdf_path, format="turtle")
            except Exception as e:
                log.warning(f"  -> Skipping RDF export for {ds1}-{ds2}: {e}")

            num_mappings = len(msdf.df)
            total_mappings += num_mappings
            log.info(f"  -> Saved {num_mappings} mappings: {tsv_path.name}")

        log.info(f"  -> Total: {total_mappings} mappings across {len(sssom_sets)} dataset pairs")

    def _query_class_graphs(self, endpoint: str, graph_uris: list[str], access_token: str = "lslod_cloud") -> dict[str, list[str]]:
        """Query which graphs each class appears in."""
        from SPARQLWrapper import SPARQLWrapper, JSON

        sparql = SPARQLWrapper(endpoint)
        sparql.setReturnFormat(JSON)
        sparql.addCustomHttpHeader("Authorization", f"Bearer {access_token}")

        query = """
        SELECT DISTINCT ?class ?graph WHERE {
            GRAPH ?graph {
                ?s a ?class .
            }
        }
        """
        sparql.setQuery(query)

        try:
            results = sparql.query().convert()
            class_graphs = {}

            for result in results["results"]["bindings"]:
                cls = result["class"]["value"]
                graph = result["graph"]["value"]

                if graph in graph_uris:
                    graph_name = graph.split("/")[-1]
                    if cls not in class_graphs:
                        class_graphs[cls] = []
                    if graph_name not in class_graphs[cls]:
                        class_graphs[cls].append(graph_name)

            log.info(f"  Found {len(class_graphs)} classes across graphs")
            return class_graphs

        except Exception as e:
            log.warning(f"  Could not query class-graph mappings: {e}")
            return {}

    def _analyze_connectivity(
        self,
        schema,
        output_dir: Path,
        class_graphs: dict[str, list[str]],
        sources_map: dict[str, Source],
    ):
        """Analyze cross-dataset connectivity in the cloud."""
        log.info("  Analyzing cross-dataset connectivity...")

        stats = {
            "total_patterns": len(schema.patterns),
            "total_classes": schema.about.class_count,
            "total_properties": schema.about.property_count,
            "sources": len(sources_map),
            "classes_per_source": {},
            "timestamp": datetime.now().isoformat(),
        }

        for cls, graphs in class_graphs.items():
            for graph_name in graphs:
                if graph_name not in stats["classes_per_source"]:
                    stats["classes_per_source"][graph_name] = 0
                stats["classes_per_source"][graph_name] += 1

        stats_path = output_dir / "connectivity_stats.json"
        stats_path.write_text(json.dumps(stats, indent=2))

        log.info(f"  -> Saved connectivity stats to {stats_path}")

class SSSOMSeedingStage(Stage):
    """Seed and enrich SSSOM mappings from external sources.

    Downloads external SSSOM files (e.g., OLS mappings) and enriches them
    by adding subject_source/object_source pointing to our VoID dataset URIs.
    """

    name = "sssom_seeding"

    def _execute(self) -> dict[str, Any]:
        from rdfsolve.schema_models.core import MinedSchema
        from rdfsolve.sssom_enrichment import enrich_external_sssom_sources

        # Check if SSSOM sources config exists
        if not self.config.sssom_sources_file.exists():
            log.warning(f"SSSOM sources file not found: {self.config.sssom_sources_file}")
            return {"sources": 0, "enriched_mappings": 0}

        log.info(f"Loading SSSOM sources from {self.config.sssom_sources_file}")

        # Load all mined schemas to build class index
        schemas: list[tuple[str, MinedSchema]] = []
        dataset_void_uris: dict[str, str] = {}

        for source in self.config.sources:
            schema_path = self.config.output_dir / source.name / f"{source.name}_schema.jsonld"
            if schema_path.exists():
                schema = MinedSchema.from_jsonld(schema_path)
                schemas.append((source.name, schema))
                dataset_void_uris[source.name] = (
                    f"https://rdfsolve.bigcat-bioinformatics.nl/dataset/{source.name}"
                )

        if not schemas:
            log.warning("No schemas found - run mining stages first")
            return {"sources": 0, "enriched_mappings": 0, "error": "no_schemas"}

        log.info(f"Loaded {len(schemas)} schemas for class indexing")

        # Download and enrich external SSSOM files
        results = enrich_external_sssom_sources(
            sssom_sources_file=self.config.sssom_sources_file,
            schemas=schemas,
            dataset_void_uris=dataset_void_uris,
            output_dir=self.config.output_dir,
            creator_id="https://orcid.org/0000-0001-5608-781X",
            creator_label="Javier Millan Acosta",
        )

        total_enriched = sum(v for v in results.values() if v > 0)
        errors = sum(1 for v in results.values() if v < 0)

        log.info(f"Enriched {total_enriched} mappings from {len(results)} sources ({errors} errors)")

        return {
            "sources": len(results),
            "enriched_mappings": total_enriched,
            "errors": errors,
            "details": results,
        }


class AnalysisStage(Stage):
    """Analyze schemas and generate statistics."""

    name = "analysis"

    def _execute(self) -> dict[str, Any]:
        from rdfsolve.overlap import jaccard_similarity

        output_dir = self.config.output_dir

        # Find all schema files (in subdirectories per source)
        schema_files = list(output_dir.glob("**/*_schema.jsonld"))
        log.info(f"Analyzing {len(schema_files)} schema files")

        schemas = {}
        all_classes = set()
        all_properties = set()
        all_namespaces = set()

        for sf in schema_files:
            name = sf.stem.replace("_mined_remote_schema", "").replace(
                "_discovered_remote_schema", ""
            )
            try:
                data = json.loads(sf.read_text())
                if not data.get("@graph"):
                    continue

                classes = set()
                properties = set()

                for item in data.get("@graph", []):
                    class_uri = item.get("@id", "")
                    if class_uri:
                        classes.add(class_uri)
                        if "#" in class_uri:
                            all_namespaces.add(class_uri.rsplit("#", 1)[0] + "#")
                        elif "/" in class_uri:
                            all_namespaces.add(class_uri.rsplit("/", 1)[0] + "/")

                    for p in item.get("patterns", []):
                        prop = p.get("property", "")
                        if prop:
                            properties.add(prop)

                schemas[name] = {"classes": classes, "properties": properties}
                all_classes.update(classes)
                all_properties.update(properties)

            except Exception as e:
                log.warning(f"Failed to load {sf}: {e}")

        # Compute overlaps
        names = sorted(schemas.keys())
        overlaps = []

        for i, n1 in enumerate(names):
            for n2 in names[i + 1 :]:
                class_sim = jaccard_similarity(schemas[n1]["classes"], schemas[n2]["classes"])
                prop_sim = jaccard_similarity(schemas[n1]["properties"], schemas[n2]["properties"])

                if class_sim > 0 or prop_sim > 0:
                    overlaps.append(
                        {
                            "source": n1,
                            "target": n2,
                            "class_jaccard": class_sim,
                            "property_jaccard": prop_sim,
                        }
                    )

        stats = {
            "total_schemas": len(schemas),
            "total_classes": len(all_classes),
            "total_properties": len(all_properties),
            "total_namespaces": len(all_namespaces),
            "overlapping_pairs": len(overlaps),
        }

        # Save
        stats_path = output_dir / "paper_statistics.json"
        stats_path.write_text(json.dumps(stats, indent=2, default=list))

        overlaps_path = output_dir / "schema_overlaps.json"
        overlaps_path.write_text(json.dumps(overlaps, indent=2))

        log.info(f"Statistics saved to {stats_path}")

        return stats


# Pipeline Runner


class Pipeline:
    """Main pipeline orchestrator."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.stages: list[Stage] = []
        self.results: dict[str, Any] = {}

    def add_stage(self, stage_cls: type[Stage]) -> Pipeline:
        self.stages.append(stage_cls(self.config))
        return self

    def run(self) -> dict[str, Any]:
        log.info("=" * 70)
        log.info("RDFSOLVE LOD CLOUD ANALYSIS PIPELINE")
        log.info("=" * 70)
        log.info(f"Sources: {len(self.config.sources)}")
        log.info(f"  Remote: {len(self.config.get_remote_sources())}")
        log.info(f"  Local: {len(self.config.get_local_sources())}")
        log.info(f"Output: {self.config.output_dir}")
        log.info("")

        start = time.time()

        for stage in self.stages:
            self.results[stage.name] = stage.run()

            if not self.results[stage.name].get("success"):
                log.error(f"Pipeline aborted at stage: {stage.name}")
                break

        elapsed = time.time() - start
        self.results["total_elapsed_seconds"] = elapsed

        log.info("")
        log.info("=" * 70)
        log.info(f"PIPELINE FINISHED in {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log.info("=" * 70)

        # Save results (with suffix to avoid overwriting between jobs)
        suffix = self.config.output_suffix or ""
        results_filename = f"pipeline_results{suffix}.json"
        results_path = self.config.output_dir / results_filename
        results_path.write_text(json.dumps(self.results, indent=2, default=str))
        log.info(f"Results saved to: {results_path}")

        return self.results


# CLI


def preflight(config: PipelineConfig, *, grouped: bool, remote: bool) -> None:
    """Check configuration and cached inputs. Do not query or start servers."""
    if remote:
        selected = config.get_remote_sources()
        if not selected:
            raise ValueError("No remote sources selected")
        log.info("Preflight: %d remote sources; endpoint availability is checked during mining", len(selected))
        return
    stage = GroupedMiningStage(config) if grouped else LocalMiningStage(config)
    stage._ensure_qlever_image()
    import shutil
    if shutil.which("singularity") is None:
        raise FileNotFoundError("singularity is not on PATH")
    sources = config.get_local_sources()
    if not sources:
        raise ValueError("No local sources selected")
    covered = set()
    if grouped:
        for name, members in stage._identify_groups(sources).items():
            workdir = config.data_dir / "qlever_groups" / name
            if stage._has_qlever_index(workdir, name):
                covered.update(source.name for source in members)
                log.info("Cached group %s: %s", name, workdir)
    missing = []
    for source in sources:
        if source.name in covered:
            continue
        workdir = config.data_dir / "qlever_workdirs" / source.name
        if stage._has_qlever_index(workdir, source.name):
            log.info("Cached source %s: %s", source.name, workdir)
        else:
            missing.append(source.name)
    if missing:
        raise FileNotFoundError(f"Prepare indices or narrow --sources. Missing indices: {missing}")
    log.info("Preflight: %d local sources have cached indices; loadability is checked at startup", len(sources))


def main():
    parser = argparse.ArgumentParser(
        description="RDFSolve LOD Cloud Analysis Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/pipeline.py --sources wikipathways aopwikirdf
    python scripts/pipeline.py --remote-only
    python scripts/pipeline.py --local-only
    python scripts/pipeline.py --skip-mining --skip-mappings
        """,
    )

    parser.add_argument("--remote-only", action="store_true", help="Remote mining only")
    parser.add_argument("--local-only", action="store_true", help="Local mining only")
    parser.add_argument(
        "--grouped-only", action="store_true", help="Grouped mining only (related sources together)"
    )
    parser.add_argument(
        "--lslod-cloud-only",
        action="store_true",
        help="LSLOD Cloud mining only (all local sources)",
    )
    parser.add_argument("--sources", nargs="+", help="Specific source names")
    parser.add_argument("--sources-file", type=Path, help="Source registry YAML")
    parser.add_argument("--preflight", action="store_true", help="Check selected inputs without mining")
    parser.add_argument("--no-index", action="store_true", help="Use prepared indices only")
    parser.add_argument(
        "--skip-providers", nargs="+", help="Skip sources from these providers (e.g., idsm)"
    )
    parser.add_argument("--skip-mining", action="store_true", help="Skip mining stages")
    parser.add_argument("--skip-mappings", action="store_true", help="Skip mapping stages")
    parser.add_argument("--skip-inference", action="store_true", help="Skip inference")
    parser.add_argument("--skip-analysis", action="store_true", help="Skip analysis stage")
    parser.add_argument("--skip-completed", action="store_true", help="Skip sources with existing schema output files")
    parser.add_argument("--ontology-as-data", action="store_true",
                        help="Opt in to bounded superclass aggregation (not observed typing)")
    parser.add_argument("--extract-ontology", action="store_true", help="Extract ontology structure (TBox: rdfs:subClassOf, domain/range)")
    parser.add_argument("--extract-metadata", action="store_true", help="Extract infrastructure metadata (VoID/DCAT)")
    parser.add_argument("--no-enrichment", action="store_true", help="Skip definitions and observed examples")
    parser.add_argument("--examples-per-pattern", type=int, default=2, choices=range(0, 21),
                        help="Examples per class and pattern (0: definitions only; default: 2)")
    parser.add_argument("--ontology-scope", choices=["schema", "full"], default="schema",
                        help="Export schema-relevant ontology or all queried axioms")
    parser.add_argument("--output-dir", type=Path, help="Output directory")
    parser.add_argument("--output-suffix", type=str, default="", help="Suffix for output files (e.g., _local, _remote)")
    parser.add_argument(
        "--output-formats",
        nargs="+",
        choices=["void", "json-ld", "shacl", "pydantic", "json"],
        default=["json-ld", "void"],
        help="Output format(s) to generate (default: json-ld void)",
    )
    parser.add_argument("--base-port", type=int, default=7019, help="First local QLever port")
    parser.add_argument("--qlever-startup-timeout", type=int, default=600,
                        help="Seconds to wait for an index to load")
    parser.add_argument("--data-dir", type=Path, help="Data-directory")
    parser.add_argument("--no-download", action="store_true",
                        help="Use existing RDF and indices; do not fetch source data")
    parser.add_argument("--timeout", type=float, default=None,
                        help="Override query timeout in seconds (default: source setting)")
    parser.add_argument("--endpoint-status-file", type=Path, help="Endpoint health check JSON")
    parser.add_argument("--download-status-file", type=Path, help="Download health check JSON")

    args = parser.parse_args()

    # Build config
    import signal

    def stop_on_signal(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, stop_on_signal)
    repo_dir = Path(__file__).resolve().parents[1]
    config = PipelineConfig(base_dir=repo_dir.parent, repo_dir=repo_dir)
    if args.sources_file:
        config.sources_file = args.sources_file.resolve()

    if args.output_dir:
        config.output_dir = args.output_dir
    if args.data_dir:
        config.data_dir = args.data_dir
    if not 1024 <= args.base_port <= 60000:
        parser.error("--base-port must be between 1024 and 60000")
    if args.qlever_startup_timeout <= 0:
        parser.error("--qlever-startup-timeout must be positive")
    config.base_port = args.base_port
    config.qlever_startup_timeout = args.qlever_startup_timeout
    config.timeout = args.timeout
    config.no_download = args.no_download
    config.no_index = args.no_index
    config.output_suffix = args.output_suffix
    config.output_formats = args.output_formats
    config.endpoint_status_file = args.endpoint_status_file
    config.download_status_file = args.download_status_file
    config.skip_mining = args.skip_mining
    config.skip_mappings = args.skip_mappings
    config.skip_inference = args.skip_inference
    config.skip_completed = args.skip_completed
    config.skip_remote = args.local_only or args.grouped_only or args.lslod_cloud_only
    config.skip_local = args.remote_only or args.grouped_only or args.lslod_cloud_only
    config.extract_ontology = args.extract_ontology
    config.ontology_scope = args.ontology_scope
    config.ontology_as_data = args.ontology_as_data
    config.extract_metadata = args.extract_metadata
    config.enrich = not args.no_enrichment
    config.examples_per_pattern = args.examples_per_pattern

    # Load sources
    config.load_sources(args.sources, skip_providers=args.skip_providers)

    if not config.sources:
        log.error("No sources loaded. Check sources.yaml or --sources argument.")
        sys.exit(1)

    if args.preflight:
        preflight(config, grouped=args.grouped_only, remote=args.remote_only)
        return

    config.output_dir.mkdir(parents=True, exist_ok=True)
    # Build pipeline
    pipeline = Pipeline(config)

    # Handle specialized mining modes
    if args.grouped_only:
        pipeline.add_stage(GroupedMiningStage)
    elif args.lslod_cloud_only:
        pipeline.add_stage(LsLodCloudStage)
    else:
        # Normal mining pipeline
        if not config.skip_mining:
            if not config.skip_remote:
                pipeline.add_stage(RemoteMiningStage)
            if not config.skip_local:
                pipeline.add_stage(LocalMiningStage)

        if not config.skip_mappings:
            pipeline.add_stage(SSSOMSeedingStage)

        if not config.skip_inference:
            log.info("Inference is a separate workflow: scripts/infer_mappings.py")

        if not args.skip_analysis:
            pipeline.add_stage(AnalysisStage)

    results = pipeline.run()

    # Check for failures
    for stage_results in results.values():
        if isinstance(stage_results, dict) and not stage_results.get("success", True):
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
