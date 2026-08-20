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

    @property
    def mode(self) -> SourceMode:
        if self.endpoint and (self.local_provider or self.download_urls):
            return SourceMode.BOTH
        elif self.endpoint:
            return SourceMode.REMOTE
        elif self.local_provider or self.download_urls:
            return SourceMode.LOCAL
        return SourceMode.UNKNOWN

    @classmethod
    def from_dict(cls, d: dict) -> Source:
        download_urls = []
        for key in ["download_ttl", "download_nt", "download_nq", "download_rdf"]:
            urls = d.get(key, [])
            if isinstance(urls, str):
                urls = [urls]
            download_urls.extend(urls)

        return cls(
            name=d.get("name", ""),
            endpoint=d.get("endpoint"),
            local_provider=d.get("local_provider"),
            download_urls=download_urls,
            graph_uris=d.get("graph_uris", []),
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
            timeout=d.get("timeout"),
        )


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
    timeout: float = 300.0
    delay: float = 1.0
    chunk_size: int = 50000
    class_batch_size: int = 50
    benchmark: bool = True

    # QLever settings (for local mining)
    qlever_image: str = "docker://docker.io/adfreiburg/qlever:latest"
    base_port: int = 7019
    qlever_startup_timeout: int = 10000  # seconds to wait for QLever to start

    # Stage control
    skip_remote: bool = False
    skip_local: bool = False
    skip_mining: bool = False
    skip_mappings: bool = False
    skip_seeding: bool = False
    skip_inference: bool = False

    # Parallelism
    parallelism: int = 4

    # Output naming
    output_suffix: str = ""

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

    def run(self) -> dict[str, Any]:
        """Execute the stage."""
        log.info("=" * 70)
        log.info(f"STAGE: {self.name.upper()}")
        log.info("=" * 70)

        self.start_time = time.time()
        try:
            self.results = self._execute()
            self.results["success"] = True
        except Exception as e:
            log.exception(f"Stage {self.name} failed: {e}")
            self.results = {"success": False, "error": str(e)}
        finally:
            self.end_time = time.time()
            elapsed = self.end_time - self.start_time
            self.results["elapsed_seconds"] = elapsed
            log.info(f"Stage {self.name} completed in {elapsed:.1f}s")

        return self.results

    def _execute(self) -> dict[str, Any]:
        raise NotImplementedError


class RemoteMiningStage(Stage):
    """Mine schemas from remote SPARQL endpoints with health checking and rate limiting."""

    name = "remote_mining"

    def _execute(self) -> dict[str, Any]:
        from rdfsolve import SchemaMiner
        from rdfsolve.endpoint_health import (
            check_endpoint_health,
            get_polite_delay,
            update_endpoint_status,
        )

        sources = self.config.get_remote_sources()
        log.info(f"Mining {len(sources)} remote endpoints")

        results = {"mined": [], "failed": [], "skipped": []}
        output_dir = self.config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        for i, source in enumerate(sources, 1):
            log.info(f"[{i}/{len(sources)}] {source.name}")

            if not source.endpoint:
                results["skipped"].append(source.name)
                continue

            # Skip endpoints known to be down (failed health check recently)
            if source.endpoint_down and source.failure_count >= 3:
                log.warning(
                    f"  Skipping: endpoint marked as down ({source.failure_count} failures)"
                )
                results["skipped"].append(source.name)
                continue

            # Quick health check before mining (unless recently checked)
            needs_health_check = True
            if source.last_checked:
                from datetime import datetime, timezone

                try:
                    last_check = datetime.fromisoformat(source.last_checked)
                    age = (datetime.now(timezone.utc) - last_check).total_seconds()
                    if age < 3600:  # Within last hour
                        needs_health_check = False
                except ValueError:
                    pass

            if needs_health_check:
                log.info(f"  Checking endpoint health...")
                health = check_endpoint_health(source.endpoint, timeout=30)
                update_endpoint_status(source, health)
                log.info(f"  Status: {health.status}")

                if health.status != "up":
                    log.warning(f"  Skipping: endpoint is {health.status}")
                    results["skipped"].append(source.name)
                    continue

            # Determine polite delay
            polite_delay = get_polite_delay(source)
            if polite_delay > 0:
                log.info(f"  Using {polite_delay}s inter-request delay")

            try:
                source_output_dir = output_dir / source.name
                source_output_dir.mkdir(parents=True, exist_ok=True)

                suffix = self.config.output_suffix
                report_path = source_output_dir / f"{source.name}{suffix}_report.json"
                miner = SchemaMiner(
                    endpoint_url=source.endpoint,
                    source_name=source.name,
                    timeout=self.config.timeout or source.timeout or 300,
                    delay=polite_delay,
                    report_path=str(report_path),
                )

                schema = miner.mine(dataset_name=source.name)

                from datetime import datetime, timezone

                source.endpoint_status = "up"
                source.last_success = datetime.now(timezone.utc).isoformat()
                source.failure_count = 0
                source.endpoint_down = False

                schema_path = source_output_dir / f"{source.name}{suffix}_schema.jsonld"
                schema_path.write_text(
                    json.dumps(schema.to_jsonld(), indent=2),
                    encoding="utf-8",
                )

                void_path = source_output_dir / f"{source.name}{suffix}_void.ttl"
                try:
                    void_graph = schema.to_void_graph()
                    if void_graph:
                        void_ttl = void_graph.serialize(format="turtle")
                        void_path.write_text(void_ttl, encoding="utf-8")
                except Exception as e:
                    log.warning(f"  Could not generate VoID: {e}")

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
                    results["mined"].append(report)
                    log.info(
                        f"  -> {report['classes']} classes, {report['properties']} props, {report['queries_sent']} queries"
                    )
                else:
                    results["mined"].append({"name": source.name, "endpoint": source.endpoint})

            except Exception as e:
                # Update failure status
                source.failure_count += 1
                source.last_error = str(e)[:500]
                if source.failure_count >= 3:
                    source.endpoint_down = True

                log.error(f"  -> FAILED: {e}")
                results["failed"].append({"name": source.name, "error": str(e)})

        # Log summary
        log.info(
            f"Mining complete: {len(results['mined'])} succeeded, "
            f"{len(results['failed'])} failed, {len(results['skipped'])} skipped"
        )

        return results


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

            try:
                qleverfile = workdir / "Qleverfile"
                if not qleverfile.exists():
                    self._prepare_qleverfile(workdir, source, port)

                index_done = workdir / ".index.done"
                if not index_done.exists():
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

    def _ensure_qlever_image(self):
        """Ensure QLever Singularity image exists."""
        image_path = self.config.data_dir / "qlever.sif"
        if image_path.exists():
            return

        log.info("Pulling QLever Singularity image...")
        subprocess.run(
            [
                "singularity",
                "pull",
                "--disable-cache",
                str(image_path),
                self.config.qlever_image,
            ],
            check=True,
        )

    def _check_data_exists(self, workdir: Path, source: Source) -> bool:
        """Check if data files already exist."""
        for ext in ["*.ttl", "*.nt", "*.nq", "*.ttl.gz", "*.nt.gz"]:
            if list(workdir.glob(ext)):
                return True
        return False

    def _prepare_qleverfile(self, workdir: Path, source: Source, port: int):
        """Generate Qleverfile for source."""
        entry = {
            "name": source.name,
            **{k: getattr(source, k) for k in dir(source) if k.startswith("download_") and getattr(source, k)},
        }
        if hasattr(source, "local_tar_url") and source.local_tar_url:
            entry["local_tar_url"] = source.local_tar_url
        if hasattr(source, "graph_uris") and source.graph_uris:
            entry["graph_uris"] = source.graph_uris

        cfg = QleverConfig(
            memory_for_queries="30G",
            timeout="600s",
            parser_buffer_size="2GB",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_content = build_qleverfile(
            entry, self.config.data_dir, port, runtime="singularity", cfg=cfg
        )

        qleverfile_path = workdir / "Qleverfile"
        qleverfile_path.write_text(qleverfile_content)
        log.info(f"    Generated Qleverfile")

    def _execute_qleverfile(self, workdir: Path, source: Source, skip_download: bool = False):
        """Execute Qleverfile data download and indexing."""
        qleverfile_path = workdir / "Qleverfile"
        if not qleverfile_path.exists():
            raise ValueError(f"Qleverfile not found in {workdir}")

        import configparser
        config = configparser.ConfigParser()
        config.read(qleverfile_path)

        rdf_dir = workdir / "rdf"
        rdf_dir.mkdir(exist_ok=True)

        input_files_pattern = config.get("index", "INPUT_FILES")
        rdf_format = config.get("data", "FORMAT")
        settings_json = config.get("index", "SETTINGS_JSON")

        input_files = list(workdir.glob(input_files_pattern.replace("rdf/", "")))
        if not input_files:
            input_files = list(rdf_dir.glob(input_files_pattern.split("/")[-1]))

        if not input_files and not skip_download:
            get_data_cmd = config.get("data", "GET_DATA_CMD")
            log.info(f"    Downloading data...")
            subprocess.run(["bash", "-c", get_data_cmd], check=True, cwd=workdir)

            input_files = list(workdir.glob(input_files_pattern.replace("rdf/", "")))
            if not input_files:
                input_files = list(rdf_dir.glob(input_files_pattern.split("/")[-1]))

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
            source.name,
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

    def _kill_existing_qlever_on_port(self, port: int):
        """Kill any existing QLever server running on the specified port."""
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info.get('cmdline', [])
                    if cmdline and 'qlever-server' in ' '.join(cmdline):
                        # Check if this process is using our port
                        if f'-p {port}' in ' '.join(cmdline) or f'-p \'{port}\'' in ' '.join(cmdline):
                            log.warning(f"  Killing existing QLever server (PID {proc.pid}) on port {port}")
                            proc.kill()
                            proc.wait(timeout=5)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except ImportError:
            # psutil not available, try pkill
            try:
                subprocess.run(['pkill', '-f', f'qlever-server.*-p {port}'], check=False)
            except Exception:
                pass

    def _qlever_start(self, workdir: Path, name: str, port: int) -> int | None:
        """Start QLever server, return PID."""
        # Kill any existing server on this port first
        self._kill_existing_qlever_on_port(port)

        image_path = self.config.data_dir / "qlever.sif"

        cmd = [
            "singularity",
            "exec",
            "--bind",
            f"{workdir}:{workdir}",
            "-W",
            str(workdir),
            str(image_path),
            "bash",
            "-c",
            f"cd '{workdir}' && exec qlever-server -i '{name}' -j 8 -p '{port}' -m 40G -c 8G -e 4G -k 200 -s 1000s -a '{name}'",
        ]

        log_path = workdir / "server.log"
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        # Wait for server to start by checking log file
        timeout = self.config.qlever_startup_timeout
        iterations = timeout // 2  # Check every 2 seconds

        log.info(f"  Waiting for QLever server (timeout: {timeout}s)...")

        for i in range(iterations):
            time.sleep(2)

            # Check if process died
            if proc.poll() is not None:
                log.error("QLever server died during startup")
                if log_path.exists():
                    log.error(f"  Last log:\n{log_path.read_text()[-500:]}")
                return None

            # Check log file for ready message
            if log_path.exists():
                log_content = log_path.read_text()
                if "The server is ready, listening for requests" in log_content:
                    log.info(f"  QLever server started after ~{(i + 1) * 2}s")
                    return proc.pid

        log.error(f"QLever server did not start in {timeout}s")
        if log_path.exists():
            log.error(f"  Server log tail:\n{log_path.read_text()[-1000:]}")
        proc.terminate()
        return None

    def _qlever_stop(self, pid: int):
        """Stop QLever server."""
        import signal

        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def _mine_local(self, source: Source, port: int):
        """Mine schema from local QLever instance."""
        from rdfsolve import SchemaMiner

        endpoint = f"http://localhost:{port}"

        output_dir = self.config.output_dir / source.name
        output_dir.mkdir(parents=True, exist_ok=True)

        suffix = self.config.output_suffix
        report_path = output_dir / f"{source.name}{suffix}_report.json"

        # Use unlimited timeout for local QLever (we control the server)
        miner = SchemaMiner(
            endpoint_url=endpoint,
            source_name=source.name,
            timeout=86400.0,  # 24 hours - effectively unlimited for local queries
            delay=self.config.delay,
            report_path=str(report_path),
        )

        schema = miner.mine(dataset_name=source.name)

        schema_path = output_dir / f"{source.name}{suffix}_schema.jsonld"
        schema_path.write_text(json.dumps(schema.to_jsonld(), indent=2))

        void_path = output_dir / f"{source.name}{suffix}_void.ttl"
        try:
            void_graph = schema.to_void_graph()
            if void_graph:
                void_ttl = void_graph.serialize(format="turtle")
                void_path.write_text(void_ttl, encoding="utf-8")
        except Exception as e:
            log.warning(f"  Could not generate VoID: {e}")


class GroupedMiningStage(Stage):
    """Mine schemas from grouped local sources.

    Loads multiple related sources into ONE QLever instance with named graphs.
    """

    name = "grouped_mining"

    def _execute(self) -> dict[str, Any]:
        sources = self.config.get_local_sources()
        groups = self._identify_groups(sources)
        log.info(f"Identified {len(groups)} source groups for combined mining")

        results = {"groups_mined": [], "failed": [], "skipped": []}
        self._ensure_qlever_image()

        qlever_workdir = self.config.data_dir / "qlever_groups"
        qlever_workdir.mkdir(parents=True, exist_ok=True)
        port = self.config.base_port + 1000

        for group_name, group_sources in groups.items():
            log.info(f"[Group: {group_name}] {len(group_sources)} sources")

            workdir = qlever_workdir / group_name
            workdir.mkdir(parents=True, exist_ok=True)

            try:
                source_data = []
                for source in group_sources:
                    source_workdir = self.config.data_dir / "qlever_workdirs" / source.name
                    if not source_workdir.exists():
                        log.warning(f"  -> Data for {source.name} not found, skipping")
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

        return results

    def _identify_groups(self, sources: list[Source]) -> dict[str, list[Source]]:
        import re
        from urllib.parse import urlparse

        groups = {}
        endpoint_to_name = {}

        for source in sources:
            group_name = None

            if source.download_urls:
                first_url = source.download_urls[0] if isinstance(source.download_urls, list) else source.download_urls
                hostname = urlparse(first_url).hostname
                if hostname:
                    if "pubchem" in hostname or "pubchem" in source.name:
                        group_name = "pubchem.ftp"
                    elif "bio2rdf" in hostname or "bio2rdf" in source.name:
                        group_name = "bio2rdf"
                    elif "rdfportal" in hostname or "rdfportal" in source.name:
                        group_name = "rdfportal"
                    elif "dbcls" in hostname or "dbcls" in source.name:
                        group_name = "dbcls"
                    else:
                        group_name = hostname.replace(".", "_")

            if not group_name and source.endpoint:
                if source.endpoint not in endpoint_to_name:
                    domain = re.sub(r"https?://", "", source.endpoint)
                    domain = domain.split("/")[0].replace(":", "_").replace(".", "_")
                    endpoint_to_name[source.endpoint] = f"endpoint_{domain}"
                group_name = endpoint_to_name[source.endpoint]

            if not group_name and source.local_provider:
                group_name = source.local_provider

            if not group_name and source.bioregistry_prefix:
                group_name = source.bioregistry_prefix

            if not group_name:
                parts = source.name.split(".")
                group_name = parts[0]

            if group_name not in groups:
                groups[group_name] = []
            groups[group_name].append(source)

        return {k: v for k, v in groups.items() if len(v) > 1}

    def _prepare_group_qleverfile(
        self, workdir: Path, group_name: str, group_sources: list[Source], port: int
    ):
        """Generate provider Qleverfile for grouped sources."""
        members = []
        for source in group_sources:
            entry = {
                "name": source.name,
                **{k: getattr(source, k) for k in dir(source) if k.startswith("download_") and getattr(source, k)},
            }
            if hasattr(source, "local_tar_url") and source.local_tar_url:
                entry["local_tar_url"] = source.local_tar_url
            if hasattr(source, "graph_uris") and source.graph_uris:
                entry["graph_uris"] = source.graph_uris
            members.append(entry)

        cfg = QleverConfig(
            memory_for_queries="80G",
            timeout="1200s",
            parser_buffer_size="4GB",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_content = build_provider_qleverfile(
            group_name, members, self.config.data_dir, port, runtime="singularity", cfg=cfg
        )

        qleverfile_path = workdir / "Qleverfile"
        qleverfile_path.write_text(qleverfile_content)
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

    def _kill_existing_qlever_on_port(self, port: int):
        """Kill any existing QLever server running on the specified port."""
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info.get('cmdline', [])
                    if cmdline and 'qlever-server' in ' '.join(cmdline):
                        # Check if this process is using our port
                        if f'-p {port}' in ' '.join(cmdline) or f'-p \'{port}\'' in ' '.join(cmdline):
                            log.warning(f"  Killing existing QLever server (PID {proc.pid}) on port {port}")
                            proc.kill()
                            proc.wait(timeout=5)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except ImportError:
            # psutil not available, try pkill
            try:
                subprocess.run(['pkill', '-f', f'qlever-server.*-p {port}'], check=False)
            except Exception:
                pass

    def _qlever_start(self, workdir: Path, name: str, port: int) -> int | None:
        # Kill any existing server on this port first
        self._kill_existing_qlever_on_port(port)

        image_path = self.config.data_dir / "qlever.sif"

        cmd = [
            "singularity",
            "exec",
            "--bind",
            f"{workdir}:{workdir}",
            "-W",
            str(workdir),
            str(image_path),
            "bash",
            "-c",
            f"cd '{workdir}' && exec qlever-server -i '{name}' -j 8 -p '{port}' -m 40G -c 8G -e 4G -k 200 -s 1000s -a '{name}'",
        ]

        log_path = workdir / "server.log"
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        timeout = self.config.qlever_startup_timeout
        iterations = timeout // 2
        log.info(f"  Waiting for QLever server (timeout: {timeout}s)...")

        for i in range(iterations):
            time.sleep(2)

            if proc.poll() is not None:
                log.error("QLever server died during startup")
                if log_path.exists():
                    log.error(f"  Last log:\n{log_path.read_text()[-500:]}")
                return None

            if log_path.exists():
                log_content = log_path.read_text()
                if "The server is ready, listening for requests" in log_content:
                    log.info(f"  QLever server started after ~{(i + 1) * 2}s")
                    return proc.pid

        log.error(f"QLever server did not start in {timeout}s")
        if log_path.exists():
            log.error(f"  Server log tail:\n{log_path.read_text()[-1000:]}")
        proc.terminate()
        return None

    def _qlever_stop(self, pid: int):
        import signal

        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

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
            timeout=86400.0,
            delay=self.config.delay,
            report_path=str(report_path),
        )

        schema = miner.mine(dataset_name=group_name)

        # Save schema as JSON-LD
        schema_path = output_dir / f"{group_name}_schema.jsonld"
        schema_path.write_text(json.dumps(schema.to_jsonld(), indent=2))

        # Save VoID
        void_path = output_dir / f"{group_name}_void.ttl"
        try:
            void_graph = schema.to_void_graph()
            if void_graph:
                void_ttl = void_graph.serialize(format="turtle")
                void_path.write_text(void_ttl, encoding="utf-8")
        except Exception as e:
            log.warning(f"  Could not generate VoID: {e}")

        log.info(f"  -> Saved grouped schema to {schema_path}")

    def _ensure_qlever_image(self):
        image_path = self.config.data_dir / "qlever.sif"
        if image_path.exists():
            return

        log.info("Pulling QLever Singularity image...")
        subprocess.run(
            [
                "singularity",
                "pull",
                "--disable-cache",
                str(image_path),
                self.config.qlever_image,
            ],
            check=True,
        )


class LsLodCloudStage(Stage):
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
                    log.warning(f"  -> Data for {source.name} not found, skipping")
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
        members = []
        for source in all_sources:
            entry = {
                "name": source.name,
                **{k: getattr(source, k) for k in dir(source) if k.startswith("download_") and getattr(source, k)},
            }
            if hasattr(source, "local_tar_url") and source.local_tar_url:
                entry["local_tar_url"] = source.local_tar_url
            if hasattr(source, "graph_uris") and source.graph_uris:
                entry["graph_uris"] = source.graph_uris
            members.append(entry)

        cfg = QleverConfig(
            memory_for_queries="250G",
            timeout="3600s",
            parser_buffer_size="8GB",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_content = build_provider_qleverfile(
            "lslod_cloud", members, self.config.data_dir, port, runtime="singularity", cfg=cfg
        )

        qleverfile_path = workdir / "Qleverfile"
        qleverfile_path.write_text(qleverfile_content)
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

    def _kill_existing_qlever_on_port(self, port: int):
        """Kill any existing QLever server running on the specified port."""
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info.get('cmdline', [])
                    if cmdline and 'qlever-server' in ' '.join(cmdline):
                        # Check if this process is using our port
                        if f'-p {port}' in ' '.join(cmdline) or f'-p \'{port}\'' in ' '.join(cmdline):
                            log.warning(f"  Killing existing QLever server (PID {proc.pid}) on port {port}")
                            proc.kill()
                            proc.wait(timeout=5)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except ImportError:
            # psutil not available, try pkill
            try:
                subprocess.run(['pkill', '-f', f'qlever-server.*-p {port}'], check=False)
            except Exception:
                pass

    def _qlever_start(self, workdir: Path, name: str, port: int) -> int | None:
        """Start QLever server for LSLOD Cloud with increased resources."""
        # Kill any existing server on this port first
        self._kill_existing_qlever_on_port(port)

        image_path = self.config.data_dir / "qlever.sif"

        # Use more resources for the complete cloud
        cmd = [
            "singularity",
            "exec",
            "--bind",
            f"{workdir}:{workdir}",
            "-W",
            str(workdir),
            str(image_path),
            "bash",
            "-c",
            f"cd '{workdir}' && exec qlever-server -i '{name}' -j 16 -p '{port}' -m 60G -c 12G -e 8G -k 500 -s 2000s -a '{name}'",
        ]

        log_path = workdir / "server.log"
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        # Wait for server to start
        timeout = self.config.qlever_startup_timeout
        iterations = timeout // 2

        log.info(f"  Waiting for QLever server (timeout: {timeout}s)...")

        for i in range(iterations):
            time.sleep(2)

            if proc.poll() is not None:
                log.error("QLever server died during startup")
                if log_path.exists():
                    log.error(f"  Last log:\n{log_path.read_text()[-500:]}")
                return None

            if log_path.exists():
                log_content = log_path.read_text()
                if "The server is ready, listening for requests" in log_content:
                    log.info(f"  QLever server started after ~{(i + 1) * 2}s")
                    return proc.pid

        log.error(f"QLever server did not start in {timeout}s")
        if log_path.exists():
            log.error(f"  Server log tail:\n{log_path.read_text()[-1000:]}")
        proc.terminate()
        return None

    def _qlever_stop(self, pid: int):
        """Stop QLever server."""
        import signal

        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

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
            timeout=86400.0,
            delay=self.config.delay,
            report_path=str(report_path),
        )

        schema = miner.mine(dataset_name="lslod_cloud")

        schema_path = output_dir / "lslod_cloud_schema.jsonld"
        schema_path.write_text(json.dumps(schema.to_jsonld(), indent=2))

        void_path = output_dir / "lslod_cloud_void.ttl"
        try:
            void_graph = schema.to_void_graph()
            if void_graph:
                void_ttl = void_graph.serialize(format="turtle")
                void_path.write_text(void_ttl, encoding="utf-8")
        except Exception as e:
            log.warning(f"  Could not generate VoID: {e}")

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
            schema_path = self.config.output_dir / source.name / f"{source.name}_schema.jsonld"
            if schema_path.exists():
                schema = MinedSchema.from_jsonld(schema_path)
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

    def _ensure_qlever_image(self):
        """Ensure QLever Singularity image exists."""
        image_path = self.config.data_dir / "qlever.sif"
        if image_path.exists():
            return

        log.info("Pulling QLever Singularity image...")
        subprocess.run(
            [
                "singularity",
                "pull",
                "--disable-cache",
                str(image_path),
                self.config.qlever_image,
            ],
            check=True,
        )


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


class SeMRASeedingStage(Stage):
    """Seed SeMRA mappings."""

    name = "semra_seeding"

    def _execute(self) -> dict[str, Any]:
        mappings_dir = self.config.output_dir / "mappings" / "semra"
        mappings_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"Seeding SeMRA mappings to {mappings_dir}")

        existing = list(mappings_dir.glob("*.jsonld"))

        return {
            "output_dir": str(mappings_dir),
            "existing_files": len(existing),
        }


class InstanceMatchingStage(Stage):
    """Run instance-level matching across endpoints."""

    name = "instance_matching"

    def _execute(self) -> dict[str, Any]:

        mappings_dir = self.config.output_dir / "mappings" / "instance_matching"
        mappings_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"Running instance matching to {mappings_dir}")

        # Get bioregistry prefixes from sources
        prefixes = set()
        for source in self.config.sources:
            if source.bioregistry_prefix:
                prefixes.add(source.bioregistry_prefix)

        log.info(f"Found {len(prefixes)} unique bioregistry prefixes")

        # This would run instance matching for each prefix
        existing = list(mappings_dir.glob("*.jsonld"))

        return {
            "output_dir": str(mappings_dir),
            "prefixes": len(prefixes),
            "existing_files": len(existing),
        }


class ClassDerivationStage(Stage):
    """Derive class-level mappings from instance evidence."""

    name = "class_derivation"

    def _execute(self) -> dict[str, Any]:

        mappings_dir = self.config.output_dir / "mappings"
        class_dir = mappings_dir / "class_derived"
        class_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"Deriving class mappings to {class_dir}")

        # Find instance mapping files
        instance_files = list((mappings_dir / "instance_matching").glob("*.jsonld"))
        sssom_files = list((mappings_dir / "sssom").glob("*.jsonld"))
        semra_files = list((mappings_dir / "semra").glob("*.jsonld"))

        input_files = instance_files + sssom_files + semra_files
        log.info(f"Found {len(input_files)} input mapping files")

        existing = list(class_dir.glob("*.jsonld"))

        return {
            "output_dir": str(class_dir),
            "input_files": len(input_files),
            "existing_files": len(existing),
        }


class InferenceStage(Stage):
    """Expand mappings using SeMRA inference."""

    name = "inference"

    def _execute(self) -> dict[str, Any]:

        mappings_dir = self.config.output_dir / "mappings"
        inference_dir = mappings_dir / "inferenced"
        inference_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"Running inference expansion to {inference_dir}")

        # Find all mapping files
        input_files = []
        for subdir in ["sssom", "semra", "class_derived"]:
            dir_path = mappings_dir / subdir
            if dir_path.exists():
                input_files.extend(dir_path.glob("*.jsonld"))

        log.info(f"Found {len(input_files)} input mapping files")

        existing = list(inference_dir.glob("*.jsonld"))

        return {
            "output_dir": str(inference_dir),
            "input_files": len(input_files),
            "existing_files": len(existing),
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
        log.info(f"PIPELINE COMPLETE in {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log.info("=" * 70)

        # Save results
        results_path = self.config.output_dir / "pipeline_results.json"
        results_path.write_text(json.dumps(self.results, indent=2, default=str))

        return self.results


# CLI


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
    parser.add_argument(
        "--skip-providers", nargs="+", help="Skip sources from these providers (e.g., idsm)"
    )
    parser.add_argument("--skip-mining", action="store_true", help="Skip mining stages")
    parser.add_argument("--skip-mappings", action="store_true", help="Skip mapping stages")
    parser.add_argument("--skip-inference", action="store_true", help="Skip inference")
    parser.add_argument("--skip-analysis", action="store_true", help="Skip analysis stage")
    parser.add_argument("--output-dir", type=Path, help="Output directory")
    parser.add_argument("--output-suffix", type=str, default="", help="Suffix for output files (e.g., _local, _remote)")
    parser.add_argument("--data-dir", type=Path, help="Data directory")
    parser.add_argument("--timeout", type=float, default=300.0, help="Query timeout")
    parser.add_argument("--endpoint-status-file", type=Path, help="Endpoint health check JSON")
    parser.add_argument("--download-status-file", type=Path, help="Download health check JSON")

    args = parser.parse_args()

    # Build config
    config = PipelineConfig()

    if args.output_dir:
        config.output_dir = args.output_dir
    if args.data_dir:
        config.data_dir = args.data_dir
    config.timeout = args.timeout
    config.output_suffix = args.output_suffix
    config.endpoint_status_file = args.endpoint_status_file
    config.download_status_file = args.download_status_file
    config.skip_mining = args.skip_mining
    config.skip_mappings = args.skip_mappings
    config.skip_inference = args.skip_inference
    config.skip_remote = args.local_only or args.grouped_only or args.lslod_cloud_only
    config.skip_local = args.remote_only or args.grouped_only or args.lslod_cloud_only

    # Load sources
    config.load_sources(args.sources, skip_providers=args.skip_providers)

    if not config.sources:
        log.error("No sources loaded. Check sources.yaml or --sources argument.")
        sys.exit(1)

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
            pipeline.add_stage(SeMRASeedingStage)
            pipeline.add_stage(InstanceMatchingStage)
            pipeline.add_stage(ClassDerivationStage)

        if not config.skip_inference:
            pipeline.add_stage(InferenceStage)

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
