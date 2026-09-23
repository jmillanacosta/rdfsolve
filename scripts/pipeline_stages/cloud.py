"""Cloud operations for the pipeline command."""

from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path
from typing import Any

from rdfsolve.qlever import QleverConfig, build_provider_qleverfile
from rdfsolve.qlever.inputs import rdf_input_files

from .config import Source
from .local import LocalMiningStage
from rdfsolve.config import mint

log = logging.getLogger(__name__)


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
                has_files = any(list(source_workdir.glob(ext)) for ext in ["*.ttl", "*.nt", "*.nq"])
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

    def _prepare_cloud_qleverfile(self, workdir: Path, all_sources: list[Source], port: int):
        """Generate Qleverfile for LSLOD Cloud."""
        qleverfile_path = workdir / "Qleverfile"
        if qleverfile_path.exists():
            return
        members = [source.qlever_entry() for source in all_sources]

        cfg = QleverConfig(
            memory_for_queries="250G",
            timeout="3600s",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_content = build_provider_qleverfile(
            "lslod_cloud",
            members,
            self.config.data_dir,
            port,
            runtime="singularity",
            cfg=cfg,
            workdir=workdir,
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
            files = rdf_input_files(source_workdir)
            if not files:
                raise ValueError(f"No prepared RDF inputs for {source.name} in {source_workdir}")
            graph_uri = mint("graph", source.name)
            input_files.extend((path, graph_uri) for path in files)

        if not input_files:
            raise ValueError("No input files found for LSLOD Cloud")

        log.info(f"  Found {len(input_files)} RDF files across {len(source_data)} sources")

        file_flags = []
        for file_path, graph_uri in input_files:
            file_flags.extend(["-f", str(file_path), "-F", file_path.suffix[1:], "-g", graph_uri])

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
            "-b",
            config.get("index", "PARSER_BUFFER_SIZE", fallback=QleverConfig().parser_buffer_size),
            "-m",
            config.get("index", "STXXL_MEMORY", fallback="16GB"),
        ]

        subprocess.run(cmd, cwd=workdir, check=True)

    def _mine_cloud(self, source_data: list[tuple[Source, Path]], port: int):
        """Mine schema from the complete LSLOD Cloud."""
        from rdfsolve import SchemaMiner

        endpoint = f"http://localhost:{port}"

        graph_uris = [mint("graph", s.name) for s, _ in source_data]

        log.info(f"  Mining with {len(graph_uris)} named graphs")

        output_dir = self.config.output_dir / "lslod_cloud"
        output_dir.mkdir(parents=True, exist_ok=True)

        report_path = output_dir / "lslod_cloud_report.json"

        miner = SchemaMiner(
            endpoint_url=endpoint,
            graph_uris=graph_uris,
            timeout=self.config.timeout if self.config.timeout is not None else 600.0,
            delay=self.config.delay,
            sparql_engine="qlever",
            chunk_size=self.config.chunk_size,
            class_batch_size=self.config.class_batch_size,
            class_chunk_size=self.config.class_chunk_size,
            enrich=self.config.enrich,
            examples_per_pattern=self.config.examples_per_pattern,
            max_response_bytes=self.config.max_response_bytes,
            report_path=str(report_path),
        )

        schema = miner.mine(dataset_name="lslod_cloud")

        self._save_schema_outputs(
            schema, output_dir, "lslod_cloud", self.config.output_suffix, helper=miner.helper
        )
        self._require_complete(miner)
        schema_path = output_dir / "lslod_cloud_schema.json"
        log.info(f"  -> Saved LSLOD Cloud schema to {schema_path}")

        log.info("  Generating SSSOM class mappings...")
        self._save_schema_connectivity(source_data, output_dir)

    def _save_schema_connectivity(self, source_data, output_dir):
        """Retain per-dataset schema links and shared vocabulary as distinct evidence."""
        from networkx import node_link_data

        from rdfsolve.analysis.connectivity import build_connectivity
        from rdfsolve.schema_models.core import MinedSchema

        schemas = {}
        for source, _ in source_data:
            path = self.config.output_dir / source.name / f"{source.name}_schema.json"
            if path.exists():
                schemas[source.name] = MinedSchema.from_json(path)
        if not schemas:
            log.warning(
                "No individual schema snapshots available for source-qualified connectivity"
            )
            return
        graph = build_connectivity(schemas)
        (output_dir / "class_connectivity.json").write_text(
            json.dumps(node_link_data(graph), indent=2)
        )
