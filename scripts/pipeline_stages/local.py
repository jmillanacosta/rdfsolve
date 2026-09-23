"""Local operations for the pipeline command."""

from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path
from typing import Any

from rdfsolve.qlever import QleverConfig, build_qleverfile
from rdfsolve.qlever.inputs import (
    expand_inputs, graph_input_directory, mapped_input_files, qlever_format, rdf_input_files,
)
from rdfsolve.schema_models.exporters.text import trim_descriptions as trim_export_text

from .base import PartialMiningError, Stage
from .config import Source

log = logging.getLogger(__name__)


class LocalMiningStage(Stage):
    """Mine schemas from local RDF dumps using QLever."""

    name = "local_mining"

    def _execute(self) -> dict[str, Any]:
        sources = self.config.get_local_sources()
        log.info(f"Processing {len(sources)} local sources")

        results = {"indexed": [], "mined": [], "partial": [], "failed": [], "skipped": []}

        self._ensure_qlever_image()

        qlever_workdir = self.config.data_dir / "qlever_workdirs"
        qlever_workdir.mkdir(parents=True, exist_ok=True)

        for i, source in enumerate(sources, 1):
            log.info(f"[{i}/{len(sources)}] {source.name}")
            # One port per source: a stopped server does not release it immediately.
            port = self.config.base_port + i - 1

            workdir = qlever_workdir / source.name
            workdir.mkdir(parents=True, exist_ok=True)

            suffix = self.config.output_suffix
            source_output_dir = self.config.output_dir / source.name
            schema_path = source_output_dir / f"{source.name}{suffix}_schema.json"
            if self.config.skip_completed and schema_path.exists():
                log.info("  Skipping: output already exists")
                results["skipped"].append(source.name)
                continue

            try:
                has_index = self._has_qlever_index(workdir, source.name)
                if not has_index and source.download_error and not self.config.no_download:
                    self._record_skip(source, source.download_error)
                    results["skipped"].append(source.name)
                    continue
                if not has_index and not self.config.no_index:
                    self._prepare_qleverfile(workdir, source, port)

                index_done = workdir / ".index.done"
                if not has_index:
                    if self.config.no_index:
                        raise FileNotFoundError(
                            f"No cached index in {workdir}; prepare it before mining"
                        )
                    log.info("  Executing Qleverfile (download + index)...")
                    try:
                        self._execute_qleverfile(workdir, source)
                        index_done.touch()
                        results["indexed"].append(source.name)
                    except Exception as idx_err:
                        log.error(f"  -> Failed: {idx_err}")
                        log.warning(f"  -> Skipping {source.name}")
                        results["failed"].append(
                            {
                                "name": source.name,
                                "error": f"Qleverfile execution failed: {idx_err}",
                            }
                        )
                        continue

                log.info(f"  Starting QLever on port {port}...")
                server_pid = self._qlever_start(workdir, source.name, port)

                if server_pid:
                    try:
                        log.info("  Mining schema...")
                        self._mine_local(source, port)
                        results["mined"].append(source.name)
                    finally:
                        self._qlever_stop(server_pid)
                else:
                    log.warning(f"  -> Server failed to start, skipping {source.name}")
                    results["failed"].append(
                        {"name": source.name, "error": "Server failed to start"}
                    )

            except Exception as e:
                state = "partial" if isinstance(e, PartialMiningError) else "failed"
                log.warning("  -> %s: %s", state.upper(), e)
                results[state].append({"name": source.name, "error": str(e)})

        return results

    def _has_qlever_index(self, workdir: Path, source_name: str) -> bool:
        """Reuse existing indices; do not overwrite partial index files."""
        from rdfsolve.qlever.index_check import has_cached_index

        return has_cached_index(workdir, source_name)

    def _prepare_qleverfile(self, workdir: Path, source: Source, port: int):
        """Generate the Qleverfile for a source without a cached index."""
        entry = source.qlever_entry()

        cfg = QleverConfig(
            memory_for_queries="80G",
            timeout="600s",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_path = workdir / "Qleverfile"
        try:
            qleverfile_content = build_qleverfile(
                entry, self.config.data_dir, port, runtime="singularity", cfg=cfg, workdir=workdir
            )
        except ValueError:
            if not qleverfile_path.exists():
                raise
            log.info("    Keeping the cached Qleverfile")
            return

        qleverfile_path.write_text(qleverfile_content, encoding="utf-8")
        log.info("    Generated Qleverfile")

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

        settings_json = config.get("index", "SETTINGS_JSON")

        directories = [graph_input_directory(workdir, graph) for graph in source.graph_sources] or [workdir]
        expanded = [path for directory in directories for path in expand_inputs(directory)]
        try:
            if not all(rdf_input_files(directory) for directory in directories):
                if not skip_download and not self.config.no_download:
                    get_data_cmd = config.get("data", "GET_DATA_CMD")
                    subprocess.run(["bash"], input=get_data_cmd, text=True, check=True, cwd=workdir)
                    expanded.extend(path for directory in directories for path in expand_inputs(directory))
            if source.graph_sources:
                mapped = mapped_input_files(workdir, list(source.graph_sources))
                for graph, fields in source.graph_sources.items():
                    expected = sum(len(urls) for urls in fields.values())
                    found = sum(mapped_graph == graph for _, mapped_graph in mapped)
                    if found != expected:
                        raise ValueError(f"Graph {graph} has {found} input files; expected {expected}")
            else:
                mapped = [(path, "") for path in rdf_input_files(workdir)]
            if not mapped:
                raise ValueError(f"No prepared RDF inputs in {workdir}")
        except BaseException:
            for path in expanded:
                path.unlink(missing_ok=True)
            raise

        settings_path = workdir / f"{source.name}.settings.json"
        settings_path.write_text(settings_json)
        file_flags = []
        for path, graph in mapped:
            file_flags.extend(["-f", str(path), "-F", qlever_format(path)])
            if graph:
                file_flags.extend(["-g", graph])

        image_path = self.config.data_dir / "qlever.sif"
        cmd = [
            "singularity",
            "exec",
            "--bind",
            f"{self.config.data_dir}:{self.config.data_dir}",
            str(image_path),
            "qlever-index",
            "-i",
            config.get("data", "NAME", fallback=source.name),
            "-s",
            str(settings_path),
            *file_flags,
            "-p",
            config.get("index", "PARALLEL_PARSING"),
            "-b",
            config.get("index", "PARSER_BUFFER_SIZE", fallback=QleverConfig().parser_buffer_size),
            "-m",
            config.get("index", "STXXL_MEMORY", fallback="16GB"),
        ]

        log.info(f"    Indexing {len(mapped)} files...")
        try:
            subprocess.run(cmd, cwd=workdir, check=True)
        finally:
            for path in expanded:
                path.unlink(missing_ok=True)

    def _mine_local(
        self,
        source: Source,
        port: int,
        *,
        graph_uris: list[str] | None = None,
        mining_context: str = "local_distribution",
    ):
        """Mine one dataset from a local QLever instance."""
        output_dir = self.config.output_dir / source.name
        output_dir.mkdir(parents=True, exist_ok=True)
        suffix = self.config.output_suffix
        report_path = output_dir / f"{source.name}{suffix}_report.json"
        miner = self._local_miner(
            port, graph_uris if graph_uris is not None else source.graph_uris or None,
            report_path, type_context_graph_uris=source.type_context_graph_uris,
        )
        schema = self._mine_schema(miner, source.name, output_dir,
                                   ontology_graph_uris=source.ontology_graph_uris or None)
        with self._output_phase(miner, report_path):
            self._save_dataset_outputs(source, schema, output_dir, miner.helper, mining_context)
        self._require_complete(miner)

    def _local_miner(self, port: int, graph_uris: list[str] | None, report_path: Path,
                     *, type_context_graph_uris: list[str] | None = None):
        """Create a miner for a local QLever instance."""
        from rdfsolve import SchemaMiner

        endpoint = f"http://localhost:{port}"
        miner = SchemaMiner(
            endpoint_url=endpoint,
            graph_uris=graph_uris,
            type_context_graph_uris=type_context_graph_uris,
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

        return miner

    def _mine_schema(self, miner, name: str, output_dir: Path,
                     *, ontology_graph_uris: list[str] | None = None):
        """Mine *name* with the configured optional phases and write their RDF files."""
        suffix = self.config.output_suffix
        if self.config.extract_ontology or self.config.extract_metadata or self.config.ontology_as_data:
            from rdfsolve.mining import mine_with_ontology

            result = mine_with_ontology(
                miner,
                extract_ontology=self.config.extract_ontology,
                ontology_scope=self.config.ontology_scope,
                ontology_graph_uris=ontology_graph_uris,
                ontology_as_data=self.config.ontology_as_data,
                ontology_term_budget=self.config.ontology_term_budget,
                extract_metadata=self.config.extract_metadata,
                dataset_name=name,
            )
            schema = result.data_schema
            if result.ontology:
                ontology_path = output_dir / f"{name}{suffix}_ontology.ttl"
                try:
                    ontology_graph = trim_export_text(
                        result.ontology, self.config.trim_descriptions
                    ).to_rdf_graph()
                    if ontology_graph:
                        schema.annotate_rdf(
                            ontology_graph,
                            include_examples=False,
                            trim_descriptions=self.config.trim_descriptions,
                        )
                        ont_ttl = ontology_graph.serialize(format="turtle")
                        ontology_path.write_text(ont_ttl, encoding="utf-8")
                except Exception as e:
                    raise RuntimeError(f"  Could not generate ontology.ttl: {e}") from e
            if result.metadata:
                metadata_path = output_dir / f"{name}{suffix}_metadata.ttl"
                try:
                    metadata_graph = trim_export_text(
                        result.metadata, self.config.trim_descriptions
                    ).to_rdf_graph()
                    if metadata_graph:
                        meta_ttl = metadata_graph.serialize(format="turtle")
                        metadata_path.write_text(meta_ttl, encoding="utf-8")
                except Exception as e:
                    raise RuntimeError(f"  Could not generate metadata.ttl: {e}") from e
        else:
            schema = miner.mine(dataset_name=name)

        return schema

    def _save_dataset_outputs(
        self, source: Source, schema, output_dir: Path, helper, mining_context: str
    ) -> None:
        """Write one dataset's schema exports and its separate evidence files."""
        output_dir.mkdir(parents=True, exist_ok=True)
        suffix = self.config.output_suffix
        self._save_schema_outputs(schema, output_dir, source.name, suffix, helper=helper)
        from rdfsolve.evidence.local_ontology_files import archive_local_ontology_files

        owl_urls = source.download_fields.get("download_owl") or []
        if isinstance(owl_urls, str):
            owl_urls = [owl_urls]
        local_ontology_files = archive_local_ontology_files(
            source_dataset_id=source.name,
            urls=[str(url) for url in owl_urls if url],
            source_workdir=self.config.data_dir / "qlever_workdirs" / source.name,
            dataset_output_dir=output_dir,
        )
        self._save_ontology_discovery(
            schema,
            output_dir,
            source.name,
            suffix,
            helper=helper,
            mining_context=mining_context,
            local_ontology_file_candidates=local_ontology_files,
        )
        self._save_declared_artifacts(
            source,
            output_dir,
            source.name,
            suffix,
            helper=helper,
            access_context=mining_context,
        )
        self._save_property_usage_evidence(
            schema, output_dir, source.name, suffix, helper=helper
        )
        (output_dir / f"{source.name}{suffix}_schema.json").write_text(json.dumps(schema.to_dict(), indent=2))

    def _ensure_qlever_image(self):
        """Require the prepared image; do not pull a new engine during mining."""
        image = self.config.data_dir / "qlever.sif"
        if not image.is_file():
            raise FileNotFoundError(f"Prepare the QLever image before mining: {image}")

    def _qlever_start(self, workdir: Path, name: str, port: int) -> int:
        from rdfsolve.qlever.lifecycle import start_server

        process = start_server(
            self.config.data_dir / "qlever.sif",
            workdir,
            name,
            port,
            startup_timeout=self.config.qlever_startup_timeout,
        )
        self._servers[process.pid] = process
        return process.pid

    def _qlever_stop(self, pid: int):
        from rdfsolve.qlever.lifecycle import stop_server

        process = self._servers.pop(pid, None)
        if process is not None:
            stop_server(process)
