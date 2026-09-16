"""Local operations for the pipeline command."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any

from rdfsolve.qlever import QleverConfig, build_qleverfile
from rdfsolve.schema_models.exporters.text import trim_descriptions as trim_export_text

from .base import Stage
from .config import Source

log = logging.getLogger(__name__)


class LocalMiningStage(Stage):
    """Mine schemas from local RDF dumps using QLever."""

    name = "local_mining"

    def _execute(self) -> dict[str, Any]:
        sources = self.config.get_local_sources()
        log.info(f"Processing {len(sources)} local sources")

        results = {"indexed": [], "mined": [], "failed": [], "skipped": []}

        self._ensure_qlever_image()

        qlever_workdir = self.config.data_dir / "qlever_workdirs"
        qlever_workdir.mkdir(parents=True, exist_ok=True)

        port = self.config.base_port

        for i, source in enumerate(sources, 1):
            log.info(f"[{i}/{len(sources)}] {source.name}")

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
                qleverfile = workdir / "Qleverfile"
                has_index = self._has_qlever_index(workdir, source.name)
                if not qleverfile.exists() and not has_index:
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
        from rdfsolve.qlever.index_check import has_cached_index

        return has_cached_index(workdir, source_name)

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

        input_files_pattern = config.get("index", "INPUT_FILES")
        rdf_format = config.get("data", "FORMAT")
        settings_json = config.get("index", "SETTINGS_JSON")

        input_files = self._qlever_input_files(workdir, input_files_pattern)

        if not input_files and not skip_download and not self.config.no_download:
            get_data_cmd = config.get("data", "GET_DATA_CMD")
            log.info("    Downloading data...")
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
            "-b",
            config.get("index", "PARSER_BUFFER_SIZE", fallback="2GB"),
            "-m",
            config.get("index", "STXXL_MEMORY", fallback="16GB"),
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

        miner = SchemaMiner(
            endpoint_url=endpoint,
            timeout=self.config.timeout if self.config.timeout is not None else 600.0,
            delay=self.config.delay,
            sparql_engine="qlever",
            chunk_size=self.config.chunk_size,
            class_batch_size=self.config.class_batch_size,
            enrich=self.config.enrich,
            examples_per_pattern=self.config.examples_per_pattern,
            max_response_bytes=self.config.max_response_bytes,
            report_path=str(report_path),
        )

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
            if result.ontology:
                ontology_path = output_dir / f"{source.name}{suffix}_ontology.ttl"
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
                metadata_path = output_dir / f"{source.name}{suffix}_metadata.ttl"
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
            schema = miner.mine(dataset_name=source.name)

        self._save_schema_outputs(schema, output_dir, source.name, suffix, helper=miner.helper)
        self._require_complete(miner)

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
