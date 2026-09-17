"""Grouped operations for the pipeline command."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any

from rdfsolve.qlever import QleverConfig, build_provider_qleverfile
from rdfsolve.qlever.inputs import (
    cached_archives,
    expand_inputs,
    qlever_format,
    rdf_input_files,
)
from rdfsolve.schema_models.exporters.text import trim_descriptions as trim_export_text

from .config import Source
from .local import LocalMiningStage

log = logging.getLogger(__name__)


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
                    results["failed"].append(
                        {
                            "name": source.name,
                            "error": f"No cached individual index in {workdir}; use --local-only to prepare it",
                        }
                    )
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

            output_dir = self.config.output_dir / f"grouped_{group_name}"
            schema_path = output_dir / f"{group_name}_schema.json"
            if self.config.skip_completed and schema_path.exists():
                log.info("  Skipping: output already exists")
                results["skipped"].append(group_name)
                continue

            try:
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
                            results["failed"].append(
                                {
                                    "name": source.name,
                                    "error": f"No grouped or individual index for {source.name}",
                                }
                            )
                        else:
                            sources_with_existing_index.append((source, source_workdir))
                    continue
                source_data = []
                for source in group_sources:
                    source_workdir = self.config.data_dir / "qlever_workdirs" / source.name
                    source_workdir.mkdir(parents=True, exist_ok=True)

                    has_files = bool(
                        rdf_input_files(source_workdir) or cached_archives(source_workdir)
                    )

                    if not has_files and self._has_qlever_index(source_workdir, source.name):
                        sources_with_existing_index.append((source, source_workdir))
                        continue
                    if (
                        not has_files
                        and not self.config.no_download
                        and (
                            source.download_urls
                            or source.local_tar_url
                            or (source_workdir / "Qleverfile").exists()
                        )
                    ):
                        log.info(f"  -> Downloading data for {source.name}...")
                        try:
                            qleverfile = source_workdir / "Qleverfile"
                            if not qleverfile.exists():
                                self._prepare_qleverfile(
                                    source_workdir, source, self.config.base_port
                                )
                            self._execute_qleverfile(source_workdir, source)
                            has_files = bool(
                                rdf_input_files(source_workdir)
                                or cached_archives(source_workdir)
                            )
                        except Exception as dl_err:
                            log.warning(f"  -> Download failed for {source.name}: {dl_err}")

                    if not has_files:
                        if self._has_qlever_index(source_workdir, source.name):
                            log.info(
                                f"  -> No RDF files but found existing QLever index for {source.name}"
                            )
                            sources_with_existing_index.append((source, source_workdir))
                        else:
                            raise FileNotFoundError(
                                f"No prepared RDF inputs for {source.name}; decompress cached downloads first"
                            )
                        continue
                    source_data.append((source, source_workdir))

                if not source_data:
                    log.warning(f"  -> No data found for group {group_name}, skipping")
                    results["skipped"].append(group_name)
                    continue

                qleverfile = workdir / "Qleverfile"
                if not qleverfile.exists():
                    self._prepare_group_qleverfile(workdir, group_name, group_sources, port)

                log.info(f"  Indexing {len(source_data)} sources...")
                self._execute_group_qleverfile(workdir, group_name, source_data)
                if not self._has_qlever_index(workdir, group_name):
                    raise RuntimeError(
                        f"Index command returned without a complete index: {workdir}"
                    )

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

        if sources_with_existing_index:
            log.info(
                f"\n=== Mining {len(sources_with_existing_index)} sources with existing indices ==="
            )
            individual_port = port + 100  # Use different port range

            for source, workdir in sources_with_existing_index:
                suffix = self.config.output_suffix
                source_output_dir = self.config.output_dir / source.name
                schema_path = source_output_dir / f"{source.name}{suffix}_schema.json"
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
                            {
                                "name": source.name,
                                "error": "Server failed to start for existing index",
                            }
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
            if not source.download_urls and not source.local_provider:
                continue

            group_name = None

            if source.download_urls:
                first_url = (
                    source.download_urls[0]
                    if isinstance(source.download_urls, list)
                    else source.download_urls
                )
                hostname = urlparse(first_url).hostname
                if hostname:
                    if source.name.startswith("pubchem.ftp.") or (
                        hostname == "ftp.ncbi.nlm.nih.gov"
                        and urlparse(first_url).path.lower().startswith("/pubchem/")
                    ):
                        group_name = "pubchem.ftp"
                    elif "bio2rdf" in hostname or "bio2rdf" in source.name:
                        group_name = "bio2rdf"
                    elif "rdfportal" in hostname or "rdfportal" in source.name:
                        group_name = "rdfportal"
                    elif "dbcls" in hostname or "dbcls" in source.name:
                        group_name = "dbcls"

            if not group_name and source.local_provider:
                group_name = source.local_provider

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
        members = [source.qlever_entry() for source in group_sources]

        cfg = QleverConfig(
            memory_for_queries="80G",
            timeout="1200s",
            parser_buffer_size="4GB",
            parallel_parsing=False,
            num_triples_per_batch=1_000_000,
        )

        qleverfile_path = workdir / "Qleverfile"
        try:
            qleverfile_content = build_provider_qleverfile(
                group_name,
                members,
                self.config.data_dir,
                port,
                runtime="singularity",
                cfg=cfg,
                workdir=workdir,
            )
        except ValueError:
            if not qleverfile_path.exists():
                raise
            log.info("  Keeping the cached provider Qleverfile")
            return

        qleverfile_path.write_text(qleverfile_content, encoding="utf-8")
        log.info("  Generated provider Qleverfile")

    def _execute_group_qleverfile(
        self, workdir: Path, group_name: str, source_data: list[tuple[Source, Path]]
    ):
        """Execute group Qleverfile indexing using existing data."""
        import configparser

        qleverfile_path = workdir / "Qleverfile"
        config = configparser.ConfigParser()
        config.read(qleverfile_path)

        settings_json = config.get("index", "SETTINGS_JSON")

        settings_path = workdir / f"{group_name}.settings.json"
        settings_path.write_text(settings_json)

        expanded: list[Path] = []
        input_files = []
        for source, source_workdir in source_data:
            expanded += expand_inputs(source_workdir)
            files = rdf_input_files(source_workdir)
            if not files:
                raise ValueError(f"No prepared RDF inputs for {source.name} in {source_workdir}")
            graph_uri = f"http://rdfsolve.org/graph/{source.name}"
            input_files.extend((path, graph_uri) for path in files)

        if not input_files:
            raise ValueError(f"No input files found for group {group_name}")

        file_flags = []
        for file_path, graph_uri in input_files:
            file_flags.extend(
                ["-f", str(file_path), "-F", qlever_format(file_path), "-g", graph_uri]
            )

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
            *file_flags,
            "-p",
            config.get("index", "PARALLEL_PARSING"),
            "-b",
            config.get("index", "PARSER_BUFFER_SIZE", fallback="2GB"),
            "-m",
            config.get("index", "STXXL_MEMORY", fallback="16GB"),
        ]

        log.info(f"  Indexing {len(input_files)} files from {len(source_data)} sources...")
        try:
            subprocess.run(cmd, cwd=workdir, check=True)
        finally:
            for path in expanded:
                path.unlink(missing_ok=True)

    def _mine_grouped(self, group_name: str, sources: list[Source], port: int):
        from rdfsolve import SchemaMiner

        endpoint = f"http://localhost:{port}"
        graph_uris = [f"http://rdfsolve.org/graph/{s.name}" for s in sources]

        output_dir = self.config.output_dir / f"grouped_{group_name}"
        output_dir.mkdir(parents=True, exist_ok=True)

        report_path = output_dir / f"{group_name}_report.json"

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

        from rdfsolve.qlever.index_check import verify_named_graphs

        verify_named_graphs(miner._helper, graph_uris)

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
            if result.ontology:
                ontology_path = output_dir / f"{group_name}_ontology.ttl"
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
                metadata_path = output_dir / f"{group_name}_metadata.ttl"
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
            schema = miner.mine(dataset_name=group_name)

        self._save_schema_outputs(
            schema, output_dir, group_name, self.config.output_suffix, helper=miner.helper
        )
        self._require_complete(miner)
        schema_path = output_dir / f"{group_name}_schema.json"
        log.info(f"  -> Saved grouped schema to {schema_path}")
