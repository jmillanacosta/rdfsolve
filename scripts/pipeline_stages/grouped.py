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
from .base import PartialMiningError
from .local import LocalMiningStage
from rdfsolve.config import mint

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

        results = {"groups_mined": [], "partial": [], "failed": [], "skipped": [], "indexed_individually": []}
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
            suffix = self.config.output_suffix
            completed = [
                self.config.output_dir / source.name / f"{source.name}{suffix}_schema.json"
                for source in group_sources
            ]
            if self.config.skip_completed and completed and all(path.exists() for path in completed):
                log.info("  Skipping: all per-dataset outputs already exist")
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
                    # Mining members one by one would lose edges between their graphs.
                    for source in group_sources:
                        results["failed"].append(
                            {
                                "name": source.name,
                                "error": (
                                    f"No grouped index for {group_name} in {workdir}; "
                                    "build it from the prepared inputs (grouped mode without "
                                    "--no-index) instead of mining members separately"
                                ),
                            }
                        )
                    continue
                source_data = []
                for source in group_sources:
                    source_workdir = self.config.data_dir / "qlever_workdirs" / source.name
                    source_workdir.mkdir(parents=True, exist_ok=True)

                    has_files = bool(
                        rdf_input_files(source_workdir) or cached_archives(source_workdir)
                    )

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
                        # An individual index cannot stand in: the group index must hold
                        # every member graph for edges between them to be mined.
                        raise FileNotFoundError(
                            f"No prepared RDF inputs for group member {source.name} in "
                            f"{source_workdir}; prepare it before building the {group_name} index"
                        )
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
                        log.info("  Mining the group across its dataset graphs...")
                        self._mine_grouped(
                            group_name, [source for source, _ in source_data], port
                        )
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
                state = "partial" if isinstance(e, PartialMiningError) else "failed"
                log.warning("  -> %s: %s", state.upper(), e)
                results[state].append({"group": group_name, "error": str(e)})

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
                    state = "partial" if isinstance(e, PartialMiningError) else "failed"
                    log.warning("[%s] -> %s: %s", source.name, state.upper(), e)
                    results[state].append({"name": source.name, "error": str(e)})

        return results

    def _identify_groups(self, sources: list[Source]) -> dict[str, list[Source]]:
        """Identify groups of sources that can be mined together locally.

        Only sources with download_urls or local_provider are included -
        endpoint-only sources cannot be grouped for local mining.
        """
        groups: dict[str, list[Source]] = {}
        for source in sources:
            if source.local_provider and not source.graph_sources:
                groups.setdefault(source.local_provider, []).append(source)

        return groups

    def _prepare_group_qleverfile(
        self, workdir: Path, group_name: str, group_sources: list[Source], port: int
    ):
        """Generate provider Qleverfile for grouped sources."""
        members = [source.qlever_entry() for source in group_sources]

        cfg = QleverConfig(
            memory_for_queries="80G",
            timeout="1200s",
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
            if len(source.graph_uris) > 1:
                raise ValueError(f"Prepare an index with the recorded graph mapping for {source.name}")
            graph_uri = source.graph_uris[0] if source.graph_uris else mint("graph", source.name)
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
            config.get("index", "PARSER_BUFFER_SIZE", fallback=QleverConfig().parser_buffer_size),
            "-m",
            config.get("index", "STXXL_MEMORY", fallback="16GB"),
        ]

        log.info(f"  Indexing {len(input_files)} files from {len(source_data)} sources...")
        try:
            subprocess.run(cmd, cwd=workdir, check=True)
        finally:
            for path in expanded:
                path.unlink(missing_ok=True)

    def _mine_grouped(self, group_name: str, sources: list[Source], port: int) -> list[str]:
        """Mine all dataset graphs of a group together, then write one schema per dataset.

        Providers such as PubChem link entities across their datasets, and their
        endpoints serve all graphs at once. Mining each dataset graph alone would
        lose the classes of objects typed in another graph. Types therefore
        resolve over every graph of the shared index, the counts phase attributes
        each edge to its graph, and each dataset keeps the edges in its own graph.
        """
        import shutil

        from rdfsolve.mining.edge_graph_split import split_by_edge_graph, unattributed_patterns
        from rdfsolve.mining.ontology_as_data import pattern_classes

        suffix = self.config.output_suffix
        group_dir = self.config.output_dir / f"grouped_{group_name}"
        group_dir.mkdir(parents=True, exist_ok=True)
        group_report = group_dir / f"{group_name}{suffix}_report.json"
        graphs = {source.name: source.graph_uris or [mint("graph", source.name)] for source in sources}
        data_graphs = list(dict.fromkeys(graph for scope in graphs.values() for graph in scope))
        type_graphs = list(dict.fromkeys(graph for source in sources for graph in source.type_context_graph_uris))
        ontology_graphs = list(dict.fromkeys(graph for source in sources for graph in source.ontology_graph_uris))

        log.info("  Mining %d dataset graphs of %s together", len(graphs), group_name)
        miner = self._local_miner(port, data_graphs, group_report, type_context_graph_uris=type_graphs)
        schema = self._mine_schema(miner, group_name, group_dir, ontology_graph_uris=ontology_graphs or None)
        member_reports: list[Path] = []
        try:
            with self._output_phase(miner, group_report):
                self._save_schema_outputs(schema, group_dir, group_name, suffix, helper=miner.helper)
                missing = unattributed_patterns(schema)
                if missing:
                    from rdfsolve.mining.report_tracking import ReportCollector

                    miner.last_report.abort_reason = f"{len(missing)} patterns lack edge-graph attribution"
                    ReportCollector(miner.last_report, group_report).flush()
                    log.warning(
                        "  %d patterns have no per-graph count and appear only in the group schema",
                        len(missing),
                    )

                for source in sources:
                    output_dir = self.config.output_dir / source.name
                    output_dir.mkdir(parents=True, exist_ok=True)
                    report_path = output_dir / f"{source.name}{suffix}_report.json"
                    member_reports.append(report_path)
                    shutil.copyfile(group_report, report_path)
                    graph_uri = graphs[source.name]
                    part = split_by_edge_graph(
                        schema, graph_uri, source.name, declared_classes=miner.declared_classes
                    )
                    classes = sorted(pattern_classes(part.patterns) - miner.subsumed_classes)
                    counts, states = miner.count_class_entities(classes, graph_uri)
                    part.about.class_entity_counts = counts
                    part.about.class_entity_count_states = states
                    log.info(
                        "  [%s] %d patterns with edges in %s", source.name, len(part.patterns), graph_uri
                    )
                    self._save_dataset_outputs(
                        source, part, output_dir, miner.helper, "grouped_local_distribution"
                    )
        finally:
            for report_path in member_reports:
                shutil.copyfile(group_report, report_path)
        self._require_complete(miner)
        return list(graphs)
