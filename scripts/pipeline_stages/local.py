"""Local operations for the pipeline command."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rdfsolve.qlever import QleverConfig, build_qleverfile
from rdfsolve.qlever.downloads import MARKER, needs_download, server_state, write_record
from rdfsolve.qlever.inputs import (
    expand_inputs,
    graph_input_directory,
    index_command,
    mapped_input_files,
    rdf_input_files,
)
from rdfsolve.schema_models.exporters.text import trim_descriptions as trim_export_text

from .base import PartialMiningError, Stage
from .config import Source

log = logging.getLogger(__name__)


# Download fields of files that hold triples without graphs.
FILES_WITHOUT_GRAPHS = frozenset(
    {"download_nt", "download_ttl", "download_owl", "download_rdf", "download_obo", "download_n3"}
)


def local_graph_scope(
    graph_uris: list[str] | None, download_fields: dict, graph_sources: dict
) -> list[str] | None:
    """Return the graph scope to mine a local index with.

    The scope of a registry entry describes the endpoint. An index built only from files
    without graphs holds no named graph, and is mined as a whole (None). The scope is kept for
    files that can hold graphs (N-Quads, archives), for files mapped to graphs, and when no
    file is known; the miner then reports a graph that the index does not hold.
    """
    if not graph_uris:
        return None
    if download_fields and not graph_sources and set(download_fields) <= FILES_WITHOUT_GRAPHS:
        return None
    return graph_uris


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
                self._set_aside_when_updated(workdir, source)
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

    def _set_aside_when_updated(self, workdir: Path, source: Source) -> None:
        """With --update-downloads, keep the folder of a source only when it has no update.

        The folder of a source with a changed file on the server, or with other URLs in the
        registry entry than were downloaded, is renamed (kept), and an empty folder is made, so
        that the source is downloaded and indexed again.
        """
        from rdfsolve.qlever.downloads import read_record, updated_urls

        urls = list(source.download_urls)
        if not self.config.update_downloads or self.config.no_download or not urls:
            return
        if not any(workdir.iterdir()):
            return
        record = read_record(workdir)
        built = max((p.stat().st_mtime for p in workdir.glob("*.meta-data.json")), default=0.0)
        if record is not None and list(record.get("urls", [])) != urls:
            reason = "the registry entry has other URLs than were downloaded"
        else:
            changed = updated_urls(urls, record, built_at=built)
            if not changed:
                log.info("  No update on the server for %s", source.name)
                return
            reason = f"{len(changed)} of {len(urls)} files changed on the server"
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        kept = workdir.with_name(f"{workdir.name}.before-update-{stamp}")
        workdir.rename(kept)
        workdir.mkdir()
        log.info("  %s: %s; downloaded and indexed again (old folder: %s)", source.name, reason, kept)

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
            has_inputs = all(rdf_input_files(directory) for directory in directories)
            urls = list(source.download_urls)
            if needs_download(workdir, urls, has_inputs=has_inputs):
                if not skip_download and not self.config.no_download:
                    get_data_cmd = config.get("data", "GET_DATA_CMD")
                    # The marker stays when the download does not end, so the next run
                    # downloads again and does not index a part of the files.
                    (workdir / MARKER).touch()
                    subprocess.run(["bash"], input=get_data_cmd, text=True, check=True, cwd=workdir)
                    # The server is asked about the files only in a run that asks for updates.
                    head = server_state if self.config.update_downloads else (lambda url: None)
                    write_record(workdir, urls, head)
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
        cmd = index_command(
            self.config.data_dir / "qlever.sif",
            self.config.data_dir,
            workdir,
            config.get("data", "NAME", fallback=source.name),
            settings_path,
            mapped,
            parallel=config.get("index", "PARALLEL_PARSING"),
            buffer=config.get(
                "index", "PARSER_BUFFER_SIZE", fallback=QleverConfig().parser_buffer_size
            ),
            memory=config.get("index", "STXXL_MEMORY", fallback="16GB"),
        )

        # QLever returns every integer type as xsd:int and a decimal as xsd:double; the numeric
        # datatypes of the source are counted from the input files, beside the index, while
        # it is built (rdfsolve.qlever.datatypes).
        import os
        from concurrent.futures import ProcessPoolExecutor

        from rdfsolve.qlever.datatypes import (
            CENSUS_FILE,
            count_literal_datatypes,
            input_format,
            merge_counts,
            write_census,
        )

        files = [(path, input_format(path)) for path, _ in mapped]
        census = ProcessPoolExecutor(max(1, min(len(files), (os.cpu_count() or 2) // 2)))
        log.info(f"    Indexing {len(mapped)} files...")
        try:
            counting = [census.submit(count_literal_datatypes, [item]) for item in files]
            subprocess.run(cmd, cwd=workdir, check=True)
            counts = merge_counts(future.result() for future in counting)
            write_census(workdir / CENSUS_FILE, counts, [path for path, _ in files])
        finally:
            census.shutdown(cancel_futures=True)
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
        previous = (
            self.config.resume_from / source.name / f"{source.name}{suffix}_report.checkpoint.jsonl"
            if self.config.resume_from
            else None
        )
        if previous and not previous.is_file():
            log.warning("  --resume-from: no checkpoint %s; %s is mined from the start", previous, source.name)
        scope = graph_uris if graph_uris is not None else source.graph_uris or None
        if graph_uris is None and scope:
            applied = local_graph_scope(scope, source.download_fields, source.graph_sources)
            if applied is None:
                log.info(
                    "  The files of %s have no graphs; the graph scope of its endpoint is not "
                    "applied and the whole index is mined", source.name,
                )
            scope = applied
        miner = self._local_miner(
            port, scope,
            report_path, type_context_graph_uris=source.type_context_graph_uris,
            resume_checkpoint=previous if previous and previous.is_file() else None,
        )
        miner.classes_as_data = source.classes_as_data
        schema = self._mine_schema(miner, source.name, output_dir,
                                   ontology_graph_uris=source.ontology_graph_uris or None)
        with self._output_phase(miner, report_path):
            self._save_dataset_outputs(
                source, schema, output_dir, miner.helper, mining_context,
                members=self._group_members(miner),
            )
        self._require_complete(miner)

    def _local_miner(self, port: int, graph_uris: list[str] | None, report_path: Path,
                     *, type_context_graph_uris: list[str] | None = None,
                     resume_checkpoint: Path | None = None):
        """Create a miner for a local QLever instance."""
        from rdfsolve import SchemaMiner

        endpoint = f"http://localhost:{port}"
        miner = SchemaMiner(
            endpoint_url=endpoint,
            graph_uris=graph_uris,
            type_context_graph_uris=type_context_graph_uris,
            timeout=self.config.timeout if self.config.timeout is not None else 600.0,
            # The run started this server itself: no wait between requests (the wait is
            # for public endpoints).
            delay=0.0,
            sparql_engine="qlever",
            chunk_size=self.config.chunk_size,
            class_batch_size=self.config.class_batch_size,
            class_chunk_size=self.config.class_chunk_size,
            enrich=self.config.enrich,
            examples_per_pattern=self.config.examples_per_pattern,
            max_response_bytes=self.config.max_response_bytes,
            report_path=str(report_path),
            resume_checkpoint=resume_checkpoint,
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
                ontology_group_before_mining=self.config.ontology_group_before_mining,
                ontology_hierarchy_files=self.config.ontology_hierarchy_files,
                extract_metadata=self.config.extract_metadata,
                dataset_name=name,
            )
            schema = result.data_schema
            self._restore_literal_datatypes(miner, schema, name)
            report_path = output_dir / f"{name}{suffix}_report.json"
            with self._output_phase(miner, report_path):
                schema_path = output_dir / f"{name}{suffix}_schema.json"
                schema_path.write_text(json.dumps(schema.to_dict(), indent=2), encoding="utf-8")
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
            self._restore_literal_datatypes(miner, schema, name)

        return schema

    def _restore_literal_datatypes(self, miner, schema, name: str) -> None:
        """Restore the numeric datatypes that the index folds, from the census of its sources."""
        from rdfsolve.qlever.datatypes import CENSUS_FILE, read_census, restore_datatypes

        census = self.config.data_dir / "qlever_workdirs" / name / CENSUS_FILE
        if census.is_file():
            patterns = [*schema.patterns, *(schema.structural_patterns or [])]
            record = {"state": "restored", "census": str(census)}
            record.update(restore_datatypes(patterns, read_census(census)))
        else:
            record = {
                "state": "not_restored",
                "reason": "no census of the source files",
                "rule": "QLever reports every integer type as xsd:int and a decimal as xsd:double",
            }
        if miner.last_report is not None:
            miner.last_report.config["literal_datatypes"] = record

    def _save_dataset_outputs(
        self,
        source: Source,
        schema,
        output_dir: Path,
        helper,
        mining_context: str,
        members: dict[str, list[str]] | None = None,
    ) -> None:
        """Write one dataset's schema exports and its separate evidence files."""
        output_dir.mkdir(parents=True, exist_ok=True)
        suffix = self.config.output_suffix
        schema = self._without_service_data(schema)
        self._save_schema_outputs(
            schema, output_dir, source.name, suffix, helper=helper, members=members
        )
        from rdfsolve.ontology.artifacts import archive_local_ontology_files

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
        import hashlib

        from rdfsolve.qlever.lifecycle import image_for_index, index_build, start_server

        image = image_for_index(self.config.data_dir, workdir, name)
        engine = {
            "image": str(image.resolve()),
            "image_sha256": hashlib.sha256(image.read_bytes()).hexdigest(),
            "index_build": index_build(workdir, name),
        }
        output_dir = self.config.output_dir / name
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / f"{name}{self.config.output_suffix}_engine.json").write_text(
            json.dumps(engine, indent=2)
        )
        log.info(f"    QLever image {image} for index build {engine['index_build']}")
        process = start_server(
            image,
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
