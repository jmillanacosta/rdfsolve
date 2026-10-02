"""Remote operations for the pipeline command."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from rdfsolve.schema_models.exporters.text import trim_descriptions as trim_export_text

from .base import PartialMiningError, Stage
from .config import Source

log = logging.getLogger(__name__)


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

        host_groups: dict[str, list[Source]] = {}
        for source in sources:
            if not source.endpoint:
                continue
            host = urlparse(source.endpoint).hostname or source.endpoint
            if host not in host_groups:
                host_groups[host] = []
            host_groups[host].append(source)

        log.info(f"Grouped into {len(host_groups)} hosts for concurrent mining")

        from threading import Lock

        results: dict[str, list[Any]] = {
            "mined": [], "matched_local": [], "partial": [], "failed": [], "skipped": []
        }
        results_lock = Lock()

        def mine_host_sources(host: str, host_sources: list[Source]) -> None:
            """Mine all sources on a single host sequentially."""
            for source in host_sources:
                result = self._mine_single_source(source)
                with results_lock:
                    if result["status"] == "mined":
                        results["mined"].append(result["data"])
                    elif result["status"] == "matched_local":
                        results["matched_local"].append(result["data"])
                    elif result["status"] == "failed":
                        results["failed"].append(result["data"])
                    elif result["status"] == "partial":
                        results["partial"].append(result["data"])
                    else:
                        results["skipped"].append(result["data"])

        if not host_groups:
            return {"mined": [], "failed": [], "skipped": ["No remote sources selected"]}

        max_workers = min(max(1, self.config.parallelism), len(host_groups))
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

        log.info(
            f"Mining complete: {len(results['mined'])} succeeded, "
            f"{len(results['failed'])} failed, {len(results['skipped'])} skipped"
        )

        return results

    def _matches_local(
        self, source: Source, miner: Any, graph_uris: list[str], output_dir: Path, suffix: str
    ) -> bool:
        """Check the endpoint against the local record of the source; True when it is equal.

        The check is written as <name><suffix>_endpoint_match.json. Without a local record, or
        without --local-records, the endpoint is mined as before.
        """
        if self.config.local_records is None:
            return False
        record = self.config.local_records / source.name / f"{source.name}_local_schema.json"
        if not record.is_file():
            return False
        from rdfsolve.mining.endpoint_match import check_endpoint_matches
        from rdfsolve.schema_models.core import MinedSchema

        local = MinedSchema.from_dict(json.loads(record.read_text(encoding="utf-8")))
        match = check_endpoint_matches(local, miner.helper, graph_uris=graph_uris or None)
        path = output_dir / f"{source.name}{suffix}_endpoint_match.json"
        path.write_text(match.model_dump_json(indent=2), encoding="utf-8")
        log.info(f"[{source.name}] Endpoint against the local record: {match.state}")
        return match.state == "equal"

    def _mine_single_source(self, source: Source) -> dict[str, Any]:
        """Mine a single source. Returns dict with status and data."""
        from datetime import timezone

        from rdfsolve import SchemaMiner
        from rdfsolve.endpoint_health import (
            check_endpoint_health,
            get_polite_delay,
            update_endpoint_status,
        )

        log.info(f"[{source.name}] Starting...")

        if not source.endpoint:
            return self._record_skip(source, "No endpoint configured")

        use_graph_store = (
            self.config.get_graphs_from_store and source.name in self.config.graph_store_urls
        )

        if source.endpoint_down and source.failure_count >= 3 and not use_graph_store:
            log.warning(f"[{source.name}] Skipping: endpoint marked as down")
            return self._record_skip(source, "Endpoint marked as down")

        needs_health_check = True
        if source.last_checked:
            try:
                last_check = datetime.fromisoformat(source.last_checked)
                age = (datetime.now(timezone.utc) - last_check).total_seconds()
                if age < 3600:
                    needs_health_check = False
            except ValueError:
                pass

        if needs_health_check and not use_graph_store:
            health = check_endpoint_health(source.endpoint, timeout=30)
            update_endpoint_status(source, health)
            if health.status != "up":
                log.warning(f"[{source.name}] Skipping: endpoint is {health.status}")
                return self._record_skip(source, f"Endpoint health: {health.status}")

        output_dir = self.config.output_dir
        source_output_dir = output_dir / source.name
        source_output_dir.mkdir(parents=True, exist_ok=True)
        suffix = self.config.output_suffix
        schema_path = source_output_dir / f"{source.name}{suffix}_schema.json"

        if self.config.skip_completed and schema_path.exists():
            log.info(f"[{source.name}] Skipping: output already exists")
            return {"status": "skipped", "data": source.name}

        polite_delay = get_polite_delay(source)
        try:
            report_path = source_output_dir / f"{source.name}{suffix}_report.json"
            from rdfsolve.evidence.declared_sources import empirical_graph_scope

            empirical_graphs = empirical_graph_scope(source)
            # The class batches of an earlier run are reused, as in a local run.
            previous = (
                self.config.resume_from / source.name / f"{source.name}{suffix}_report.checkpoint.jsonl"
                if self.config.resume_from
                else None
            )
            if previous and not previous.is_file():
                log.warning("  --resume-from: no checkpoint %s; %s is mined from the start", previous, source.name)
            miner = SchemaMiner(
                endpoint_url=source.endpoint,
                get_graphs_from_store=use_graph_store,
                graph_store_url=self.config.graph_store_urls.get(source.name),
                graph_store_dir=source_output_dir / "downloads",
                graph_store_max_bytes=self.config.max_response_bytes,
                graph_uris=empirical_graphs or None,
                type_context_graph_uris=source.type_context_graph_uris,
                timeout=(
                    self.config.timeout
                    if self.config.timeout is not None
                    else source.timeout
                    if source.timeout is not None
                    else 300.0
                ),
                delay=polite_delay,
                sparql_engine=source.sparql_engine,
                sparql_strategy=source.sparql_strategy,
                chunk_size=self.config.chunk_size,
                class_batch_size=self.config.class_batch_size,
                class_chunk_size=self.config.class_chunk_size,
                enrich=self.config.enrich,
                examples_per_pattern=self.config.examples_per_pattern,
                max_response_bytes=self.config.max_response_bytes,
                excluded_graph_prefixes=self.config.exclude_graph_prefixes,
                report_path=str(report_path),
                resume_checkpoint=previous if previous and previous.is_file() else None,
            )
            if self._matches_local(source, miner, empirical_graphs, source_output_dir, suffix):
                miner.close()
                return {"status": "matched_local", "data": source.name}

            if self.config.extract_ontology or self.config.extract_metadata or self.config.ontology_as_data:
                from rdfsolve.mining import mine_with_ontology

                result = mine_with_ontology(
                    miner,
                    extract_ontology=self.config.extract_ontology,
                    ontology_scope=self.config.ontology_scope,
                    ontology_graph_uris=source.ontology_graph_uris or None,
                    ontology_as_data=self.config.ontology_as_data,
                    ontology_term_budget=self.config.ontology_term_budget,
                    ontology_group_before_mining=self.config.ontology_group_before_mining,
                    ontology_hierarchy_files=self.config.ontology_hierarchy_files,
                    extract_metadata=self.config.extract_metadata,
                    dataset_name=source.name,
                )
                schema = result.data_schema
                if result.ontology:
                    ontology_path = source_output_dir / f"{source.name}{suffix}_ontology.ttl"
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
                        raise RuntimeError(
                            f"[{source.name}] Could not generate ontology.ttl: {e}"
                        ) from e
                if result.metadata:
                    metadata_path = source_output_dir / f"{source.name}{suffix}_metadata.ttl"
                    try:
                        metadata_graph = trim_export_text(
                            result.metadata, self.config.trim_descriptions
                        ).to_rdf_graph()
                        if metadata_graph:
                            meta_ttl = metadata_graph.serialize(format="turtle")
                            metadata_path.write_text(meta_ttl, encoding="utf-8")
                    except Exception as e:
                        raise RuntimeError(
                            f"[{source.name}] Could not generate metadata.ttl: {e}"
                        ) from e
            else:
                schema = miner.mine(dataset_name=source.name)

            if not use_graph_store:
                source.endpoint_status = "up"
                source.last_success = datetime.now(timezone.utc).isoformat()
                source.failure_count = 0
                source.endpoint_down = False

            schema = self._without_service_data(schema)
            with self._output_phase(miner, report_path):
                self._save_schema_outputs(
                    schema,
                    source_output_dir,
                    source.name,
                    suffix,
                    helper=miner.helper,
                    members=self._group_members(miner),
                )
                self._save_ontology_discovery(
                    schema,
                    source_output_dir,
                    source.name,
                    suffix,
                    helper=miner.helper,
                    mining_context="remote_endpoint",
                )
                self._save_declared_artifacts(
                    source,
                    source_output_dir,
                    source.name,
                    suffix,
                    helper=miner.helper,
                    access_context="remote_endpoint",
                )
                self._save_property_usage_evidence(
                    schema, source_output_dir, source.name, suffix, helper=miner.helper
                )
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
                return {
                    "status": "mined",
                    "data": {"name": source.name, "endpoint": source.endpoint},
                }

        except Exception as e:
            if isinstance(e, PartialMiningError):
                log.warning("[%s] -> PARTIAL: %s", source.name, e)
                return {"status": "partial", "data": {"name": source.name, "error": str(e)}}
            if not use_graph_store:
                source.failure_count += 1
                source.last_error = str(e)[:500]
                if source.failure_count >= 3:
                    source.endpoint_down = True

            log.error(f"[{source.name}] -> FAILED: {e}")
            return {"status": "failed", "data": {"name": source.name, "error": str(e)}}
