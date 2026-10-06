"""Remote operations for the pipeline command."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from rdfsolve.graph_parts import has_graph_parts
from rdfsolve.schema_models.exporters.text import trim_descriptions as trim_export_text

from .base import PartialMiningError, Stage, rdf_only
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

    def _void_strategy(self, source: Source, graphs: list[str] | None) -> Any:
        """Return a strategy that reads the source's schema from the VoID its endpoint publishes.

        None when the endpoint publishes no full VoID, when the VoID does not describe the
        source's graphs, or when --no-void-first is given: the source is then mined. The VoID of
        an endpoint is fetched once; the sources of one endpoint are mined one after another.
        """
        if not self.config.void_first:
            return None
        if source.classes_as_data or source.membership_properties:
            # VoID partitions by rdf:type: records that are classes (Rhea, SwissLipids) or that a
            # category property places (Monarch) are described by the miner, not by the VoID.
            # Such settings of single graphs (graph_settings) keep the VoID for the source; those
            # graphs are mined for their own schemas (_save_graph_parts).
            log.info("[%s] Its classes are not read from rdf:type alone; mined", source.name)
            return None
        from rdfsolve.mining.void_strategy import VoidStrategy, find_published_void, void_for_source
        from rdfsolve.sparql_helper import SparqlHelper

        cache: dict[str, Any] = self.__dict__.setdefault("_published_void", {})
        if source.endpoint not in cache:
            entry = source.model_dump()
            with SparqlHelper.from_source_entry(entry, timeout=300) as helper:
                cache[source.endpoint] = find_published_void(helper)
        published = cache[source.endpoint]
        if published is None:
            return None
        scoped = void_for_source(published, graphs or source.graph_uris or None)
        if scoped is None:
            log.info(
                "[%s] The VoID of %s does not describe its graphs; mined",
                source.name,
                published.graph,
            )
            return None
        log.info(
            "[%s] Schema from the VoID in %s (issued %s)",
            source.name,
            published.graph,
            published.issued,
        )
        return VoidStrategy(scoped, void_graph=published.graph, issued=published.issued)

    def _remote_miner(
        self,
        source: Source,
        *,
        strategy: Any,
        use_graph_store: bool,
        graph_store_dir: Path,
        graph_uris: list[str] | None,
        type_context_graph_uris: list[str],
        delay: float,
        classes_as_data: bool,
        membership_properties: list[str],
        report_path: Path,
        resume_checkpoint: Path | None = None,
    ) -> Any:
        """Create the miner of a source's endpoint with the run's settings."""
        from rdfsolve import SchemaMiner

        return SchemaMiner(
            strategy=strategy,
            endpoint_url=source.endpoint,
            get_graphs_from_store=use_graph_store,
            graph_store_url=self.config.graph_store_urls.get(source.name),
            graph_store_dir=graph_store_dir,
            graph_store_max_bytes=self.config.max_response_bytes,
            graph_uris=graph_uris,
            type_context_graph_uris=type_context_graph_uris,
            timeout=(
                self.config.timeout
                if self.config.timeout is not None
                else source.timeout
                if source.timeout is not None
                else 300.0
            ),
            delay=delay,
            sparql_engine=source.sparql_engine,
            sparql_strategy=source.sparql_strategy,
            chunk_size=self.config.chunk_size,
            class_batch_size=self.config.class_batch_size,
            class_chunk_size=self.config.class_chunk_size,
            enrich=self.config.enrich,
            examples_per_pattern=self.config.examples_per_pattern,
            max_response_bytes=self.config.max_response_bytes,
            excluded_graph_prefixes=self.config.exclude_graph_prefixes,
            classes_as_data=classes_as_data,
            membership_properties=membership_properties,
            report_path=str(report_path),
            resume_checkpoint=resume_checkpoint,
        )

    def _save_graph_parts(
        self, source: Source, schema: Any, miner: Any, strategy: Any, delay: float
    ) -> None:
        """Write one schema per data graph of a source, the remote view of each graph.

        A graph whose settings differ from the source's (graph_settings) is mined on its own
        with them. With the schema read from the endpoint's VoID, every other graph's part is
        the VoID scoped to that graph (no query); the links whose other end another graph
        describes keep the class of that end. A graph that the VoID does not describe has no
        part. Without VoID, the mined schema is cut by the graph of each edge, as in the local
        channel (entities are not counted per graph on the endpoint).
        """
        from rdfsolve.graph_parts import (
            GRAPHS_DIR,
            graph_part_dir,
            graph_parts,
            write_graph_parts_index,
        )
        from rdfsolve.mining.edge_graph_split import split_by_edge_graph, unattributed_patterns
        from rdfsolve.mining.void_strategy import VoidStrategy, void_for_source

        suffix = self.config.output_suffix
        published = self.__dict__.get("_published_void", {}).get(source.endpoint)
        from_void = isinstance(strategy, VoidStrategy) and published is not None
        missing = [] if from_void else unattributed_patterns(schema)
        rows: list[dict[str, Any]] = []
        incomplete: list[str] = []
        for part in graph_parts(source, self.config.registry):
            part_dir = graph_part_dir(self.config.output_dir, source.name, part.name)
            piece = None
            if part.own_settings:
                log.info("[%s] %s: mined on its own with its graph settings", part.name, part.graph)
                part_dir.mkdir(parents=True, exist_ok=True)
                piece, part_miner = self._mine_graph_part(source, part, part_dir, delay)
                derivation = "mined_with_graph_settings"
                report = part_miner.last_report
                if report is None or report.completion_state != "complete":
                    incomplete.append(part.name)
            elif from_void:
                scoped = void_for_source(published, [part.graph])
                derivation = "void_scoped_to_graph" if scoped is not None else "not_in_void"
                if scoped is not None:
                    piece = _void_part(scoped, schema, part.graph, part.name, source.endpoint)
            else:
                piece = split_by_edge_graph(
                    schema, part.graph, part.name, declared_classes=miner.declared_classes
                )
                derivation = "edge_graph_split"
            row = {**part.record(), "derivation": derivation}
            if piece is not None:
                part_dir.mkdir(parents=True, exist_ok=True)
                self._save_schema_outputs(piece, part_dir, part.name, suffix)
                row["patterns"] = len(piece.patterns)
                row["schema"] = f"{GRAPHS_DIR}/{part.name}/{part.name}{suffix}_schema.json"
            rows.append(row)
        write_graph_parts_index(
            self.config.output_dir, source.name, suffix, "remote", rows,
            unattributed_patterns=len(missing),
        )
        if miner.last_report is not None:
            miner.last_report.config["graph_parts"] = {
                "parts": [row["name"] for row in rows if "schema" in row],
                "unattributed_patterns": len(missing),
            }
        if incomplete:
            raise RuntimeError(f"Per-graph mining incomplete: {incomplete}")

    def _mine_graph_part(self, source: Source, part: Any, part_dir: Path, delay: float) -> Any:
        """Mine one graph of a source's endpoint on its own, with the settings of that graph."""
        suffix = self.config.output_suffix
        context = [g for g in source.graph_uris if g != part.graph] + source.type_context_graph_uris
        miner = self._remote_miner(
            source,
            strategy=None,
            use_graph_store=False,
            graph_store_dir=part_dir / "downloads",
            graph_uris=[part.graph],
            type_context_graph_uris=list(dict.fromkeys(context)),
            delay=delay,
            classes_as_data=part.classes_as_data,
            membership_properties=list(part.membership_properties),
            report_path=part_dir / f"{part.name}{suffix}_report.json",
        )
        if self.config.ontology_as_data:
            from rdfsolve.mining import mine_with_ontology

            schema = mine_with_ontology(
                miner,
                ontology_scope=self.config.ontology_scope,
                ontology_graph_uris=source.ontology_graph_uris or None,
                ontology_as_data=True,
                ontology_term_budget=self.config.ontology_term_budget,
                ontology_group_before_mining=self.config.ontology_group_before_mining,
                ontology_hierarchy_files=self.config.ontology_hierarchy_files,
                dataset_name=part.name,
            ).data_schema
        else:
            schema = miner.mine(dataset_name=part.name)
        return self._without_service_data(schema), miner

    def _mine_single_source(self, source: Source) -> dict[str, Any]:
        """Mine a single source. Returns dict with status and data."""
        from datetime import timezone

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
            strategy = None if use_graph_store else self._void_strategy(source, empirical_graphs)
            miner = self._remote_miner(
                source,
                strategy=strategy,
                use_graph_store=use_graph_store,
                graph_store_dir=source_output_dir / "downloads",
                graph_uris=empirical_graphs or None,
                type_context_graph_uris=source.type_context_graph_uris,
                delay=polite_delay,
                classes_as_data=source.classes_as_data,
                membership_properties=source.membership_properties,
                report_path=report_path,
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
                            rdf_only(ontology_graph, "ontology", miner.last_report)
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
                            rdf_only(metadata_graph, "metadata", miner.last_report)
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
                    report=miner.last_report,
                )
                self._save_ontology_discovery(
                    schema,
                    source_output_dir,
                    source.name,
                    suffix,
                    helper=miner.helper,
                    mining_context="remote_endpoint",
                    published_void=self.__dict__.get("_published_void", {}).get(source.endpoint),
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
                if has_graph_parts(source) and not use_graph_store:
                    self._save_graph_parts(source, schema, miner, strategy, polite_delay)
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


def _void_part(void: Any, schema: Any, graph: str, name: str, endpoint: str) -> Any:
    """Return the part of a VoID-first schema that the VoID scoped to one graph describes.

    The counts of a graph's partitions and linksets are triples in that graph.
    """
    from rdfsolve.mining.edge_graph_split import graph_part_about
    from rdfsolve.schema_models.readers.void import void_graph_to_minedschema

    read = void_graph_to_minedschema(void, endpoint=endpoint, report_untyped=False)
    patterns = [
        p.model_copy(
            update={
                "count_semantics": "triples_in_graph",
                "graphs": {graph: p.count} if p.count else None,
            }
        )
        for p in read.patterns
    ]
    about = graph_part_about(
        schema.about,
        name,
        [graph],
        patterns,
        class_entity_counts=read.about.class_entity_counts,
    )
    return schema.model_copy(
        update={
            "patterns": patterns,
            "raw_patterns": None,
            "term_patterns": None,
            "structural_patterns": None,
            "collections": None,
            "about": about,
            "navigation": None,
            "enrichment": read.enrichment,
            "source_metadata": read.source_metadata,
            "class_extensions": None,
            "restriction_patterns": None,
            "shapes": None,
        }
    )
