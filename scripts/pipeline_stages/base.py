"""Base operations for the pipeline command."""

from __future__ import annotations

import json
import logging
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import PipelineConfig

log = logging.getLogger(__name__)


class PartialMiningError(RuntimeError):
    """Saved patterns have incomplete evidence."""


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
            produced = any(
                self.results.get(key)
                for key in ("mined", "groups_mined", "indexed_individually", "enriched_mappings")
            )
            skipped = bool(self.results.get("skipped"))
            state = "partial" if self.results.get("partial") else (
                ("partial" if produced else "failed")
                if failed
                else ("partial" if skipped and produced else "skipped" if skipped else "complete")
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
            log.info(
                f"Stage {self.name}: {self.results.get('state', 'interrupted')} in {elapsed:.1f}s"
            )

        return self.results

    def _execute(self) -> dict[str, Any]:
        raise NotImplementedError

    def _record_skip(self, source, reason):
        """Write the reason for a source excluded from this attempt."""
        output = self.config.output_dir / source.name
        output.mkdir(parents=True, exist_ok=True)
        path = output / f"{source.name}{self.config.output_suffix}_report.json"
        if not path.exists():
            path.write_text(json.dumps({
                "dataset_name": source.name, "endpoint_url": source.endpoint,
                "graph_uris": source.graph_uris, "completion_state": "skipped",
                "reason": reason, "finished_at": datetime.now(timezone.utc).isoformat(),
            }, indent=2))
        return {"status": "skipped", "data": {"name": source.name, "reason": reason}}

    @contextmanager
    def _output_phase(self, miner, report_path):
        """Retain output failures in the mining report."""
        from rdfsolve.mining.report_tracking import ReportCollector

        report = miner.last_report
        collector = ReportCollector(report, report_path)
        report.finished_at = None
        phase = collector.start_phase("pipeline-outputs")
        collector.flush()
        try:
            yield
        except Exception as error:
            collector.finish_phase(phase, error=str(error))
            raise PartialMiningError(f"Pipeline outputs incomplete: {error}") from error
        else:
            collector.finish_phase(phase)
        finally:
            report.finished_at = datetime.now(timezone.utc).isoformat()
            collector.flush()

    @staticmethod
    def _require_complete(miner: Any) -> None:
        report = miner.last_report
        if report is None:
            raise RuntimeError("Mining incomplete: no mining report")
        if report.completion_state != "complete":
            reasons = [report.abort_reason] if report.abort_reason else []
            if report.dropped_invalid_uris:
                reasons.append(f"{report.dropped_invalid_uris} malformed IRIs dropped")
            if report.query_failures:
                reasons.append(f"{len(report.query_failures)} query failures")
            unfinished = [p.name for p in report.phases if p.error or not p.finished_at]
            if unfinished:
                reasons.append(f"unfinished phases: {unfinished}")
            error = PartialMiningError if report.completion_state == "partial" else RuntimeError
            raise error(f"Mining incomplete: {'; '.join(reasons) or 'see the source report'}")


    def _save_ontology_discovery(
        self,
        schema: Any,
        output_dir: Path,
        name: str,
        suffix: str,
        *,
        helper: Any,
        mining_context: str = "unknown",
        local_ontology_file_candidates: list[Any] | None = None,
    ) -> None:
        """Save ontology discovery/acquisition evidence for one mined snapshot.

        Remote endpoint graph discovery and local distribution file discovery are
        deliberately separate evidence channels.  ``download_owl``-derived file
        candidates must only be passed by local/grouped mining stages; they say
        nothing about which graphs are loaded or exposed by a remote endpoint.
        """
        local_candidates = list(local_ontology_file_candidates or [])
        if not self.config.discover_ontology_graphs and not local_candidates:
            return

        graph_candidates = []
        if self.config.discover_ontology_graphs:
            from rdfsolve.mining.ontology_discovery import discover_remote_ontology_graphs

            result = discover_remote_ontology_graphs(
                helper,
                observed_classes=schema.get_classes(),
                observed_properties=schema.get_properties(),
                max_graphs=self.config.ontology_discovery_max_graphs,
            )
            graph_candidates = result.candidates
            path = output_dir / f"{name}{suffix}_ontology_discovery.json"
            path.write_text(result.model_dump_json(indent=2), encoding="utf-8")

            schema.about.ontology_graph_uris = sorted(
                {
                    candidate.graph_uri
                    for candidate in result.candidates
                    if candidate.graph_uri != "DEFAULT"
                }
            )

        # Build a usage-scoped acquisition plan.  Provider graph evidence is
        # attached only to endpoint/index discovery; OWL-formatted files from a
        # local source bundle are retained as local file candidates and are only
        # promoted after parsing + empirical overlap.
        from rdfsolve.evidence.ontology import observed_terms_from_patterns
        from rdfsolve.evidence.ontology_acquisition import build_ontology_acquisition_plan

        observed = observed_terms_from_patterns(schema.patterns)
        provider_endpoint = (
            getattr(helper, "endpoint_url", None)
            if mining_context == "remote_endpoint"
            else None
        )
        acquisition = build_ontology_acquisition_plan(
            name,
            observed,
            graph_candidates=graph_candidates,
            endpoint_url=provider_endpoint,
            local_ontology_file_candidates=local_candidates,
            mining_context=mining_context,
        )
        acquisition_path = output_dir / f"{name}{suffix}_ontology_acquisition.json"
        acquisition_path.write_text(acquisition.model_dump_json(indent=2), encoding="utf-8")

    def _save_declared_artifacts(
        self,
        source: Any,
        output_dir: Path,
        name: str,
        suffix: str,
        *,
        helper: Any | None,
        access_context: str,
    ) -> None:
        """Archive configured provider RDF separately from empirical evidence."""
        if not self.config.collect_declared_artifacts:
            return
        from rdfsolve.evidence.declared_sources import harvest_configured_declared_artifacts

        bundle = harvest_configured_declared_artifacts(
            source=source,
            dataset_id=name,
            output_dir=output_dir,
            access_context=access_context,
            helper=helper,
            base_dir=self.config.repo_dir or Path('.'),
            max_download_bytes=self.config.max_response_bytes,
        )
        if bundle.artifacts or bundle.evidence or bundle.errors:
            path = output_dir / f"{name}{suffix}_declared_artifacts.json"
            path.write_text(bundle.model_dump_json(indent=2), encoding="utf-8")

    def _save_property_usage_evidence(
        self,
        schema: Any,
        output_dir: Path,
        name: str,
        suffix: str,
        *,
        helper: Any,
    ) -> None:
        """Write class/property subject-support evidence as a separate artifact."""
        if not self.config.collect_property_usage_evidence:
            return
        from rdfsolve.evidence.observed import collect_property_usage_evidence

        classes = sorted({pattern.subject_class for pattern in schema.patterns})
        report_path = output_dir / f"{name}{suffix}_report.json"
        report = json.loads(report_path.read_text()) if report_path.is_file() else {}
        evidence = collect_property_usage_evidence(
            dataset_id=name,
            classes=classes,
            class_entity_counts=schema.about.class_entity_counts,
            class_entity_count_states=schema.about.class_entity_count_states,
            helper=helper,
            graph_uris=schema.about.graph_uris,
            batch_size=min(max(1, self.config.class_batch_size), 10),
            chunk_size=self.config.class_chunk_size or self.config.chunk_size,
            collect_node_kinds=self.config.collect_property_value_profiles,
            collect_datatypes=self.config.collect_property_value_profiles,
            collect_histograms=self.config.collect_property_value_histograms,
            shared_extensions=(report.get("config") or {}).get("shared_extensions"),
        )
        path = output_dir / f"{name}{suffix}_property_usage.json"
        path.write_text(evidence.model_dump_json(indent=2), encoding="utf-8")

    def _save_schema_outputs(
        self,
        schema: Any,
        output_dir: Path,
        name: str,
        suffix: str,
        helper=None,
    ) -> None:
        """Save schema in requested output formats.

        Args:
            schema: MinedSchema object to save
            output_dir: Directory to save outputs
            name: Source name
            suffix: Output file suffix
        """
        path = output_dir / f"{name}{suffix}_schema.json"
        path.write_text(json.dumps(schema.to_dict(), indent=2), encoding="utf-8")
        formats = self.config.output_formats
        if self.config.trim_descriptions is not None:
            log.warning(
                "[%s] Export descriptions are truncated to %s characters; text is lossy",
                name,
                self.config.trim_descriptions,
            )
        if self.config.navigation_hops:
            from rdfsolve.mining.navigation import discover_paths_with_fallback

            schema.navigation = discover_paths_with_fallback(
                schema,
                max_hops=self.config.navigation_hops,
                min_hops=min(self.config.navigation_min_hops, self.config.navigation_hops),
                max_paths_per_length=self.config.navigation_limit,
                helper=helper,
                probe_limit=self.config.navigation_probes,
            )

        path = output_dir / f"{name}{suffix}_schema.json"
        path.write_text(
            json.dumps(schema.to_dict(trim_descriptions=self.config.trim_descriptions), indent=2),
            encoding="utf-8",
        )

        if "json-ld" in formats:
            path = output_dir / f"{name}{suffix}_schema.jsonld"
            path.write_text(
                json.dumps(
                    schema.to_jsonld(trim_descriptions=self.config.trim_descriptions), indent=2
                ),
                encoding="utf-8",
            )

        if "void" in formats:
            path = output_dir / f"{name}{suffix}_void.ttl"
            try:
                void_graph = schema.to_void_graph(trim_descriptions=self.config.trim_descriptions)
                if void_graph:
                    void_ttl = void_graph.serialize(format="turtle")
                    path.write_text(void_ttl, encoding="utf-8")
            except Exception as e:
                raise RuntimeError(f"[{name}] Could not generate VoID: {e}") from e

        if "shacl" in formats:
            path = output_dir / f"{name}{suffix}_shacl.ttl"
            try:
                shacl_ttl = schema.to_shacl(trim_descriptions=self.config.trim_descriptions)
                path.write_text(shacl_ttl, encoding="utf-8")
            except Exception as e:
                raise RuntimeError(f"[{name}] Could not generate SHACL: {e}") from e

        if "pydantic" in formats:
            path = output_dir / f"{name}{suffix}_schema.py"
            try:
                pydantic_code = schema.to_pydantic(
                    schema_name=name, trim_descriptions=self.config.trim_descriptions
                )
                path.write_text(pydantic_code, encoding="utf-8")
            except Exception as e:
                raise RuntimeError(f"[{name}] Could not generate Pydantic: {e}") from e
