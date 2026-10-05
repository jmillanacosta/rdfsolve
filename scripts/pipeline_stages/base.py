"""Base operations for the pipeline command."""

from __future__ import annotations

import functools
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


def rdf_only(graph, what: str, report=None):
    """Return *graph* without the triples whose terms are not RDF IRIs, which no RDF syntax
    writes (rdfsolve.schema_models.iri_quality), and log how many were left out.

    With a mining *report*, the terms left out and their triples are recorded as data-quality
    findings (config iri_findings, graphs: *what*).
    """
    from rdfsolve.schema_models.iri_quality import graph_findings, rdf_terms_only

    terms = graph_findings(graph)
    before = len(graph)
    rdf_terms_only(graph)
    if len(graph) < before:
        log.warning(
            "%s: %d triples with %d terms that are not RDF IRIs left out (report: iri_findings)",
            what,
            before - len(graph),
            len(terms),
        )
        if report is not None:
            found = report.config.setdefault("iri_findings", {})
            found.setdefault("graphs", {})[what] = {
                "triples_left_out": before - len(graph),
                "terms": [{"iri": iri, "triples": n} for iri, n in terms.items()],
            }
    return graph


def _logged_step(step: str):
    """Log when an output step starts and ends, so that a long step is seen in the job log."""

    def wrap(method):
        @functools.wraps(method)
        def run(self, *args, **kwargs):
            started = time.monotonic()
            log.info("Output step %s started", step)
            try:
                return method(self, *args, **kwargs)
            finally:
                log.info("Output step %s finished in %.1f s", step, time.monotonic() - started)

        return run

    return wrap


class PartialMiningError(RuntimeError):
    """Saved patterns have incomplete evidence."""


def restriction_scope(about: Any) -> list[str] | None:
    """Return the graphs in which restrictions are mined: the data and the ontology graphs.

    None (the whole dataset) when the schema has no graph scope.
    """
    if not about.graph_uris:
        return None
    return list(dict.fromkeys([*about.graph_uris, *(about.ontology_graph_uris or [])]))


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

    @staticmethod
    def _group_members(miner) -> dict[str, list[str]]:
        """Return the member terms of each group of ontology terms in the report of a miner."""
        config = getattr(getattr(miner, "last_report", None), "config", None)
        grouping = config.get("ontology_term_grouping") if isinstance(config, dict) else None
        members = grouping.get("representative_members") if isinstance(grouping, dict) else None
        return dict(members) if isinstance(members, dict) else {}

    @contextmanager
    def _output_phase(self, miner, report_path):
        """Retain output failures in the mining report."""
        from rdfsolve.mining.report_tracking import ReportCollector

        report = miner.last_report
        collector = ReportCollector(report, report_path, fresh=False)
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


    @_logged_step("ontology discovery")
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
        published_void: Any | None = None,
    ) -> None:
        """Save ontology discovery/acquisition evidence for one mined snapshot.

        Remote endpoint graph discovery and local distribution file discovery are
        deliberately separate evidence channels.  ``download_owl``-derived file
        candidates must only be passed by local/grouped mining stages; they say
        nothing about which graphs are loaded or exposed by a remote endpoint.

        When the endpoint publishes a VoID whose service description lists its named
        graphs, those names are inspected and the endpoint is not scanned for them; the
        discovery file records the VoID graph as their source.
        """
        local_candidates = list(local_ontology_file_candidates or [])
        if not self.config.discover_ontology_graphs and not local_candidates:
            return

        graph_candidates = []
        if self.config.discover_ontology_graphs:
            from rdfsolve.ontology.discovery import discover_remote_ontology_graphs
            from rdfsolve.schema_models.readers.void import service_description_graph_names

            named = (
                service_description_graph_names(published_void.void)
                if published_void is not None
                else []
            )
            known: dict[str, Any] = {}
            if named:
                evidence = f"{published_void.graph} (issued {published_void.issued})"
                log.info(
                    "[%s] Ontology discovery: %d named graphs from the service description "
                    "in %s; the endpoint is not scanned for graphs",
                    name,
                    len(named),
                    evidence,
                )
                known = {
                    "graph_uris": named,
                    "graph_names_source": "service_description",
                    "graph_names_evidence": evidence,
                }
            result = discover_remote_ontology_graphs(
                helper,
                observed_classes=schema.get_classes(),
                observed_properties=schema.get_properties(),
                max_graphs=self.config.ontology_discovery_max_graphs,
                **known,
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
        from rdfsolve.ontology.sources import build_ontology_acquisition_plan
        from rdfsolve.ontology.usage import observed_terms_from_patterns

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

    @_logged_step("declared artifacts")
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

    @_logged_step("property usage evidence")
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

    def _without_service_data(self, schema: Any) -> Any:
        """Return the schema without engine and service data when the run asks for it.

        The removed patterns stay in the record of the cleaning (about.cleaned).
        """
        if not self.config.clean_service_data:
            return schema
        from rdfsolve.schema_models._constants import (
            SUGGESTED_SERVICE_GRAPHS,
            SUGGESTED_SERVICE_NAMESPACES,
        )

        cleaned = schema.clean_schema(
            namespaces=SUGGESTED_SERVICE_NAMESPACES, graph_uris=SUGGESTED_SERVICE_GRAPHS
        )
        log.info(
            "Engine and service data: %d patterns removed", cleaned.about.cleaned["patterns_removed"]
        )
        return cleaned

    @_logged_step("schema outputs")
    def _save_schema_outputs(
        self,
        schema: Any,
        output_dir: Path,
        name: str,
        suffix: str,
        helper=None,
        members: dict[str, list[str]] | None = None,
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
        if self.config.navigation_hops and helper is not None:
            from rdfsolve.mining.navigation import find_tested_paths

            # Only paths that instances of the data follow are written (owner, 2026-09-30).
            log.info("[%s] Navigation: testing paths (budget %s s)", name, self.config.navigation_budget)
            schema.navigation = find_tested_paths(
                schema,
                helper,
                max_hops=self.config.navigation_hops,
                budget_s=self.config.navigation_budget,
                members=members,
            )

        if self.config.restriction_patterns and helper is not None:
            from rdfsolve.mining.restrictions import mine_restriction_patterns

            log.info("[%s] Restriction patterns: mining", name)
            schema.restriction_patterns = mine_restriction_patterns(
                helper, graph_uris=restriction_scope(schema.about)
            )
            log.info(
                "[%s] Restriction patterns: %d (%s)",
                name,
                len(schema.restriction_patterns.patterns),
                schema.restriction_patterns.state,
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
                trim = self.config.trim_descriptions
                # The class shapes, and the shapes of the tested paths in a file of their own.
                path.write_text(
                    schema.to_shacl(trim_descriptions=trim, paths="without"), encoding="utf-8"
                )
                if schema.navigation is not None and schema.navigation.paths:
                    paths_file = output_dir / f"{name}{suffix}_paths.shacl.ttl"
                    paths_file.write_text(
                        schema.to_shacl(trim_descriptions=trim, paths="only"), encoding="utf-8"
                    )
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
