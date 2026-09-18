"""Base operations for the pipeline command."""

from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Any

from .config import PipelineConfig

log = logging.getLogger(__name__)


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
            state = (
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
            raise RuntimeError(f"Mining incomplete: {'; '.join(reasons) or 'see the source report'}")

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
                void_graph = schema.to_void_graph(
                    base_url=self.config.void_base_url,
                    trim_descriptions=self.config.trim_descriptions,
                )
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
