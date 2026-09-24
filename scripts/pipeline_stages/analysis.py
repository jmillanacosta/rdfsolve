"""Analysis operations for the pipeline command."""

from __future__ import annotations

import logging
from typing import Any

from .base import Stage

log = logging.getLogger(__name__)


class SSSOMSeedingStage(Stage):
    """Keep relevant external SSSOM assertions with their original provenance."""

    name = "sssom_seeding"

    def _execute(self) -> dict[str, Any]:
        from rdfsolve.mappings.enrichment import enrich_external_sssom_sources

        if not self.config.sssom_sources_file.exists():
            log.warning(f"SSSOM sources file not found: {self.config.sssom_sources_file}")
            return {"sources": 0, "enriched_mappings": 0}

        log.info(f"Loading SSSOM sources from {self.config.sssom_sources_file}")

        from rdfsolve.analysis.io import iter_extractions
        from rdfsolve.release.build import build_release_manifest, write_release_manifest

        write_release_manifest(build_release_manifest(self.config.output_dir), self.config.output_dir)

        schemas = [(dataset, schema) for dataset, _, schema in iter_extractions(self.config.output_dir)
                   if schema is not None]
        from rdfsolve.config import mint

        dataset_void_uris = {name: mint("dataset", name) for name, _ in schemas}
        if not schemas:
            raise ValueError("No canonical schema snapshots found; run mining first")

        log.info(f"Loaded {len(schemas)} schemas for class indexing")

        results = enrich_external_sssom_sources(
            sssom_sources_file=self.config.sssom_sources_file,
            schemas=schemas,
            dataset_void_uris=dataset_void_uris,
            output_dir=self.config.output_dir,
            creator_id="https://orcid.org/0000-0001-5608-781X",
            creator_label="Javier Millan Acosta",
        )

        total_enriched = sum(v for v in results.values() if v > 0)
        errors = sum(1 for v in results.values() if v < 0)

        log.info(
            f"Enriched {total_enriched} mappings from {len(results)} sources ({errors} errors)"
        )

        return {
            "sources": len(results),
            "enriched_mappings": total_enriched,
            "failed": errors,
            "details": results,
        }


class AnalysisStage(Stage):
    """Compare canonical schemas and preserve mapping provenance."""

    name = "analysis"

    def _execute(self) -> dict[str, Any]:
        from rdfsolve.analysis.release import analyze_release, write_release_analysis
        from rdfsolve.release.build import build_release_manifest, write_release_manifest

        output = self.config.output_dir
        manifest = build_release_manifest(output)
        write_release_manifest(manifest, output)
        from rdfsolve.mining.types import METADATA_RECORD_TYPES

        result = analyze_release(output, excluded_subject_classes=METADATA_RECORD_TYPES)
        write_release_analysis(result, output)
        write_release_manifest(
            build_release_manifest(output, release_id=manifest.release_id, issued=manifest.issued),
            output,
        )
        return result["paper_statistics"]
