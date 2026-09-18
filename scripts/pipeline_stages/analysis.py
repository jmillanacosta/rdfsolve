"""Analysis operations for the pipeline command."""

from __future__ import annotations

import json
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

        from rdfsolve.analysis.io import load_schemas

        schemas = list(load_schemas(self.config.output_dir).items())
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

    def _execute(self):
        from networkx import node_link_data

        from rdfsolve.analysis.connectivity import build_connectivity, compare_schemas
        from rdfsolve.analysis.io import load_schemas, read_class_mappings

        output = self.config.output_dir
        schemas = load_schemas(output)
        if not schemas:
            raise ValueError(
                "No canonical *_schema.json files found in the selected output directory"
            )
        mappings, imports = [], {}
        for path in sorted((output / "mappings" / "enriched").glob("*.sssom.tsv")):
            edges, report = read_class_mappings(path, schemas)
            mappings.extend(edges)
            imports[str(path.relative_to(output))] = report
        graph = build_connectivity(schemas, class_mappings=mappings)
        overlaps = compare_schemas(schemas)
        stats = {
            "total_schemas": len(schemas),
            "class_nodes": len(graph),
            "schema_edges": sum(d["kind"] == "schema" for _, _, d in graph.edges(data=True)),
            "explicit_mapping_edges": len(mappings),
            "mapping_imports": imports,
            "overlapping_pairs": sum(
                p["shared_classes"] > 0 or p["shared_predicates"] > 0 for p in overlaps
            ),
        }
        for name, data in (
            ("paper_statistics.json", stats),
            ("schema_overlaps.json", overlaps),
            ("class_connectivity.json", node_link_data(graph)),
        ):
            (output / name).write_text(json.dumps(data, indent=2))
        return stats
