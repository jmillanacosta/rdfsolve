#!/usr/bin/env python3
"""Enrich and elongate mined schemas, then merge both into one schema per dataset."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from rdfsolve.analysis.io import load_schemas
from rdfsolve.mining.enrichment import query_enrichment
from rdfsolve.mining.navigation import discover_paths_with_fallback
from rdfsolve.schema_models import MinedSchema
from rdfsolve.sparql_helper import SparqlHelper

log = logging.getLogger("enrich_schemas")


def already_enriched(schema: MinedSchema) -> bool:
    """Report whether a stored schema carries queried annotations."""
    enrichment = schema.enrichment
    return enrichment.state != "skipped" and enrichment.query_count > 0


def already_elongated(schema: MinedSchema, max_hops: int) -> bool:
    """Report whether stored routes already reach the requested hop bound."""
    return schema.navigation is not None and schema.navigation.max_hops >= max_hops


def enrich(schema: MinedSchema, args) -> str:
    """Query definitions and examples for one schema. Return what happened."""
    endpoint = args.endpoint or schema.about.endpoint
    if not endpoint:
        return "no endpoint recorded"
    with SparqlHelper(
        endpoint,
        timeout=args.timeout,
        inter_request_delay=args.delay,
        max_response_bytes=args.max_response_mb * 1024 * 1024,
    ) as helper:
        schema.enrichment = query_enrichment(
            schema,
            helper,
            schema.about.graph_uris,
            examples_per_pattern=args.examples_per_pattern,
            delay=args.delay,
        )
    return schema.enrichment.state


def write(schema: MinedSchema, path: Path) -> None:
    """Write one canonical schema snapshot."""
    path.write_text(json.dumps(schema.to_dict(), indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="Directory holding *_schema.json files")
    parser.add_argument("--output-dir", type=Path, required=True, help="New output directory")
    parser.add_argument("--max-hops", type=int, default=5, help="Hop bound to attempt first")
    parser.add_argument("--min-hops", type=int, default=3, help="Lowest hop bound to accept")
    parser.add_argument("--max-paths-per-length", type=int, default=100)
    parser.add_argument("--examples-per-pattern", type=int, default=1)
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between queries")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--max-response-mb", type=int, default=64)
    parser.add_argument("--endpoint", help="Query this endpoint instead of the recorded one")
    parser.add_argument("--sources", nargs="+", help="Only process these dataset names")
    parser.add_argument("--redo-enrichment", action="store_true", help="Re-query stored annotations")
    parser.add_argument("--skip-enrichment", action="store_true", help="Only compose routes")
    parser.add_argument("--extraction-mode", choices=["remote", "local", "grouped", "unknown"])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")

    schemas = load_schemas(args.run_dir, extraction_mode=args.extraction_mode)
    if args.sources:
        unknown = sorted(set(args.sources) - set(schemas))
        if unknown:
            raise ValueError(f"Unknown dataset names: {unknown}")
        schemas = {name: schemas[name] for name in args.sources}
    if not schemas:
        raise FileNotFoundError(f"No mined schemas under {args.run_dir}")
    args.output_dir.mkdir(parents=True, exist_ok=False)

    report: dict[str, dict] = {}
    for name, schema in sorted(schemas.items()):
        target = args.output_dir / name
        target.mkdir(parents=True)
        entry: dict[str, object] = {"patterns": len(schema.patterns)}

        enriched = schema.model_copy(deep=True)
        if args.skip_enrichment:
            entry["enrichment"] = "skipped by request"
        elif already_enriched(schema) and not args.redo_enrichment:
            entry["enrichment"] = f"kept {schema.enrichment.state}"
        else:
            try:
                entry["enrichment"] = enrich(enriched, args)
            except Exception as error:  # noqa: BLE001 - one failure must not stop the run
                entry["enrichment"] = f"failed: {type(error).__name__}: {error}"
                log.warning("[%s] enrichment failed: %s", name, error)
        entry["definitions"] = len(enriched.enrichment.definitions)
        entry["labels"] = len(enriched.enrichment.labels)
        write(enriched, target / f"{name}_schema_enriched.json")

        routed = schema.model_copy(deep=True)
        if already_elongated(schema, args.max_hops):
            entry["navigation"] = f"kept {schema.navigation.max_hops} hops"
        else:
            routed.navigation = discover_paths_with_fallback(
                routed,
                max_hops=args.max_hops,
                min_hops=args.min_hops,
                max_paths_per_length=args.max_paths_per_length,
            )
            deepest = max((len(route.steps) for route in routed.navigation.paths), default=0)
            entry["navigation"] = f"{deepest} hops composed, bound {routed.navigation.max_hops}"
        entry["walk_counts"] = routed.navigation.walk_counts if routed.navigation else {}
        write(routed, target / f"{name}_schema_paths.json")

        full = schema.model_copy(deep=True)
        full.enrichment = enriched.enrichment
        full.navigation = routed.navigation
        write(full, target / f"{name}_schema_full.json")
        (target / f"{name}_shapes_full.ttl").write_text(full.to_shacl())
        report[name] = entry
        log.info("[%s] %s", name, entry)

    (args.output_dir / "enrich_report.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
