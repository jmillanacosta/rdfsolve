#!/usr/bin/env python
"""Discover VoID descriptions from SPARQL endpoints.

This script queries endpoints for their published VoID (Vocabulary of
Interlinked Datasets) metadata and exports schema artifacts in multiple
formats: VoID Turtle (.ttl) and JSON-LD (.jsonld).
"""

import argparse
import logging
from pathlib import Path

from rdfsolve import discover_void_source, load_sources

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Discover VoID descriptions from SPARQL endpoints"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/void"),
        help="Output directory for VoID artifacts",
    )
    parser.add_argument(
        "--sources",
        type=Path,
        default=Path("data/sources.yaml"),
        help="Path to sources.yaml file",
    )
    parser.add_argument(
        "--source-names",
        nargs="+",
        help="Specific source names to process (default: all with endpoints)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger("rdfsolve").setLevel(logging.DEBUG)

    # Load sources
    log.info("Loading sources from %s", args.sources)
    sources_data = load_sources(args.sources)
    sources = sources_data.get("sources", {})

    # Filter sources with endpoints
    endpoint_sources = {
        name: src
        for name, src in sources.items()
        if src.get("endpoint")
    }

    if args.source_names:
        endpoint_sources = {
            name: src
            for name, src in endpoint_sources.items()
            if name in args.source_names
        }

    log.info("Found %d sources with endpoints", len(endpoint_sources))

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Discover VoID for each source
    success_count = 0
    fail_count = 0

    for name, source in endpoint_sources.items():
        endpoint = source["endpoint"]
        log.info("Discovering VoID for %s: %s", name, endpoint)

        try:
            result = discover_void_source(
                endpoint=endpoint,
                name=name,
                output_dir=args.output_dir,
                entry=source,
            )

            if result.get("partitions"):
                partitions_count = len(result["partitions"])
                files = result.get("files", {})
                log.info(
                    "  ✓ Found %d partitions for %s",
                    partitions_count,
                    name,
                )
                if files:
                    log.info("    Exported:")
                    for fmt, path in files.items():
                        log.info("      - %s: %s", fmt, Path(path).name)
                success_count += 1
            else:
                log.warning("  ! No VoID partitions found for %s", name)
                fail_count += 1

        except Exception as e:
            log.error("  ✗ Failed to discover VoID for %s: %s", name, e)
            fail_count += 1

    log.info("=" * 60)
    log.info("VoID Discovery Complete")
    log.info("=" * 60)
    log.info("Success: %d", success_count)
    log.info("Failed:  %d", fail_count)
    log.info("Output:  %s", args.output_dir)


if __name__ == "__main__":
    main()
