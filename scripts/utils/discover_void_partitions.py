#!/usr/bin/env python3
"""Discover VoID partitions from SPARQL endpoints.

VoID (Vocabulary of Interlinked Datasets) is a standard for describing dataset metadata.
Some endpoints publish VoID descriptions that include class partitions and property statistics.
This script discovers those partitions and exports them as schemas.

Usage:
    python scripts/discover_void_partitions.py
    python scripts/discover_void_partitions.py --output-dir output/void
    python scripts/discover_void_partitions.py --filter "name contains uniprot"
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdfsolve.api import discover_void_source
from rdfsolve.models.source_model import SourcesRegistry


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Discover VoID partitions from SPARQL endpoints",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sources",
        type=Path,
        default=Path(__file__).parent.parent / "data" / "sources.yaml",
        help="Path to sources.yaml (default: data/sources.yaml)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/void"),
        help="Output directory for VoID schemas (default: output/void)",
    )
    parser.add_argument(
        "--filter",
        help="Filter sources by expression (e.g., 'name contains uniprot')",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Query timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )

    args = parser.parse_args()
    setup_logging(verbose=args.verbose)
    logger = logging.getLogger(__name__)

    # Load sources
    registry = SourcesRegistry.from_yaml(args.sources)

    # Filter to endpoints only
    sources = [s for s in registry.sources if s.endpoint and not s.endpoint_down]

    # Apply filter if provided
    if args.filter:
        # Simple contains filter for now
        if "contains" in args.filter:
            parts = args.filter.split("contains", 1)
            if len(parts) == 2:
                field = parts[0].strip()
                value = parts[1].strip().strip('"\'')
                sources = [
                    s for s in sources
                    if value.lower() in str(getattr(s, field, "")).lower()
                ]

    logger.info(f"Discovering VoID metadata from {len(sources)} endpoints...")

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    successes = []
    failures = []
    no_partitions = []

    for i, source in enumerate(sources, 1):
        name = source.name
        endpoint = source.endpoint

        logger.info(f"[{i}/{len(sources)}] {name}")
        logger.info(f"  Endpoint: {endpoint}")

        try:
            # Discover VoID metadata
            result = discover_void_source(
                name=name,
                endpoint=endpoint,
                output_dir=args.output_dir,
                entry=source.model_dump(),
            )

            partition_count = result.get("partition_count", 0)
            graphs_found = result.get("graphs_found", 0)

            if partition_count > 0:
                logger.info(f"  ✓ Found {partition_count} VoID partitions in {graphs_found} graphs")
                successes.append((name, partition_count))
            else:
                logger.info(f"  - No VoID partitions found (searched {graphs_found} graphs)")
                no_partitions.append(name)

        except Exception as e:
            error_msg = str(e)[:150]
            logger.error(f"  ✗ Failed: {error_msg}")
            failures.append((name, error_msg))

    # Summary
    logger.info("=" * 70)
    logger.info("VoID Discovery Complete")
    logger.info("=" * 70)
    logger.info(f"Endpoints with partitions: {len(successes)}")
    logger.info(f"Endpoints without partitions: {len(no_partitions)}")
    logger.info(f"Failed: {len(failures)}")

    if successes:
        logger.info("\nEndpoints with VoID partitions:")
        for name, count in sorted(successes, key=lambda x: x[1], reverse=True)[:20]:
            logger.info(f"  {name:30s}: {count} partitions")
        if len(successes) > 20:
            logger.info(f"  ... and {len(successes) - 20} more")

    if failures:
        logger.info("\nFailed endpoints:")
        for name, error in failures[:10]:
            logger.info(f"  {name:30s}: {error[:80]}")
        if len(failures) > 10:
            logger.info(f"  ... and {len(failures) - 10} more")

    logger.info(f"\nResults saved to: {args.output_dir}")

    return 0 if successes else 1


if __name__ == "__main__":
    sys.exit(main())
