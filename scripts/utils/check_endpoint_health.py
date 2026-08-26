#!/usr/bin/env python3
"""Check health status of all SPARQL endpoints in sources.yaml.

This script:
1. Checks each endpoint with a simple ASK query
2. Records response times and errors
3. Updates sources.yaml with health status
4. Generates a health report

Usage:
    python scripts/check_endpoint_health.py
    python scripts/check_endpoint_health.py --no-save  # Don't update YAML
    python scripts/check_endpoint_health.py --sources custom_sources.yaml
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdfsolve.endpoint_health import health_check_all_endpoints


def setup_logging(verbose: bool = False):
    """Setup basic logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Check health of all SPARQL endpoints",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sources",
        type=Path,
        default=Path(__file__).parent.parent / "data" / "sources.yaml",
        help="Path to sources.yaml (default: data/sources.yaml)",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save health status back to sources.yaml",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="Save JSON report to this file",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(verbose=args.verbose)

    # Run health checks
    results = health_check_all_endpoints(
        sources_yaml=args.sources,
        save=not args.no_save,
    )

    # Generate report
    report = {
        "total_endpoints": len(results),
        "timestamp": results[next(iter(results))].timestamp if results else "",
        "status_summary": {},
        "endpoints": {},
    }

    for name, health in results.items():
        status = health.status
        report["status_summary"][status] = report["status_summary"].get(status, 0) + 1
        report["endpoints"][name] = {
            "endpoint_url": health.endpoint_url,
            "status": health.status,
            "response_time": health.response_time,
            "error_message": health.error_message,
        }

    # Save report if requested
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nReport saved to {args.report}")

    # Print summary
    print("\n" + "=" * 60)
    print("ENDPOINT HEALTH CHECK SUMMARY")
    print("=" * 60)
    for status, count in sorted(report["status_summary"].items()):
        print(f"{status:15s}: {count:3d} endpoints")
    print("=" * 60)

    # Print failures
    failures = [
        (name, health)
        for name, health in results.items()
        if health.status != "up"
    ]
    if failures:
        print(f"\n{len(failures)} endpoints with issues:\n")
        for name, health in failures[:20]:  # Show first 20
            print(f"  {name:30s} {health.status:15s} {health.error_message[:60]}")
        if len(failures) > 20:
            print(f"\n  ... and {len(failures) - 20} more (see report for full list)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
