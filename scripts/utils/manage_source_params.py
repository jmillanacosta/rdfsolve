#!/usr/bin/env python3
"""Manage source parameters in sources.yaml.

This script allows bulk updates to mining parameters like delays, timeouts,
failure thresholds, and health check settings without manually editing YAML.

Usage Examples:

    # Set default delay for all public endpoints
    python scripts/manage_source_params.py --set-delay 3.0 --filter "endpoint contains sparql"

    # Reset failure counts for all sources
    python scripts/manage_source_params.py --reset-failures

    # Mark specific endpoint as down
    python scripts/manage_source_params.py --mark-down uniprot

    # Configure failure threshold (how many failures before marking down)
    python scripts/manage_source_params.py --set-failure-threshold 5

    # Set timeout for slow endpoints
    python scripts/manage_source_params.py --set-timeout 600 --filter "avg_response_time > 10"

    # Remove health check data
    python scripts/manage_source_params.py --clear-health-data

    # List sources matching filter
    python scripts/manage_source_params.py --list --filter "endpoint_status = down"
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def parse_filter(filter_str: str, source: dict) -> bool:
    """Evaluate a filter expression against a source dict.

    Supported operators:
    - contains: "endpoint contains sparql"
    - =, !=, >, <, >=, <=: "failure_count > 3"
    - startswith, endswith: "name startswith pubchem"

    Args:
        filter_str: Filter expression
        source: Source dict

    Returns:
        True if source matches filter
    """
    if not filter_str:
        return True

    parts = filter_str.split(maxsplit=2)
    if len(parts) < 3:
        # Try as simple field presence check
        if len(parts) == 1:
            return bool(source.get(parts[0]))
        return False

    field, op, value = parts

    # Get field value
    field_val = source.get(field, "")

    # Handle None
    if field_val is None:
        field_val = ""

    # Type conversions
    if value.lower() in ("true", "false"):
        value = value.lower() == "true"
        if isinstance(field_val, str):
            field_val = field_val.lower() == "true"

    # Try numeric comparison
    try:
        if op in (">", "<", ">=", "<="):
            field_val = float(field_val) if field_val not in (None, "") else 0.0
            value = float(value)
    except (ValueError, TypeError):
        pass

    # Evaluate
    if op == "contains":
        return value.lower() in str(field_val).lower()
    elif op == "startswith":
        return str(field_val).startswith(value)
    elif op == "endswith":
        return str(field_val).endswith(value)
    elif op == "=":
        return str(field_val) == str(value)
    elif op == "!=":
        return str(field_val) != str(value)
    elif op == ">":
        return field_val > value
    elif op == "<":
        return field_val < value
    elif op == ">=":
        return field_val >= value
    elif op == "<=":
        return field_val <= value

    return False


def main():
    parser = argparse.ArgumentParser(
        description="Manage source parameters in sources.yaml",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # I/O options
    parser.add_argument(
        "--sources",
        type=Path,
        default=Path(__file__).parent.parent / "data" / "sources.yaml",
        help="Path to sources.yaml (default: data/sources.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying file",
    )

    # Filter options
    parser.add_argument(
        "--filter",
        type=str,
        help='Filter sources (e.g., "endpoint contains sparql", "failure_count > 3")',
    )
    parser.add_argument(
        "--source",
        type=str,
        action="append",
        help="Specific source name(s) to update (can be repeated)",
    )

    # Query options
    parser.add_argument(
        "--list",
        action="store_true",
        help="List sources matching filter (no modifications)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics about sources",
    )

    # Parameter updates
    parser.add_argument(
        "--set-delay",
        type=float,
        metavar="SECONDS",
        help="Set inter-request delay (seconds)",
    )
    parser.add_argument(
        "--set-timeout",
        type=float,
        metavar="SECONDS",
        help="Set query timeout (seconds)",
    )
    parser.add_argument(
        "--set-status",
        choices=["up", "down", "timeout", "rate_limited", "unknown"],
        help="Set endpoint status",
    )
    parser.add_argument(
        "--mark-down",
        action="store_true",
        help="Mark endpoint as down (sets endpoint_down=true, failure_count=3)",
    )
    parser.add_argument(
        "--mark-up",
        action="store_true",
        help="Mark endpoint as up (resets endpoint_down, failure_count, status)",
    )

    # Batch operations
    parser.add_argument(
        "--reset-failures",
        action="store_true",
        help="Reset failure_count to 0 for all sources",
    )
    parser.add_argument(
        "--clear-health-data",
        action="store_true",
        help="Clear all health check data (status, timestamps, errors)",
    )
    parser.add_argument(
        "--set-failure-threshold",
        type=int,
        metavar="COUNT",
        help="Update failure_count threshold (conceptual - updates matching sources)",
    )

    args = parser.parse_args()

    # Load sources
    if not args.sources.exists():
        print(f"Error: {args.sources} not found")
        return 1

    with open(args.sources, encoding="utf-8") as fh:
        sources_data = yaml.safe_load(fh)

    if not isinstance(sources_data, list):
        print(f"Error: Expected list in {args.sources}")
        return 1

    # Build index
    sources_by_name = {s["name"]: s for s in sources_data}

    # Determine target sources
    if args.source:
        # Specific sources by name
        target_sources = []
        for name in args.source:
            if name in sources_by_name:
                target_sources.append(sources_by_name[name])
            else:
                print(f"Warning: Source '{name}' not found")
    elif args.filter:
        # Filter-based selection
        target_sources = [s for s in sources_data if parse_filter(args.filter, s)]
    else:
        # All sources
        target_sources = sources_data

    print(f"Selected {len(target_sources)} / {len(sources_data)} sources")

    # List mode
    if args.list:
        print("\nMatching sources:")
        for s in target_sources:
            status = s.get("endpoint_status", "unknown")
            failures = s.get("failure_count", 0)
            endpoint = s.get("endpoint", "none")
            print(f"  {s['name']:30s} {status:15s} failures={failures:2d} {endpoint}")
        return 0

    # Stats mode
    if args.stats:
        total = len(sources_data)
        remote = sum(1 for s in sources_data if s.get("endpoint"))
        local = sum(1 for s in sources_data if s.get("download_ttl") or s.get("local_provider"))

        status_counts = {}
        for s in sources_data:
            status = s.get("endpoint_status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        failure_counts = {}
        for s in sources_data:
            fc = s.get("failure_count", 0)
            failure_counts[fc] = failure_counts.get(fc, 0) + 1

        print("\n" + "=" * 60)
        print("SOURCES STATISTICS")
        print("=" * 60)
        print(f"Total sources:        {total}")
        print(f"  Remote endpoints:   {remote}")
        print(f"  Local sources:      {local}")
        print()
        print("Endpoint status:")
        for status in sorted(status_counts.keys()):
            print(f"  {status:15s}: {status_counts[status]:3d}")
        print()
        print("Failure counts:")
        for fc in sorted(failure_counts.keys()):
            print(f"  {fc} failures:      {failure_counts[fc]:3d} sources")
        print("=" * 60)
        return 0

    # Apply updates
    modified_count = 0
    changes = []

    for source in target_sources:
        source_changes = []

        # Set delay
        if args.set_delay is not None:
            old = source.get("delay")
            source["delay"] = args.set_delay
            source_changes.append(f"delay: {old} -> {args.set_delay}")

        # Set timeout
        if args.set_timeout is not None:
            old = source.get("timeout")
            source["timeout"] = args.set_timeout
            source_changes.append(f"timeout: {old} -> {args.set_timeout}")

        # Set status
        if args.set_status:
            old = source.get("endpoint_status", "unknown")
            source["endpoint_status"] = args.set_status
            source["last_checked"] = datetime.now(timezone.utc).isoformat()
            source_changes.append(f"endpoint_status: {old} -> {args.set_status}")

        # Mark down
        if args.mark_down:
            source["endpoint_down"] = True
            source["endpoint_status"] = "down"
            source["failure_count"] = 3
            source["last_checked"] = datetime.now(timezone.utc).isoformat()
            source_changes.append("marked as down")

        # Mark up
        if args.mark_up:
            source["endpoint_down"] = False
            source["endpoint_status"] = "up"
            source["failure_count"] = 0
            source["last_success"] = datetime.now(timezone.utc).isoformat()
            source["last_checked"] = datetime.now(timezone.utc).isoformat()
            source_changes.append("marked as up (reset failures)")

        # Reset failures
        if args.reset_failures:
            old = source.get("failure_count", 0)
            if old > 0:
                source["failure_count"] = 0
                source["endpoint_down"] = False
                source_changes.append(f"failure_count: {old} -> 0")

        # Clear health data
        if args.clear_health_data:
            source["endpoint_status"] = "unknown"
            source["last_checked"] = ""
            source["last_success"] = ""
            source["last_error"] = ""
            source["failure_count"] = 0
            source["endpoint_down"] = False
            source["avg_response_time"] = None
            source_changes.append("cleared health data")

        # Failure threshold (mark down sources exceeding threshold)
        if args.set_failure_threshold is not None:
            fc = source.get("failure_count", 0)
            if fc >= args.set_failure_threshold:
                source["endpoint_down"] = True
                source_changes.append(
                    f"marked down (failures={fc} >= {args.set_failure_threshold})"
                )

        if source_changes:
            modified_count += 1
            changes.append((source["name"], source_changes))

    # Show changes
    if changes:
        print(f"\n{modified_count} sources will be modified:")
        for name, source_changes in changes[:20]:
            print(f"\n  {name}:")
            for change in source_changes:
                print(f"    - {change}")
        if len(changes) > 20:
            print(f"\n  ... and {len(changes) - 20} more sources")

    # Save or dry-run
    if args.dry_run:
        print("\nDRY RUN - no changes written")
        return 0

    if modified_count == 0:
        print("\nNo changes to apply")
        return 0

    # Confirm
    if not args.filter and not args.source:
        response = input(f"\nApply changes to {modified_count} sources? [y/N]: ")
        if response.lower() not in ("y", "yes"):
            print("Cancelled")
            return 0

    # Write back
    with open(args.sources, "w", encoding="utf-8") as fh:
        yaml.dump(
            sources_data,
            fh,
            default_flow_style=False,
            sort_keys=False,
            width=200,
            allow_unicode=True,
        )

    print(f"\nUpdated {args.sources}")
    print(f"Modified {modified_count} sources")

    return 0


if __name__ == "__main__":
    sys.exit(main())
