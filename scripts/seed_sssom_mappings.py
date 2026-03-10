#!/usr/bin/env python3
"""Download and convert SSSOM mapping bundles into rdfsolve JSON-LD files.

Reads ``data/sssom_sources.yaml`` (or a custom YAML via ``--sources-yaml``),
downloads each archive, extracts every ``.sssom.tsv`` file, converts it to a
:class:`~rdfsolve.models.SsomMapping` JSON-LD, and writes the output to
``docker/mappings/sssom/`` (configurable via ``--output-dir``).

Usage
-----
Process all sources in the default YAML::

    python scripts/seed_sssom_mappings.py

Process only selected sources by name::

    python scripts/seed_sssom_mappings.py --name ols_mappings

Use a custom YAML and output directory::

    python scripts/seed_sssom_mappings.py \\
        --sources-yaml data/my_sssom_sources.yaml \\
        --output-dir /tmp/sssom_out/

Dry-run (list sources, do not download)::

    python scripts/seed_sssom_mappings.py --list

Each output file is named::

    {source_name}__{sssom_filename_stem}.jsonld

and is placed in the output directory, ready for ingestion by the rdfsolve
mapping pipeline (``seed_inferenced_mappings``).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCES_YAML = ROOT / "data" / "sssom_sources.yaml"
DEFAULT_OUTPUT_DIR = ROOT / "docker" / "mappings" / "sssom"


def _load_sources(yaml_path: Path) -> list[dict]:
    if not yaml_path.exists():
        print(f"ERROR: SSSOM sources YAML not found: {yaml_path}", file=sys.stderr)
        sys.exit(1)
    entries = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or []
    if not isinstance(entries, list):
        print(
            f"ERROR: {yaml_path} must be a YAML list of source entries.",
            file=sys.stderr,
        )
        sys.exit(1)
    return entries


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Download SSSOM mapping bundles and convert to rdfsolve JSON-LD. "
            "Sources are listed in data/sssom_sources.yaml."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--name",
        nargs="+",
        default=None,
        metavar="NAME",
        dest="names",
        help=(
            "Process only sources with these names (as defined in the YAML). "
            "Default: process all sources."
        ),
    )
    parser.add_argument(
        "--sources-yaml",
        default=str(DEFAULT_SOURCES_YAML),
        metavar="PATH",
        help=f"Path to SSSOM sources YAML (default: {DEFAULT_SOURCES_YAML})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        metavar="DIR",
        help=f"Output directory for class mappings (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--property-mappings-dir",
        default=None,
        metavar="DIR",
        help=(
            "Output directory for entries with type: property_mappings. "
            "Defaults to <output-dir>/../property_mappings/."
        ),
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available source names and exit (no download).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    yaml_path = Path(args.sources_yaml)
    entries = _load_sources(yaml_path)

    if args.list:
        print(f"Sources in {yaml_path}:")
        for entry in entries:
            name = entry.get("name", "<unnamed>")
            provider = entry.get("provider", "")
            url = entry.get("url", "")
            print(f"  {name:30s}  {provider:20s}  {url}")
        return

    # Add project root to sys.path
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))

    from rdfsolve.api import seed_sssom_mappings

    out_dir = Path(args.output_dir)
    prop_dir = (
        Path(args.property_mappings_dir)
        if args.property_mappings_dir
        else out_dir.parent / "property_mappings"
    )

    # Split entries by type and route to the appropriate output directory
    all_entries = _load_sources(yaml_path)
    if args.names:
        name_set = set(args.names)
        all_entries = [e for e in all_entries if e.get("name") in name_set]

    class_entries = [e for e in all_entries if e.get("type") != "property_mappings"]
    prop_entries  = [e for e in all_entries if e.get("type") == "property_mappings"]

    all_succeeded: list[str] = []
    all_failed:    list[dict] = []

    if class_entries:
        result = seed_sssom_mappings(
            sssom_sources_yaml=str(yaml_path),
            output_dir=str(out_dir),
            names=[e["name"] for e in class_entries],
        )
        all_succeeded.extend(result.get("succeeded", []))
        all_failed.extend(result.get("failed", []))

    if prop_entries:
        result = seed_sssom_mappings(
            sssom_sources_yaml=str(yaml_path),
            output_dir=str(prop_dir),
            names=[e["name"] for e in prop_entries],
        )
        all_succeeded.extend(result.get("succeeded", []))
        all_failed.extend(result.get("failed", []))

    result = {"succeeded": all_succeeded, "failed": all_failed, "skipped": []}

    print("\nResults:")
    for s in result["succeeded"]:
        print(f"  OK   {s}")
    for f in result["failed"]:
        file_id = f.get("file") or f.get("source", "?")
        print(f"  FAIL {file_id}: {f.get('error')}", file=sys.stderr)
    if result["skipped"]:
        print(f"  (skipped: {result['skipped']})")

    total_ok = len(result["succeeded"])
    total_fail = len(result["failed"])
    print(f"\n{total_ok} file(s) written, {total_fail} failure(s).")

    if result["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
