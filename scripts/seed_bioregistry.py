#!/usr/bin/env python3
"""Enrich sources.yaml with Bioregistry metadata in-place.

Reads ``data/sources.yaml`` (or a custom path), calls
:func:`rdfsolve.sources.enrich_source_with_bioregistry` for each entry, and
writes the resolved ``bioregistry_*`` fields back into the YAML file,
preserving all existing keys and entry order.

Usage::

    python scripts/seed_bioregistry.py                    # default data/sources.yaml
    python scripts/seed_bioregistry.py -s data/sources_all.yaml
    python scripts/seed_bioregistry.py --dry-run          # preview, no file written
    python scripts/seed_bioregistry.py --names drugbank chebi
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

_BIOREGISTRY_KEYS = [
    "bioregistry_prefix",
    "bioregistry_name",
    "bioregistry_description",
    "bioregistry_homepage",
    "bioregistry_license",
    "bioregistry_domain",
    "bioregistry_uri_prefix",
    "bioregistry_uri_prefixes",
    "bioregistry_logo",
    "bioregistry_keywords",
    "bioregistry_synonyms",
    "bioregistry_publications",
    "bioregistry_extra_providers",
    "bioregistry_mappings",
]


def _enrich_node(node: dict, dry_run: bool) -> str | None:
    """Resolve and embed Bioregistry metadata into a raw YAML node dict."""
    from rdfsolve.sources import SourceEntry, enrich_source_with_bioregistry

    # Build a minimal SourceEntry from the raw node so the resolver can use
    # 'name', 'void_iri', 'endpoint', etc.
    entry: SourceEntry = {}  # type: ignore[assignment]
    for key in ("name", "void_iri", "endpoint", "graph_uris"):
        if key in node:
            entry[key] = node[key]  # type: ignore[literal-required]

    prefix = enrich_source_with_bioregistry(entry)

    if prefix is None:
        return None

    # Copy resolved bioregistry_* fields back into the raw YAML node,
    # replacing any stale values.
    for key in _BIOREGISTRY_KEYS:
        if key in entry:
            node[key] = entry[key]  # type: ignore[literal-required]
        else:
            # Remove stale key if it was previously set but no longer resolved.
            node.pop(key, None)

    return prefix


def main() -> None:
    """Parse CLI arguments and run enrichment."""
    parser = argparse.ArgumentParser(
        description="Seed sources.yaml with Bioregistry metadata.",
    )
    parser.add_argument(
        "-s",
        "--sources",
        default=str(ROOT / "data" / "sources.yaml"),
        help="Path to sources YAML file (default: data/sources.yaml).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved prefixes but do not write the file.",
    )
    parser.add_argument(
        "--names",
        nargs="+",
        metavar="NAME",
        help="Only process entries whose 'name' matches one of these values.",
    )
    args = parser.parse_args()

    sources_path = Path(args.sources)
    if not sources_path.exists():
        print(f"ERROR: file not found: {sources_path}", file=sys.stderr)
        sys.exit(1)

    with open(sources_path, encoding="utf-8") as fh:
        nodes: list[dict] = yaml.safe_load(fh)

    if not isinstance(nodes, list):
        print("ERROR: expected a YAML list of source mappings.", file=sys.stderr)
        sys.exit(1)

    name_filter: set[str] | None = set(args.names) if args.names else None

    resolved = 0
    skipped = 0
    for node in nodes:
        name = node.get("name", "")
        if name_filter and name not in name_filter:
            continue

        prefix = _enrich_node(node, dry_run=args.dry_run)
        if prefix:
            resolved += 1
            print(f"  {name!r:40s} → {prefix}")
        else:
            skipped += 1

    print(f"\nResolved: {resolved}  |  No match: {skipped}")

    if args.dry_run:
        print("(dry-run: file not written)")
        return

    with open(sources_path, "w", encoding="utf-8") as fh:
        yaml.dump(
            nodes,
            fh,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )
    print(f"Written: {sources_path}")


if __name__ == "__main__":
    main()
