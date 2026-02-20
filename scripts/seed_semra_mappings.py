#!/usr/bin/env python3
"""Import SeMRA external mappings and write one JSON-LD per (source, prefix).

Writes ``{source}_{prefix}.jsonld`` to ``docker/mappings/semra/`` for
each unique subject prefix found in the downloaded mappings.

Usage
-----
Import from a single source::

    python scripts/seed_semra_mappings.py --sources fplx

Import from multiple sources, keep only chebi and uniprot::

    python scripts/seed_semra_mappings.py \\
        --sources biomappingspositive gilda \\
        --prefixes chebi uniprot

Import ALL registered sources::

    python scripts/seed_semra_mappings.py --sources all

Import ALL Wikidata mappings (217 prefixes, needs network + time)::

    python scripts/seed_semra_mappings.py --sources wikidata

Import Wikidata for specific prefixes only::

    python scripts/seed_semra_mappings.py --sources wikidata --prefixes chebi ncbitaxon

Write to a custom directory::

    python scripts/seed_semra_mappings.py \\
        --sources fplx \\
        --output-dir my_mappings/semra/

Registered sources (use their short synonym or full key):
  fplx, pubchemmesh, ncitchebi, ncithgnc, ncitgo, ncituniprot,
  biomappingspositive, gilda, clo, wikidata, omimgene,
  cbms2019, compath, rdfsolveinstance  (and "all" for all of them)

Note: ``wikidata`` fetches one file per bioregistry prefix (217 available).
  Use ``--prefixes`` to restrict which ones are downloaded.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = ROOT / "docker" / "mappings" / "semra"

# Canonical short synonyms for every registered source.
# Keep in sync with semra_source.py / SeMRA's SOURCE_RESOLVER.
_ALL_SOURCES = [
    "fplx",
    "pubchemmesh",
    "ncitchebi",
    "ncithgnc",
    "ncitgo",
    "ncituniprot",
    "biomappingspositive",
    "gilda",
    "clo",
    "wikidata",
    "omimgene",
    "cbms2019",
    "compath",
    "rdfsolveinstance",
]


def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Import SeMRA mappings and write JSON-LD files. "
            "Pass --sources all to import every registered source."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        required=True,
        metavar="SOURCE",
        help=(
            "SeMRA source key(s) to import, or 'all' to import every "
            "registered source. Short synonyms accepted (e.g. 'fplx', "
            "'ncitchebi'). Available: " + ", ".join(_ALL_SOURCES)
        ),
    )
    parser.add_argument(
        "--prefixes",
        nargs="*",
        default=None,
        metavar="PREFIX",
        help=(
            "Keep only these bioregistry prefixes. "
            "Default: keep all."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        metavar="DIR",
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    # Add project root to sys.path so rdfsolve is importable
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))

    # Expand "all" to the full source list
    sources: list[str] = []
    for s in args.sources:
        if s.lower() == "all":
            sources.extend(_ALL_SOURCES)
        else:
            sources.append(s)
    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for s in sources:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    sources = deduped

    from rdfsolve.api import seed_semra_mappings

    result = seed_semra_mappings(
        sources=sources,
        keep_prefixes=args.prefixes,
        output_dir=args.output_dir,
    )

    print("\nResults:")
    for s in result["succeeded"]:
        print(f"  OK {s}")
    for f in result["failed"]:
        src = f.get("source", "?")
        pfx = f.get("prefix")
        loc = f"{src}/{pfx}" if pfx else src
        print(f"  FAIL {loc}: {f.get('error')}", file=sys.stderr)
    if result["skipped"]:
        print(f"  (skipped: {result['skipped']})")

    if result["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
