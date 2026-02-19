#!/usr/bin/env python3
"""Probe bioregistry resources and write instance mapping JSON-LD files.

Writes ``{prefix}_instance_mapping.jsonld`` to ``docker/schemas/`` for
every supplied prefix, using all endpoints in ``data/sources.csv``.

Usage
-----
Probe a single resource against all endpoints::

    python scripts/seed_instance_mappings.py --prefixes ensembl

Probe several resources, restrict to two datasets::

    python scripts/seed_instance_mappings.py \\
        --prefixes ensembl uniprot chebi \\
        --datasets aopwikirdf wikipathways

Re-probe even if the output file already exists::

    python scripts/seed_instance_mappings.py \\
        --prefixes ensembl --no-skip-existing

Write to a custom directory::

    python scripts/seed_instance_mappings.py \\
        --prefixes ensembl --output-dir my_mappings/

Use a different mapping predicate::

    python scripts/seed_instance_mappings.py \\
        --prefixes ensembl \\
        --predicate http://www.w3.org/2004/02/skos/core#exactMatch
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "data" / "sources.csv"
DEFAULT_OUTPUT_DIR = ROOT / "docker" / "schemas"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed instance mapping JSON-LD files to docker/schemas/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--prefixes", nargs="+", required=True, metavar="PREFIX",
        help="One or more bioregistry prefixes to probe (e.g. ensembl uniprot).",
    )
    parser.add_argument(
        "--sources-csv",
        default=str(DEFAULT_CSV),
        help=f"Sources CSV path (default: {DEFAULT_CSV})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--predicate",
        default="http://www.w3.org/2004/02/skos/core#narrowMatch",
        help="Mapping predicate URI (default: skos:narrowMatch)",
    )
    parser.add_argument(
        "--datasets", nargs="*", default=None, metavar="DATASET",
        help="Restrict probing to these dataset names.",
    )
    parser.add_argument(
        "--timeout", type=float, default=60.0,
        help="SPARQL request timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--no-skip-existing", action="store_true", default=False,
        help="Re-probe even if the output file already exists.",
    )
    args = parser.parse_args()

    # Ensure rdfsolve is importable when run from the repo root
    sys.path.insert(0, str(ROOT / "src"))

    from rdfsolve.api import seed_instance_mappings

    result = seed_instance_mappings(
        prefixes=args.prefixes,
        sources_csv=args.sources_csv,
        output_dir=args.output_dir,
        predicate=args.predicate,
        dataset_names=args.datasets,
        timeout=args.timeout,
        skip_existing=not args.no_skip_existing,
    )

    print(f"\nDone — {len(result['succeeded'])} succeeded, "
          f"{len(result['failed'])} failed")

    for prefix in result["succeeded"]:
        outfile = Path(args.output_dir) / f"{prefix}_instance_mapping.jsonld"
        print(f"  ✓  {prefix}  →  {outfile}")

    for item in result["failed"]:
        print(f"  ✗  {item['prefix']}: {item['error']}", file=sys.stderr)

    if result["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
