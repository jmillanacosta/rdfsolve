#!/usr/bin/env python3
r"""Probe bioregistry resources and write instance mapping JSON-LD files.

Writes ``{prefix}_instance_mapping.jsonld`` to ``docker/schemas/`` for
every supplied prefix, using all endpoints in ``data/sources.csv``.

When an output file already exists the new probe results are **merged**
into it: new graph nodes are appended, existing nodes get new predicate
targets added, and ``uri_formats_queried`` / ``pattern_count`` are
updated.

Usage
-----
Probe a single resource against all endpoints::

    python scripts/seed_instance_mappings.py --prefixes ensembl

Probe several resources, restrict to two datasets (always merges)::

    python scripts/seed_instance_mappings.py \\
        --prefixes ensembl uniprot chebi \\
        --datasets aopwikirdf wikipathways

Re-probe all datasets even if the output file already exists::

    python scripts/seed_instance_mappings.py \\
        --prefixes ensembl

Skip prefixes whose output file already exists without re-probing::

    python scripts/seed_instance_mappings.py \\
        --prefixes ensembl --skip-existing

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
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "data" / "sources.csv"
DEFAULT_OUTPUT_DIR = ROOT / "docker" / "mappings" / "instance_matching"


def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )
    # Also set the rdfsolve logger specifically
    logging.getLogger("rdfsolve").setLevel(logging.DEBUG)

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
        help=(
            "Restrict probing to these dataset names. "
            "When set, the existing file is always re-probed for the "
            "given datasets and new results are merged in."
        ),
    )
    parser.add_argument(
        "--timeout", type=float, default=60.0,
        help="SPARQL request timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", default=False,
        help=(
            "Skip prefixes whose output file already exists without "
            "re-probing. By default, existing files are always "
            "re-probed and new results are merged in."
        ),
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
        skip_existing=args.skip_existing,
    )


    for prefix in result["succeeded"]:
        Path(args.output_dir) / f"{prefix}_instance_mapping.jsonld"

    for _item in result["failed"]:
        pass

    if result["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
