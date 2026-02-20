#!/usr/bin/env python3
"""Run inference over all mapping files and write an inferenced JSON-LD.

Reads all ``*.jsonld`` files from ``docker/mappings/instance_matching/``
and ``docker/mappings/semra/``, applies SeMRA inference operations
(inversion by default, transitivity by default, generalisation optional),
deduplicates, and writes to ``docker/mappings/inferenced/``.

Usage
-----
Run with defaults (inversion + transitivity)::

    python scripts/seed_inferenced_mappings.py

Skip transitivity, enable generalisation::

    python scripts/seed_inferenced_mappings.py \\
        --no-transitivity --generalisation

Custom input/output directories::

    python scripts/seed_inferenced_mappings.py \\
        --input-dir docker/mappings \\
        --output-dir docker/mappings/inferenced \\
        --name my_inferred
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = ROOT / "docker" / "mappings"
DEFAULT_OUTPUT_DIR = ROOT / "docker" / "mappings" / "inferenced"


def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Infer new mappings from existing JSON-LD files.",
    )
    parser.add_argument(
        "--input-dir",
        default=str(DEFAULT_INPUT_DIR),
        metavar="DIR",
        help=(
            "Root mapping directory containing instance_matching/ "
            "and semra/ subdirs "
            f"(default: {DEFAULT_INPUT_DIR})"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        metavar="DIR",
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--name",
        default="inferenced_mappings",
        metavar="NAME",
        help="Output file stem (default: inferenced_mappings)",
    )
    parser.add_argument(
        "--no-inversion",
        action="store_true",
        default=False,
        help="Disable inversion inference (default: on).",
    )
    parser.add_argument(
        "--no-transitivity",
        action="store_true",
        default=False,
        help="Disable transitivity inference (default: on).",
    )
    parser.add_argument(
        "--generalisation",
        action="store_true",
        default=False,
        help="Enable generalisation inference (default: off).",
    )
    parser.add_argument(
        "--chain-cutoff",
        type=int,
        default=3,
        metavar="N",
        help="Max chain length for transitivity (default: 3).",
    )
    args = parser.parse_args()

    # Add project root to sys.path so rdfsolve is importable
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))

    from rdfsolve.api import seed_inferenced_mappings

    result = seed_inferenced_mappings(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        output_name=args.name,
        inversion=not args.no_inversion,
        transitivity=not args.no_transitivity,
        generalisation=args.generalisation,
        chain_cutoff=args.chain_cutoff,
    )

    if result["output_path"]:
        print(
            f"\nOK {result['output_edges']} edges written to "
            f"{result['output_path']}\n"
            f"  (from {result['input_edges']} input edges, "
            f"ops: {result['inference_types']})"
        )
    else:
        print("⚠ No input mapping files found.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
