#!/usr/bin/env python3
"""Mine JSON-LD schemas from data/sources.csv and save to docker/schemas/.

Usage:
    python scripts/seed_schemas.py                    # mine all sources
    python scripts/seed_schemas.py --qlever            # mine QLever sources only
    python scripts/seed_schemas.py --all-sources       # mine both CSVs
    python scripts/seed_schemas.py --limit 5          # mine first 5 only
    python scripts/seed_schemas.py --names aopwikirdf wikipathways
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "data" / "sources.csv"
QLEVER_CSV = ROOT / "data" / "qlever.csv"
OUTPUT_DIR = ROOT / "docker" / "schemas"


def mine_one(row: dict) -> dict | None:
    """Mine a single source and return the JSON-LD dict, or None on failure."""
    from rdfsolve.api import mine_schema

    name = row["dataset_name"]
    endpoint = row["endpoint_url"]
    graph = row["graph_uri"] if row.get("use_graph", "").lower() == "true" else None
    two_phase = row.get("two_phase", "").strip().lower() in (
        "true", "1", "yes",
    )

    try:
        result = mine_schema(
            endpoint_url=endpoint,
            dataset_name=name,
            graph_uris=[graph] if graph else None,
            two_phase=two_phase,
        )
        return result
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mine schemas → docker/schemas/",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max sources to mine (0=all)",
    )
    parser.add_argument(
        "--names", nargs="*",
        help="Mine only these dataset names",
    )

    src = parser.add_mutually_exclusive_group()
    src.add_argument(
        "--qlever", action="store_true",
        help="Mine only QLever-hosted sources",
    )
    src.add_argument(
        "--all-sources", action="store_true",
        help="Mine both default and QLever sources",
    )

    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Resolve which CSV file(s) to read
    if args.qlever:
        csv_files = [QLEVER_CSV]
    elif args.all_sources:
        csv_files = [DEFAULT_CSV, QLEVER_CSV]
    else:
        csv_files = [DEFAULT_CSV]

    rows: list[dict[str, str]] = []
    for csv_file in csv_files:
        with open(csv_file, newline="") as f:
            reader = csv.DictReader(f)
            rows.extend(reader)

    if args.names:
        rows = [r for r in rows if r["dataset_name"] in args.names]
    if args.limit:
        rows = rows[: args.limit]


    success = 0
    for row in rows:
        name = row["dataset_name"]
        outfile = OUTPUT_DIR / f"{name}_schema.jsonld"

        # Skip if already exists
        if outfile.exists():
            success += 1
            continue

        result = mine_one(row)
        if result:
            with open(outfile, "w") as fp:
                json.dump(result, fp, indent=2)
            success += 1



if __name__ == "__main__":
    main()
