#!/usr/bin/env python3
"""Mine JSON-LD schemas from a sources file and save to docker/schemas/.

Reads the source registry via ``rdfsolve.sources.load_sources()`` which
auto-detects YAML, JSON-LD, or CSV by file extension.

Usage:
    python scripts/seed_schemas.py                              # default sources.yaml
    python scripts/seed_schemas.py -s data/sources_all.yaml     # whole-db entries
    python scripts/seed_schemas.py --limit 5                    # first 5 only
    python scripts/seed_schemas.py --names aopwikirdf wikipathways
    python scripts/seed_schemas.py --no-filter-service          # keep service namespaces
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

OUTPUT_DIR = ROOT / "docker" / "schemas"


def mine_one(
    entry: dict,
    filter_service: bool = True,
) -> dict | None:
    """Mine a single source and return the JSON-LD dict, or None on failure."""
    from rdfsolve.api import mine_schema

    name = entry["name"]
    endpoint = entry.get("endpoint", "")
    if not endpoint:
        return None

    use_graph = entry.get("use_graph", False)
    graph_uris = entry.get("graph_uris", []) if use_graph else None
    two_phase = entry.get("two_phase", True)

    try:
        result = mine_schema(
            endpoint_url=endpoint,
            dataset_name=name,
            graph_uris=graph_uris if graph_uris else None,
            two_phase=two_phase,
            filter_service_namespaces=filter_service,
        )
        return result
    except Exception as exc:
        print(f"  ERROR mining {name}: {exc}", file=sys.stderr)
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mine schemas → docker/schemas/",
    )
    parser.add_argument(
        "-s", "--sources",
        type=str,
        default=None,
        help="Path to sources file (YAML/JSON-LD/CSV). Default: data/sources.yaml",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max sources to mine (0=all)",
    )
    parser.add_argument(
        "--names", nargs="*",
        help="Mine only these dataset names",
    )
    parser.add_argument(
        "--no-filter-service", action="store_true",
        help="Keep service/system namespace patterns in output",
    )

    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── load sources via rdfsolve.sources ─────────────────────────
    from rdfsolve.sources import load_sources

    entries = load_sources(args.sources)

    if args.names:
        entries = [e for e in entries if e["name"] in args.names]
    if args.limit:
        entries = entries[: args.limit]

    print(f"Mining {len(entries)} source(s)…")

    success = 0
    for entry in entries:
        name = entry["name"]
        outfile = OUTPUT_DIR / f"{name}_schema.jsonld"

        # Skip if already exists
        if outfile.exists():
            print(f"  [SKIP] {name} — already exists")
            success += 1
            continue

        print(f"  [MINE] {name} …", end=" ", flush=True)
        result = mine_one(
            entry,
            filter_service=not args.no_filter_service,
        )
        if result:
            with open(outfile, "w") as fp:
                json.dump(result, fp, indent=2)
            print("OK")
            success += 1
        else:
            print("FAIL")

    print(f"\nDone: {success}/{len(entries)} succeeded.")


if __name__ == "__main__":
    main()
