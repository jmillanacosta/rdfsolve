#!/usr/bin/env python3
"""Report schema-walk lengths without querying or verifying instance joins."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from rdfsolve.schema_models import MinedSchema


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("schema", type=Path, help="Canonical mined-schema JSON")
    parser.add_argument("--max-hops", type=int, default=3)
    parser.add_argument("--max-paths-per-length", type=int, default=100)
    parser.add_argument("--output-dir", type=Path, help="New directory for the schema and SHACL")
    args = parser.parse_args()
    started = time.perf_counter()
    schema = MinedSchema.from_json(args.schema)
    routes = schema.discover_paths(max_hops=args.max_hops, max_paths_per_length=args.max_paths_per_length)
    summary = {
        "source": schema.about.dataset_name,
        "source_version": schema.about.schema_version,
        "input": str(args.schema.resolve()),
        "endpoint_queries": 0,
        "instance_support": "not_checked",
        "schema_edges": routes.edge_count,
        "schema_walk_counts_by_hops": routes.walk_counts,
        "saved_candidates_by_hops": {
            hops: sum(len(path.steps) == hops for path in routes.paths)
            for hops in range(2, routes.max_hops + 1)
        },
        "truncated_lengths": routes.truncated_lengths,
        "seconds": round(time.perf_counter() - started, 3),
        "note": "Counts describe schema walks, not occurrences or coverage in instance data.",
    }
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=False)
        (args.output_dir / "schema.json").write_text(json.dumps(schema.to_dict(), indent=2) + "\n")
        (args.output_dir / "shapes.ttl").write_text(schema.to_shacl())
        (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
