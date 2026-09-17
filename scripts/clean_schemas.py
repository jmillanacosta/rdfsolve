#!/usr/bin/env python3
"""Remove service and engine patterns from mined schemas in a run directory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from rdfsolve.analysis.io import load_schemas
from rdfsolve.schema_models._constants import (
    SUGGESTED_SERVICE_GRAPHS,
    SUGGESTED_SERVICE_NAMESPACES,
)


def selections(args) -> tuple[list[str], list[str]]:
    """Read namespaces and graph IRIs from the rules file and the command line."""
    namespaces = list(args.namespace)
    graph_uris = list(args.graph)
    if args.rules:
        rules = yaml.safe_load(args.rules.read_text()) or {}
        unknown = set(rules) - {"namespaces", "graph_uris"}
        if unknown:
            raise ValueError(f"Unknown keys in {args.rules}: {sorted(unknown)}")
        namespaces += rules.get("namespaces") or []
        graph_uris += rules.get("graph_uris") or []
    if args.suggested:
        namespaces += SUGGESTED_SERVICE_NAMESPACES
        graph_uris += SUGGESTED_SERVICE_GRAPHS
    if not namespaces and not graph_uris:
        raise ValueError("Select --namespace, --graph, --rules or --suggested; nothing is implicit")
    return namespaces, graph_uris


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="Directory holding *_schema.json files")
    parser.add_argument("--output-dir", type=Path, required=True, help="New output directory")
    parser.add_argument("--rules", type=Path, help="YAML with namespaces and graph_uris lists")
    parser.add_argument("--namespace", action="append", default=[], help="IRI prefix to remove")
    parser.add_argument("--graph", action="append", default=[], help="Graph IRI prefix to remove")
    parser.add_argument(
        "--suggested", action="store_true", help="Add the suggested service namespaces and graphs"
    )
    parser.add_argument(
        "--drop-unattributed",
        action="store_true",
        help="Also drop patterns that carry no graph evidence when --graph is used",
    )
    args = parser.parse_args()

    namespaces, graph_uris = selections(args)
    schemas = load_schemas(args.run_dir)
    if not schemas:
        raise FileNotFoundError(f"No mined schemas under {args.run_dir}")
    args.output_dir.mkdir(parents=True, exist_ok=False)

    report = {"namespaces": namespaces, "graph_uris": graph_uris, "datasets": {}}
    for name, schema in sorted(schemas.items()):
        cleaned = schema.clean_schema(
            namespaces=namespaces,
            graph_uris=graph_uris,
            drop_unattributed=args.drop_unattributed,
        )
        target = args.output_dir / name
        target.mkdir(parents=True)
        (target / f"{name}_schema_clean.json").write_text(
            json.dumps(cleaned.to_dict(), indent=2) + "\n"
        )
        report["datasets"][name] = {
            "patterns_before": len(schema.patterns),
            "patterns_after": len(cleaned.patterns),
            "removed": len(schema.patterns) - len(cleaned.patterns),
        }
        if not cleaned.patterns:
            report["datasets"][name]["state"] = "empty after cleaning"
    (args.output_dir / "clean_report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
