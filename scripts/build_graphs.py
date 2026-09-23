#!/usr/bin/env python
"""Export dataset-scoped connectivity from canonical schemas and class mappings."""

import argparse
import json
from pathlib import Path

from networkx import node_link_data

from rdfsolve.analysis.connectivity import build_connectivity, compare_schemas
from rdfsolve.analysis.io import load_schemas, read_class_mappings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("schemas_dir", type=Path)
    parser.add_argument("--mappings-dir", type=Path)
    parser.add_argument("--output", type=Path, default=Path("output/graphs"))
    parser.add_argument("--extraction-mode", choices=["remote", "local", "grouped", "unknown"])
    args = parser.parse_args()
    schemas = load_schemas(args.schemas_dir, extraction_mode=args.extraction_mode)
    if not schemas:
        parser.error("No canonical schema snapshots found")
    edges, imports = [], {}
    if args.mappings_dir:
        for path in sorted(args.mappings_dir.rglob("*.sssom.tsv")):
            found, report = read_class_mappings(path, schemas)
            edges.extend(found)
            imports[str(path)] = report
    graph = build_connectivity(schemas, class_mappings=edges)
    args.output.mkdir(parents=True, exist_ok=True)
    for name, data in (
        ("class_connectivity.json", node_link_data(graph)),
        ("schema_overlaps.json", compare_schemas(schemas)),
        ("mapping_imports.json", imports),
    ):
        (args.output / name).write_text(json.dumps(data, indent=2))
    print(
        f"{len(schemas)} datasets, {len(graph)} class nodes, {graph.number_of_edges()} evidence edges"
    )


if __name__ == "__main__":
    main()
