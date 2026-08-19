#!/usr/bin/env python
"""Build connectivity graphs from mined schemas."""

import json
import logging
from collections import defaultdict
from pathlib import Path

import networkx as nx
import pandas as pd
from rdfsolve.models import MinedSchema
from rdfsolve.schema_utils import extract_class_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def collect_schemas(schemas_dir: Path) -> dict[str, list[MinedSchema]]:
    by_dataset = defaultdict(list)
    for sf in sorted(schemas_dir.rglob("*_schema.jsonld")):
        try:
            ms = MinedSchema.from_jsonld(sf)
            ds = ms.about.dataset_name or sf.parent.name
            by_dataset[ds].append(ms)
        except Exception as exc:
            log.warning("SKIP %s: %s", sf.name, exc)
    return dict(by_dataset)


def select_best_schema(candidates: list[MinedSchema]) -> MinedSchema:
    p1 = [s for s in candidates if s.about.strategy == "qlever_oneshot"]
    if p1:
        return max(p1, key=lambda s: s.about.pattern_count or 0)
    p2 = [s for s in candidates if (s.about.strategy or "").startswith("qlever")]
    if p2:
        return max(p2, key=lambda s: s.about.pattern_count or 0)
    p3 = [s for s in candidates if s.about.pattern_count]
    if p3:
        return max(p3, key=lambda s: s.about.pattern_count)
    return max(candidates, key=lambda s: len(s.patterns))


def build_schema_graph(schemas: list[MinedSchema]) -> nx.Graph:
    G = nx.Graph()

    for schema in schemas:
        name = schema.about.dataset_name or "unknown"
        classes = extract_class_set(schema)
        G.add_node(name, pattern_count=len(schema.patterns), classes=len(classes))

    names = list(G.nodes())
    for i, n1 in enumerate(names):
        classes1 = extract_class_set(schemas[i])
        for j, n2 in enumerate(names[i + 1:], i + 1):
            classes2 = extract_class_set(schemas[j])
            overlap = len(classes1 & classes2)
            if overlap > 0:
                G.add_edge(n1, n2, weight=overlap)

    return G


def export_to_parquet(G: nx.Graph, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    if G.number_of_edges() > 0:
        edges_df = pd.DataFrame([
            {"source": u, "target": v, "weight": d["weight"]}
            for u, v, d in G.edges(data=True)
        ])
        edges_df.to_parquet(output_dir / "schema_edges.parquet")

    comp_map = {n: i for i, comp in enumerate(nx.connected_components(G)) for n in comp}
    nodes_df = pd.DataFrame([
        {
            "dataset": n,
            "pattern_count": d.get("pattern_count", 0),
            "component": comp_map.get(n, 0)
        }
        for n, d in G.nodes(data=True)
    ])
    nodes_df.to_parquet(output_dir / "schema_nodes.parquet")

    log.info("Exported graphs to %s", output_dir)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("schemas_dir", type=Path)
    parser.add_argument("--output", type=Path, default=Path("output/graphs"))
    args = parser.parse_args()

    log.info("Collecting schemas from %s", args.schemas_dir)
    by_dataset = collect_schemas(args.schemas_dir)
    log.info("Found %d datasets", len(by_dataset))

    schemas = [select_best_schema(candidates) for candidates in by_dataset.values()]
    log.info("Selected %d best schemas", len(schemas))

    G = build_schema_graph(schemas)
    log.info("Built schema graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())

    export_to_parquet(G, args.output)
    log.info("Done")


if __name__ == "__main__":
    main()
