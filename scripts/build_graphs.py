#!/usr/bin/env python
"""Build connectivity graphs from schemas and SSSOM mappings.

Creates two types of graphs:
1. Dataset-level graph: Nodes are datasets, edges represent connections
2. Class-level graph: Nodes are classes, edges are schema patterns + SSSOM mappings

Edge sources:
- Schema patterns: Intra-dataset class relationships (property edges)
- SSSOM mappings: Inter-dataset class mappings (skos:relatedMatch, etc.)
"""

import logging
from collections import defaultdict
from pathlib import Path

import networkx as nx
import pandas as pd

from rdfsolve.models import MinedSchema
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_utils import extract_class_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def parse_sssom_tsv(filepath: Path) -> tuple[dict, list[dict]]:
    """Parse SSSOM TSV file returning metadata and mappings."""
    metadata: dict = {}
    mappings: list[dict] = []
    headers: list[str] = []

    with filepath.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                content = line[1:].strip()
                if ":" in content:
                    key, _, value = content.partition(":")
                    metadata[key.strip()] = value.strip()
            elif not headers:
                headers = line.split("\t")
            else:
                values = line.split("\t")
                mappings.append(dict(zip(headers, values)))

    return metadata, mappings


def extract_dataset_from_source(source_uri: str) -> str:
    """Extract dataset name from VoID URI."""
    if "/dataset/" in source_uri:
        return source_uri.split("/dataset/")[-1].strip()
    return source_uri


def collect_schemas(schemas_dir: Path) -> dict[str, list[MinedSchema]]:
    """Collect all schemas grouped by dataset."""
    by_dataset: dict[str, list[MinedSchema]] = defaultdict(list)
    for sf in sorted(schemas_dir.rglob("*_schema.jsonld")):
        try:
            ms = MinedSchema.from_jsonld(sf)
            ds = ms.about.dataset_name or sf.parent.name
            by_dataset[ds].append(ms)
        except Exception as exc:
            log.warning("SKIP %s: %s", sf.name, exc)
    return dict(by_dataset)


def collect_sssom_mappings(mappings_dir: Path) -> list[tuple[dict, list[dict]]]:
    """Collect all SSSOM mapping files."""
    sssom_files = []
    for f in mappings_dir.rglob("*.sssom.tsv"):
        try:
            metadata, mappings = parse_sssom_tsv(f)
            if mappings:
                sssom_files.append((metadata, mappings))
        except Exception as exc:
            log.warning("SKIP %s: %s", f.name, exc)
    return sssom_files


def select_best_schema(candidates: list[MinedSchema]) -> MinedSchema:
    """Select the best schema from candidates based on strategy and pattern count."""
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


def build_dataset_graph(
    schemas: list[MinedSchema],
    sssom_data: list[tuple[dict, list[dict]]],
) -> nx.Graph:
    """Build dataset-level connectivity graph.

    Nodes: datasets
    Edges:
        - weight: number of connections
        - schema_overlap: shared classes from schema analysis
        - sssom_mappings: mappings from SSSOM files
    """
    G = nx.Graph()

    # Add dataset nodes from schemas
    for schema in schemas:
        name = schema.about.dataset_name or "unknown"
        classes = extract_class_set(schema)
        G.add_node(
            name,
            pattern_count=len(schema.patterns),
            class_count=len(classes),
        )

    # Add edges from schema class overlap
    names = list(G.nodes())
    schema_map = {s.about.dataset_name: s for s in schemas if s.about.dataset_name}

    for i, n1 in enumerate(names):
        s1 = schema_map.get(n1)
        if not s1:
            continue
        classes1 = extract_class_set(s1)

        for j, n2 in enumerate(names[i + 1 :], i + 1):
            s2 = schema_map.get(n2)
            if not s2:
                continue
            classes2 = extract_class_set(s2)
            overlap = len(classes1 & classes2)

            if overlap > 0:
                if G.has_edge(n1, n2):
                    G[n1][n2]["schema_overlap"] += overlap
                    G[n1][n2]["weight"] += overlap
                else:
                    G.add_edge(n1, n2, schema_overlap=overlap, sssom_mappings=0, weight=overlap)

    # Add edges from SSSOM mappings
    for metadata, mappings in sssom_data:
        subj_source = metadata.get("subject_source", "")
        obj_source = metadata.get("object_source", "")

        ds1 = extract_dataset_from_source(subj_source)
        ds2 = extract_dataset_from_source(obj_source)

        if ds1 and ds2 and ds1 != ds2:
            # Ensure nodes exist
            if ds1 not in G:
                G.add_node(ds1, pattern_count=0, class_count=0)
            if ds2 not in G:
                G.add_node(ds2, pattern_count=0, class_count=0)

            mapping_count = len(mappings)
            if G.has_edge(ds1, ds2):
                G[ds1][ds2]["sssom_mappings"] += mapping_count
                G[ds1][ds2]["weight"] += mapping_count
            else:
                G.add_edge(ds1, ds2, schema_overlap=0, sssom_mappings=mapping_count, weight=mapping_count)

    return G


def build_class_graph(
    schemas: list[MinedSchema],
    sssom_data: list[tuple[dict, list[dict]]],
) -> nx.MultiDiGraph:
    """Build class-level connectivity graph.

    Nodes: class URIs
    Edges:
        - From schema patterns: subject_class -> object_class (property edges)
        - From SSSOM mappings: subject_id -> object_id (mapping edges)
    """
    G = nx.MultiDiGraph()

    # Add edges from schema patterns
    for schema in schemas:
        dataset = schema.about.dataset_name or "unknown"
        for pat in schema.patterns:
            # Skip sentinel objects (Literal, Resource, BlankNode)
            if not pat.object_class or pat.object_class in _SENTINEL_OBJECTS:
                continue
            # Also skip blank node prefixes
            if pat.object_class.startswith("_:"):
                continue

            # Add nodes with dataset membership
            for uri in (pat.subject_class, pat.object_class):
                if uri and uri not in G:
                    G.add_node(uri, datasets=set(), node_type="class")
                if uri:
                    G.nodes[uri]["datasets"].add(dataset)

            # Add edge (schema pattern = property relationship)
            if pat.subject_class and pat.object_class:
                G.add_edge(
                    pat.subject_class,
                    pat.object_class,
                    edge_type="schema_pattern",
                    predicate=pat.property_uri,
                    dataset=dataset,
                    count=pat.count or 0,
                )

    # Add edges from SSSOM mappings (class-to-class semantic mappings)
    for metadata, mappings in sssom_data:
        subj_source = extract_dataset_from_source(metadata.get("subject_source", ""))
        obj_source = extract_dataset_from_source(metadata.get("object_source", ""))

        for m in mappings:
            subj_id = m.get("subject_id", "")
            obj_id = m.get("object_id", "")
            predicate = m.get("predicate_id", "")
            confidence = m.get("confidence", "")
            justification = m.get("mapping_justification", "")

            # Skip invalid or empty mappings
            if not subj_id or not obj_id:
                continue

            # Add nodes (classes) with dataset membership
            for uri, ds in ((subj_id, subj_source), (obj_id, obj_source)):
                if uri not in G:
                    G.add_node(uri, datasets=set(), node_type="class")
                if ds:
                    G.nodes[uri]["datasets"].add(ds)

            # Add edge (SSSOM mapping = semantic equivalence/similarity)
            G.add_edge(
                subj_id,
                obj_id,
                edge_type="sssom_mapping",
                predicate=predicate,
                subject_source=subj_source,
                object_source=obj_source,
                confidence=float(confidence) if confidence else None,
                justification=justification,
            )

    # Convert dataset sets to lists for serialization
    for node in G.nodes():
        G.nodes[node]["datasets"] = list(G.nodes[node].get("datasets", set()))

    return G


def export_dataset_graph(G: nx.Graph, output_dir: Path):
    """Export dataset-level graph to parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if G.number_of_edges() > 0:
        edges_df = pd.DataFrame(
            [
                {
                    "source": u,
                    "target": v,
                    "weight": d.get("weight", 0),
                    "schema_overlap": d.get("schema_overlap", 0),
                    "sssom_mappings": d.get("sssom_mappings", 0),
                }
                for u, v, d in G.edges(data=True)
            ]
        )
        edges_df.to_parquet(output_dir / "dataset_edges.parquet")

    comp_map = {n: i for i, comp in enumerate(nx.connected_components(G)) for n in comp}
    nodes_df = pd.DataFrame(
        [
            {
                "dataset": n,
                "pattern_count": d.get("pattern_count", 0),
                "class_count": d.get("class_count", 0),
                "component": comp_map.get(n, 0),
            }
            for n, d in G.nodes(data=True)
        ]
    )
    nodes_df.to_parquet(output_dir / "dataset_nodes.parquet")

    log.info("Exported dataset graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())


def export_class_graph(G: nx.MultiDiGraph, output_dir: Path):
    """Export class-level graph to parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if G.number_of_edges() > 0:
        edges_data = []
        for u, v, d in G.edges(data=True):
            edges_data.append(
                {
                    "source": u,
                    "target": v,
                    "edge_type": d.get("edge_type", ""),
                    "predicate": d.get("predicate", ""),
                    "dataset": d.get("dataset", ""),
                    "subject_source": d.get("subject_source", ""),
                    "object_source": d.get("object_source", ""),
                    "count": d.get("count"),
                    "confidence": d.get("confidence"),
                    "justification": d.get("justification", ""),
                }
            )
        edges_df = pd.DataFrame(edges_data)
        edges_df.to_parquet(output_dir / "class_edges.parquet")

    nodes_df = pd.DataFrame(
        [
            {
                "class_uri": n,
                "node_type": d.get("node_type", "class"),
                "datasets": ",".join(d.get("datasets", [])),
                "dataset_count": len(d.get("datasets", [])),
            }
            for n, d in G.nodes(data=True)
        ]
    )
    nodes_df.to_parquet(output_dir / "class_nodes.parquet")

    log.info("Exported class graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build connectivity graphs from schemas and SSSOM mappings")
    parser.add_argument("schemas_dir", type=Path, help="Directory containing schema files")
    parser.add_argument("--mappings-dir", type=Path, help="Directory containing SSSOM mapping files")
    parser.add_argument("--output", type=Path, default=Path("output/graphs"), help="Output directory")
    args = parser.parse_args()

    # Collect schemas
    log.info("Collecting schemas from %s", args.schemas_dir)
    by_dataset = collect_schemas(args.schemas_dir)
    log.info("Found %d datasets with schemas", len(by_dataset))

    schemas = [select_best_schema(candidates) for candidates in by_dataset.values()]
    log.info("Selected %d best schemas", len(schemas))

    # Collect SSSOM mappings
    sssom_data: list[tuple[dict, list[dict]]] = []
    if args.mappings_dir and args.mappings_dir.exists():
        log.info("Collecting SSSOM mappings from %s", args.mappings_dir)
        sssom_data = collect_sssom_mappings(args.mappings_dir)
        log.info("Found %d SSSOM mapping files", len(sssom_data))
    else:
        # Try default location
        default_mappings = args.schemas_dir / "mappings"
        if default_mappings.exists():
            log.info("Collecting SSSOM mappings from %s", default_mappings)
            sssom_data = collect_sssom_mappings(default_mappings)
            log.info("Found %d SSSOM mapping files", len(sssom_data))

    # Build and export dataset-level graph
    log.info("Building dataset-level graph...")
    dataset_graph = build_dataset_graph(schemas, sssom_data)
    export_dataset_graph(dataset_graph, args.output)

    # Build and export class-level graph
    log.info("Building class-level graph...")
    class_graph = build_class_graph(schemas, sssom_data)
    export_class_graph(class_graph, args.output)

    # Summary statistics
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    log.info("Dataset graph: %d datasets, %d connections", dataset_graph.number_of_nodes(), dataset_graph.number_of_edges())
    log.info("Class graph: %d classes, %d edges", class_graph.number_of_nodes(), class_graph.number_of_edges())

    # Edge type breakdown
    schema_edges = sum(1 for _, _, d in class_graph.edges(data=True) if d.get("edge_type") == "schema_pattern")
    sssom_edges = sum(1 for _, _, d in class_graph.edges(data=True) if d.get("edge_type") == "sssom_mapping")
    log.info("  Schema pattern edges: %d", schema_edges)
    log.info("  SSSOM mapping edges: %d", sssom_edges)

    log.info("Done. Output: %s", args.output)


if __name__ == "__main__":
    main()
