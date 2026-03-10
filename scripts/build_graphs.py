#!/usr/bin/env python3
"""Build dataset-level connectivity graphs and export to Parquet.

Pipeline step 4b + 12 (see docs/notes/pipeline_planning.md):

  4b  Schema selection – pick the best canonical schema per dataset and copy
      to results/paper_data/schemas/.

  12  Graph construction – build G_schema / G_raw / G_inferred at the dataset
      level and export edge / node tables as Parquet plus a JSON metrics file.

Output layout::

    results/paper_data/
        schemas/                 ← selected canonical schemas (JSON-LD)
        graphs/
            edges_schema.parquet
            edges_raw.parquet
            edges_inferred.parquet
            nodes.parquet
        benchmarks.jsonl         ← one JSON object per pipeline step
        metadata.json

Usage
-----
Full run (all schemas in docker/schemas/)::

    python scripts/build_graphs.py

Filter to specific datasets (--datasets accepts shell-style patterns)::

    python scripts/build_graphs.py --datasets aopwikirdf wikipathways chembl

Custom input/output::

    python scripts/build_graphs.py \\
        --schemas-dir docker/schemas \\
        --mappings-dir docker/mappings \\
        --output-dir results/paper_data

Dry-run (schema selection only, no graph build)::

    python scripts/build_graphs.py --schema-only
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

DEFAULT_SCHEMAS_DIR  = ROOT / "docker" / "schemas"
DEFAULT_MAPPINGS_DIR = ROOT / "docker" / "mappings"
DEFAULT_OUTPUT_DIR   = ROOT / "results" / "paper_data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Schema selection
# ─────────────────────────────────────────────────────────────────────────────

def select_best_schema(candidates: list) -> "Any":  # list[MinedSchema]
    """Return the single best schema from a list of candidates.

    Priority order (from pipeline_planning.md §3 Phase 1):
      P1 – qlever_oneshot               (highest pattern_count)
      P2 – any qlever-* strategy        (highest pattern_count)
      P3 – any strategy with counts     (highest pattern_count)
      P4 – fallback: most patterns (len)
    """
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


def collect_schemas(
    schemas_dir: Path,
    dataset_filter: list[str] | None = None,
) -> dict[str, list]:  # dataset_name → list[MinedSchema]
    """Load all *_schema.jsonld files and group by dataset_name."""
    from rdfsolve.models import MinedSchema

    by_dataset: dict[str, list] = defaultdict(list)

    schema_files = sorted(schemas_dir.rglob("*_schema.jsonld"))
    logger.info("Found %d schema files under %s", len(schema_files), schemas_dir)

    for sf in schema_files:
        # Optional dataset filter: check all path components relative to
        # schemas_dir, not just the immediate parent.  Schema files can be
        # nested under <dataset>/qlever/typed/one_shot/ so sf.parent.name
        # would be "one_shot", not the dataset name.
        if dataset_filter:
            try:
                rel_parts = sf.relative_to(schemas_dir).parts
            except ValueError:
                rel_parts = sf.parts
            if not any(
                fnmatch.fnmatch(part, pat)
                for part in rel_parts
                for pat in dataset_filter
            ):
                continue
        try:
            ms = MinedSchema.from_jsonld(sf)
            ds = ms.about.dataset_name or sf.parent.name
            by_dataset[ds].append(ms)
        except Exception as exc:
            logger.warning("  SKIP %s: %s", sf.name, exc)

    return dict(by_dataset)


# ─────────────────────────────────────────────────────────────────────────────
# Graph builders
# ─────────────────────────────────────────────────────────────────────────────

def build_schema_graph(schemas: list) -> "nx.Graph":
    """Build G_schema: dataset-level typed-object cross-links from schema patterns.

    Returns
    -------
    G : nx.Graph
        Dataset-level graph (undirected, weighted).
    class_to_datasets : dict[str, set[str]]
        Mapping from class URI → set of dataset names that use it as a subject.
    edge_predicates : dict[tuple[str,str], Counter]
        For each ordered (dataset_a, dataset_b) pair (a < b), a Counter of
        predicate URIs and how many schema patterns contribute them.
    """
    import networkx as nx
    from collections import Counter

    G = nx.Graph()
    class_to_datasets: dict[str, set[str]] = defaultdict(set)
    # (min_ds, max_ds) → Counter{predicate_uri: count}
    edge_predicates: dict[tuple[str, str], Counter] = defaultdict(Counter)

    for ms in schemas:
        ds = ms.about.dataset_name or ""
        G.add_node(ds, pattern_count=ms.about.pattern_count or len(ms.patterns))
        for pat in ms.patterns:
            if pat.subject_class not in ("Literal", "Resource"):
                class_to_datasets[pat.subject_class].add(ds)

    for ms in schemas:
        ds_src = ms.about.dataset_name or ""
        for pat in ms.patterns:
            if pat.object_class in ("Literal", "Resource"):
                continue
            for ds_tgt in class_to_datasets.get(pat.object_class, ()):
                if ds_tgt == ds_src:
                    continue
                if G.has_edge(ds_src, ds_tgt):
                    G[ds_src][ds_tgt]["weight"] += 1
                else:
                    G.add_edge(ds_src, ds_tgt, weight=1)
                pair = (ds_src, ds_tgt) if ds_src < ds_tgt else (ds_tgt, ds_src)
                edge_predicates[pair][pat.property_uri] += 1

    return G, class_to_datasets, edge_predicates


# ─────────────────────────────────────────────────────────────────────────────
# Parquet export helpers
# ─────────────────────────────────────────────────────────────────────────────

def graph_to_edges_df(G: "nx.Graph") -> "pd.DataFrame":
    import pandas as pd
    rows = [
        {"dataset_a": u, "dataset_b": v, "weight": d.get("weight", 1)}
        for u, v, d in G.edges(data=True)
    ]
    return pd.DataFrame(rows, columns=["dataset_a", "dataset_b", "weight"])


def graph_to_nodes_df(G: "nx.Graph", component_map: dict[str, int]) -> "pd.DataFrame":
    import pandas as pd
    rows = [
        {
            "dataset":        n,
            "pattern_count":  G.nodes[n].get("pattern_count", 0),
            "weighted_degree": G.degree(n, weight="weight"),
            "component_id":   component_map.get(n, -1),
        }
        for n in sorted(G.nodes())
    ]
    return pd.DataFrame(rows)


def component_map(G: "nx.Graph") -> dict[str, int]:
    import networkx as nx
    comps = list(nx.connected_components(G))
    comps_sorted = sorted(comps, key=len, reverse=True)
    cmap: dict[str, int] = {}
    for idx, comp in enumerate(comps_sorted):
        for node in comp:
            cmap[node] = idx
    return cmap


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark helpers
# ─────────────────────────────────────────────────────────────────────────────

class BenchmarkLog:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        # append mode so multiple runs accumulate
        self._fh = path.open("a", encoding="utf-8")

    def record(self, **kwargs: object) -> None:
        kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        self._fh.write(json.dumps(kwargs, ensure_ascii=False) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Select canonical schemas and build dataset-level graphs → Parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--schemas-dir",
        default=str(DEFAULT_SCHEMAS_DIR),
        metavar="DIR",
        help=f"Root schemas directory (default: {DEFAULT_SCHEMAS_DIR})",
    )
    parser.add_argument(
        "--mappings-dir",
        default=str(DEFAULT_MAPPINGS_DIR),
        metavar="DIR",
        help=f"Root mappings directory (default: {DEFAULT_MAPPINGS_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        metavar="DIR",
        help=f"paper_data output root (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        metavar="NAME",
        help=(
            "Only process these dataset folder names "
            "(shell-style globs accepted, e.g. 'chembl*'). "
            "Default: all."
        ),
    )
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Run schema selection only; skip graph construction.",
    )
    parser.add_argument(
        "--no-copy-schemas",
        action="store_true",
        help="Skip copying selected schemas to output-dir/schemas/.",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    schemas_dir  = Path(args.schemas_dir)
    mappings_dir = Path(args.mappings_dir)
    output_dir   = Path(args.output_dir)
    graphs_dir   = output_dir / "graphs"
    schemas_out  = output_dir / "schemas"

    graphs_dir.mkdir(parents=True, exist_ok=True)
    schemas_out.mkdir(parents=True, exist_ok=True)

    bench = BenchmarkLog(output_dir / "benchmarks.jsonl")

    # ── Step 4b: Schema selection ─────────────────────────────────────────────
    logger.info("═══ Step 4b: Schema selection ═══")
    t0 = time.perf_counter()

    by_dataset = collect_schemas(schemas_dir, dataset_filter=args.datasets or None)
    if not by_dataset:
        logger.error("No schemas found under %s", schemas_dir)
        sys.exit(1)

    selected: dict[str, object] = {}  # dataset → MinedSchema
    for ds, candidates in sorted(by_dataset.items()):
        best = select_best_schema(candidates)
        selected[ds] = best
        logger.info(
            "  %-30s  strategy=%-25s  patterns=%d",
            ds,
            best.about.strategy,
            best.about.pattern_count or len(best.patterns),
        )
        bench.record(
            step="schema_selection",
            dataset=ds,
            selected_strategy=best.about.strategy,
            pattern_count=best.about.pattern_count or len(best.patterns),
            candidates=len(candidates),
        )

    t_sel = time.perf_counter() - t0
    logger.info("Schema selection: %d datasets in %.1fs", len(selected), t_sel)
    bench.record(step="schema_selection_total", datasets=len(selected), elapsed_s=round(t_sel, 2))

    # Copy selected schemas to paper_data/schemas/
    if not args.no_copy_schemas:
        import shutil
        copied = 0
        for ds, ms in selected.items():
            # Locate the source file — from_jsonld loses the path, but
            # the schema file is always in schemas_dir/<ds>/<ds>_schema.jsonld
            # or schemas_dir/<ds_folder>/<name>_schema.jsonld (we search).
            matches = list(schemas_dir.rglob(f"*_schema.jsonld"))
            for candidate_path in matches:
                if candidate_path.parent.name == ds or candidate_path.stem.startswith(ds):
                    dest = schemas_out / f"{ds}_schema.jsonld"
                    if not dest.exists():
                        shutil.copy2(candidate_path, dest)
                        copied += 1
                    break
        logger.info("Copied %d schema files → %s", copied, schemas_out)

    if args.schema_only:
        logger.info("--schema-only: stopping after selection.")
        bench.close()
        return

    # ── Step 12: Graph construction ───────────────────────────────────────────
    logger.info("═══ Step 12: Graph construction ═══")

    schemas_list = list(selected.values())

    # G_schema
    logger.info("Building G_schema …")
    t0 = time.perf_counter()
    G_schema, class_to_datasets, schema_edge_predicates = build_schema_graph(schemas_list)
    t_sch = time.perf_counter() - t0
    logger.info(
        "G_schema — nodes: %d  edges: %d  (%.1fs)",
        G_schema.number_of_nodes(), G_schema.number_of_edges(), t_sch,
    )
    bench.record(
        step="build_graphs",
        graph="G_schema",
        nodes=G_schema.number_of_nodes(),
        edges=G_schema.number_of_edges(),
        elapsed_s=round(t_sch, 2),
    )

    # G_raw
    from rdfsolve.models import Mapping

    SSSOM_DIR = mappings_dir / "sssom"
    SEMRA_DIR = mappings_dir / "semra"
    INST_DIR  = mappings_dir / "instance_matching"
    INF_DIR   = mappings_dir / "inferenced"

    raw_paths = (
        sorted(SSSOM_DIR.glob("*.jsonld")) if SSSOM_DIR.exists() else []
    ) + (
        sorted(SEMRA_DIR.glob("*.jsonld")) if SEMRA_DIR.exists() else []
    ) + (
        sorted(INST_DIR.glob("*.jsonld"))  if INST_DIR.exists()  else []
    )

    if raw_paths:
        logger.info("Building G_raw from %d mapping files …", len(raw_paths))
        t0 = time.perf_counter()
        G_raw = Mapping.dataset_graph(
            paths=raw_paths,
            class_to_datasets=class_to_datasets,
            base_graph=G_schema.copy(),
            strategies={"sssom_import", "semra_import", "instance_matcher"},
        )
        t_raw = time.perf_counter() - t0
        new_raw = (
            {frozenset(e) for e in G_raw.edges()}
            - {frozenset(e) for e in G_schema.edges()}
        )
        logger.info(
            "G_raw     — nodes: %d  edges: %d  (+%d new)  (%.1fs)",
            G_raw.number_of_nodes(), G_raw.number_of_edges(), len(new_raw), t_raw,
        )
        bench.record(
            step="build_graphs",
            graph="G_raw",
            nodes=G_raw.number_of_nodes(),
            edges=G_raw.number_of_edges(),
            new_pairs=len(new_raw),
            input_files=len(raw_paths),
            elapsed_s=round(t_raw, 2),
        )
    else:
        logger.warning("No SSSOM/SeMRA/instance mapping files found — G_raw = G_schema")
        G_raw = G_schema.copy()

    # G_inferred
    inf_paths = sorted(INF_DIR.glob("*.jsonld")) if INF_DIR.exists() else []
    if inf_paths:
        logger.info("Building G_inferred from %d inferenced files …", len(inf_paths))
        t0 = time.perf_counter()
        G_inferred = Mapping.dataset_graph(
            paths=inf_paths,
            class_to_datasets=class_to_datasets,
            base_graph=G_raw.copy(),
            strategies={"inferenced"},
        )
        t_inf = time.perf_counter() - t0
        new_inf = (
            {frozenset(e) for e in G_inferred.edges()}
            - {frozenset(e) for e in G_raw.edges()}
        )
        logger.info(
            "G_inferred— nodes: %d  edges: %d  (+%d new)  (%.1fs)",
            G_inferred.number_of_nodes(), G_inferred.number_of_edges(), len(new_inf), t_inf,
        )
        bench.record(
            step="build_graphs",
            graph="G_inferred",
            nodes=G_inferred.number_of_nodes(),
            edges=G_inferred.number_of_edges(),
            new_pairs=len(new_inf),
            elapsed_s=round(t_inf, 2),
        )
    else:
        logger.warning("No inferenced mapping files found — G_inferred = G_raw")
        G_inferred = G_raw.copy()

    # ── Export to Parquet ─────────────────────────────────────────────────────
    logger.info("Exporting Parquet …")
    cmap_inferred = component_map(G_inferred)

    for label, G, fname in [
        ("G_schema",   G_schema,   "edges_schema.parquet"),
        ("G_raw",      G_raw,      "edges_raw.parquet"),
        ("G_inferred", G_inferred, "edges_inferred.parquet"),
    ]:
        df = graph_to_edges_df(G)
        out_path = graphs_dir / fname
        df.to_parquet(out_path, index=False)
        logger.info("  %s → %s  (%d rows)", label, out_path.name, len(df))

    # Nodes table based on G_inferred (most connected)
    df_nodes = graph_to_nodes_df(G_inferred, cmap_inferred)
    nodes_path = graphs_dir / "nodes.parquet"
    df_nodes.to_parquet(nodes_path, index=False)
    logger.info("  nodes → %s  (%d rows)", nodes_path.name, len(df_nodes))

    # ── Strategy + predicate counts from mapping files ────────────────────────
    import pandas as pd

    # Predicate-level schema edge table (for property-mapping analysis in notebook)
    pred_edge_rows = [
        {"dataset_a": a, "dataset_b": b, "predicate": pred, "count": cnt}
        for (a, b), counter in schema_edge_predicates.items()
        for pred, cnt in counter.items()
    ]
    if pred_edge_rows:
        df_pred_edges = pd.DataFrame(
            pred_edge_rows,
            columns=["dataset_a", "dataset_b", "predicate", "count"],
        )
        df_pred_edges.sort_values(
            ["dataset_a", "dataset_b", "count"],
            ascending=[True, True, False],
            inplace=True,
        )
        pred_edges_path = graphs_dir / "schema_edge_predicates.parquet"
        df_pred_edges.to_parquet(pred_edges_path, index=False)
        logger.info(
            "  schema_edge_predicates → %s  (%d rows)",
            pred_edges_path.name, len(df_pred_edges),
        )
    else:
        logger.warning("  schema_edge_predicates: empty — nothing to write")

    try:
        import ujson as _json_fast
    except ImportError:
        import json as _json_fast  # type: ignore[assignment]

    _SKIP_KEYS = frozenset({"void:inDataset", "dcterms:created"})
    strategy_counts: dict[str, int] = {}
    predicate_counts: dict[str, int] = {}

    all_mapping_paths = (
        list(SSSOM_DIR.glob("*.jsonld")) if SSSOM_DIR.exists() else []
    ) + (
        list(SEMRA_DIR.glob("*.jsonld")) if SEMRA_DIR.exists() else []
    ) + (
        list(INF_DIR.glob("*.jsonld"))   if INF_DIR.exists()   else []
    )
    for mf in all_mapping_paths:
        try:
            raw = _json_fast.loads(mf.read_bytes())
            strategy = raw.get("@about", {}).get("strategy", "unknown")
            ctx = {
                **raw.get("@about", {}).get("curie_map", {}),
                **raw.get("@context", {}),
            }
            for node in raw.get("@graph", ()):
                for key in node:
                    if key[0] == "@" or key in _SKIP_KEYS:
                        continue
                    val = node[key]
                    targets = val if isinstance(val, list) else (val,)
                    n = sum(
                        1 for t in targets
                        if isinstance(t, dict) and t.get("@id")
                    )
                    strategy_counts[strategy] = strategy_counts.get(strategy, 0) + n
                    predicate_counts[key] = predicate_counts.get(key, 0) + n
        except Exception:
            pass

    def _shorten(uri: str) -> str:
        for sep in ("#", "/"):
            idx = uri.rfind(sep)
            if idx >= 0 and idx < len(uri) - 1:
                return uri[idx + 1:]
        return uri

    if strategy_counts:
        strat_df = pd.DataFrame(
            [{"strategy": k, "count": v} for k, v in strategy_counts.items()]
        ).sort_values("count", ascending=False).reset_index(drop=True)
        strat_df.to_parquet(output_dir / "strategy_counts.parquet", index=False)
        logger.info("  strategy_counts → strategy_counts.parquet  (%d rows)", len(strat_df))

    if predicate_counts:
        pred_df = pd.DataFrame([
            {"predicate": k, "predicate_short": _shorten(k), "count": v}
            for k, v in predicate_counts.items()
        ]).sort_values("count", ascending=False).reset_index(drop=True)
        pred_df.to_parquet(output_dir / "predicate_counts.parquet", index=False)
        logger.info("  predicate_counts → predicate_counts.parquet  (%d rows)", len(pred_df))

    # ── metadata.json ─────────────────────────────────────────────────────────
    from rdfsolve.version import VERSION
    import subprocess

    try:
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT, stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        git_sha = "unknown"

    metadata = {
        "rdfsolve_version": VERSION,
        "git_sha": git_sha,
        "run_date": datetime.now(timezone.utc).isoformat(),
        "datasets": sorted(selected.keys()),
        "n_datasets": len(selected),
        "G_schema":   {"nodes": G_schema.number_of_nodes(),   "edges": G_schema.number_of_edges()},
        "G_raw":      {"nodes": G_raw.number_of_nodes(),      "edges": G_raw.number_of_edges()},
        "G_inferred": {"nodes": G_inferred.number_of_nodes(), "edges": G_inferred.number_of_edges()},
    }
    meta_path = output_dir / "metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    logger.info("metadata.json written.")

    bench.record(step="build_graphs_complete", **{
        k: v for k, v in metadata.items() if k != "datasets"
    })
    bench.close()

    logger.info("═══ Done ═══")
    logger.info("  Output: %s", output_dir)
    logger.info("  Graphs: %s", graphs_dir)
    for fname in sorted(graphs_dir.iterdir()):
        logger.info("    %s", fname.name)


if __name__ == "__main__":
    main()
