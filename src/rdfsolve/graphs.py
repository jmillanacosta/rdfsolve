"""Dataset-level connectivity graph construction and Parquet export.

Consolidates the graph-building logic previously in ``scripts/build_graphs.py``.

Public API
----------
- :func:`select_best_schema` — pick canonical schema per dataset
- :func:`collect_schemas` — load & group ``*_schema.jsonld`` files
- :func:`build_schema_graph` — G_schema from schema patterns
- :func:`build_mapping_graph` — G_raw / G_inferred from mapping files
- :func:`export_graphs_to_parquet` — write edge/node Parquet tables
- :func:`run_graph_pipeline` — full pipeline entry point (step 4b + 12)
"""

from __future__ import annotations

import fnmatch
import json
import logging
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema selection
# ---------------------------------------------------------------------------


def select_best_schema(candidates: list[Any]) -> Any:
    """Return the single best schema from a list of :class:`MinedSchema`.

    Priority order:

    1. ``qlever_oneshot`` strategy → highest ``pattern_count``
    2. Any ``qlever*`` strategy → highest ``pattern_count``
    3. Any strategy with a non-zero ``pattern_count``
    4. Fallback: most patterns by ``len(patterns)``
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
) -> dict[str, list[Any]]:
    """Load all ``*_schema.jsonld`` files and group by ``dataset_name``.

    Parameters
    ----------
    schemas_dir:
        Root directory containing schema JSON-LD files (possibly nested).
    dataset_filter:
        Optional list of dataset name globs to include.

    Returns
    -------
    dict mapping dataset name → list of MinedSchema objects.
    """
    from rdfsolve.models import MinedSchema

    by_dataset: dict[str, list[Any]] = defaultdict(list)
    schema_files = sorted(schemas_dir.rglob("*_schema.jsonld"))
    logger.info("Found %d schema files under %s", len(schema_files), schemas_dir)

    for sf in schema_files:
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


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------


def build_schema_graph(
    schemas: list[Any],
) -> tuple[Any, dict[str, set[str]], dict[tuple[str, str], Counter]]:
    """Build G_schema: dataset-level cross-links from schema patterns.

    Returns
    -------
    G : nx.Graph
        Undirected weighted dataset-level graph.
    class_to_datasets : dict[str, set[str]]
        class URI → set of dataset names using it as a subject.
    edge_predicates : dict[tuple[str,str], Counter]
        ``(dataset_a, dataset_b)`` → Counter of predicate URIs.
    """
    import networkx as nx

    G = nx.Graph()
    class_to_datasets: dict[str, set[str]] = defaultdict(set)
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


def build_mapping_graph(
    mapping_paths: list[Path],
    class_to_datasets: dict[str, set[str]],
    base_graph: Any,
    strategies: set[str],
) -> Any:
    """Overlay mapping edges onto *base_graph*.

    Delegates to :meth:`rdfsolve.models.Mapping.dataset_graph`.
    """
    from rdfsolve.models import Mapping

    return Mapping.dataset_graph(
        paths=mapping_paths,
        class_to_datasets=class_to_datasets,
        base_graph=base_graph,
        strategies=strategies,
    )


# ---------------------------------------------------------------------------
# Parquet export helpers
# ---------------------------------------------------------------------------


def _graph_to_edges_df(G: Any) -> Any:
    import pandas as pd

    rows = [
        {"dataset_a": u, "dataset_b": v, "weight": d.get("weight", 1)}
        for u, v, d in G.edges(data=True)
    ]
    return pd.DataFrame(rows, columns=["dataset_a", "dataset_b", "weight"])


def _graph_to_nodes_df(G: Any, comp_map: dict[str, int]) -> Any:
    import pandas as pd

    rows = [
        {
            "dataset": n,
            "pattern_count": G.nodes[n].get("pattern_count", 0),
            "weighted_degree": G.degree(n, weight="weight"),
            "component_id": comp_map.get(n, -1),
        }
        for n in sorted(G.nodes())
    ]
    return pd.DataFrame(rows)


def _component_map(G: Any) -> dict[str, int]:
    import networkx as nx

    comps = sorted(nx.connected_components(G), key=len, reverse=True)
    cmap: dict[str, int] = {}
    for idx, comp in enumerate(comps):
        for node in comp:
            cmap[node] = idx
    return cmap


def export_graphs_to_parquet(
    *,
    G_schema: Any,
    G_raw: Any,
    G_inferred: Any,
    graphs_dir: Path,
) -> None:
    """Write edge and node Parquet tables for all three graph layers."""
    cmap = _component_map(G_inferred)

    for label, G, fname in [
        ("G_schema", G_schema, "edges_schema.parquet"),
        ("G_raw", G_raw, "edges_raw.parquet"),
        ("G_inferred", G_inferred, "edges_inferred.parquet"),
    ]:
        df = _graph_to_edges_df(G)
        out_path = graphs_dir / fname
        df.to_parquet(out_path, index=False)
        logger.info("  %s -> %s  (%d rows)", label, out_path.name, len(df))

    df_nodes = _graph_to_nodes_df(G_inferred, cmap)
    nodes_path = graphs_dir / "nodes.parquet"
    df_nodes.to_parquet(nodes_path, index=False)
    logger.info("  nodes -> %s  (%d rows)", nodes_path.name, len(df_nodes))


# ---------------------------------------------------------------------------
# Strategy / predicate counts from mapping files
# ---------------------------------------------------------------------------


def _compute_mapping_counts(
    mappings_dir: Path,
) -> tuple[dict[str, int], dict[str, int]]:
    """Scan mapping JSON-LD files and return (strategy_counts, predicate_counts)."""
    try:
        import ujson as _json_fast
    except ImportError:
        import json as _json_fast  # type: ignore[assignment]

    _SKIP_KEYS = frozenset({"void:inDataset", "dcterms:created"})

    strategy_counts: dict[str, int] = {}
    predicate_counts: dict[str, int] = {}

    for subdir in ("sssom", "semra", "inferenced"):
        d = mappings_dir / subdir
        if not d.exists():
            continue
        for mf in d.glob("*.jsonld"):
            try:
                raw = _json_fast.loads(mf.read_bytes())
                strategy = raw.get("@about", {}).get("strategy", "unknown")
                for node in raw.get("@graph", ()):
                    for key in node:
                        if key[0] == "@" or key in _SKIP_KEYS:
                            continue
                        val = node[key]
                        targets = val if isinstance(val, list) else (val,)
                        n = sum(
                            1
                            for t in targets
                            if isinstance(t, dict) and t.get("@id")
                        )
                        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + n
                        predicate_counts[key] = predicate_counts.get(key, 0) + n
            except Exception:
                pass

    return strategy_counts, predicate_counts


def _shorten_uri(uri: str) -> str:
    for sep in ("#", "/"):
        idx = uri.rfind(sep)
        if idx >= 0 and idx < len(uri) - 1:
            return uri[idx + 1 :]
    return uri


def export_mapping_counts(
    mappings_dir: Path,
    output_dir: Path,
) -> None:
    """Export strategy_counts.parquet and predicate_counts.parquet."""
    import pandas as pd

    strategy_counts, predicate_counts = _compute_mapping_counts(mappings_dir)

    if strategy_counts:
        strat_df = (
            pd.DataFrame(
                [{"strategy": k, "count": v} for k, v in strategy_counts.items()]
            )
            .sort_values("count", ascending=False)
            .reset_index(drop=True)
        )
        strat_df.to_parquet(output_dir / "strategy_counts.parquet", index=False)
        logger.info(
            "  strategy_counts -> strategy_counts.parquet  (%d rows)", len(strat_df)
        )

    if predicate_counts:
        pred_df = (
            pd.DataFrame(
                [
                    {
                        "predicate": k,
                        "predicate_short": _shorten_uri(k),
                        "count": v,
                    }
                    for k, v in predicate_counts.items()
                ]
            )
            .sort_values("count", ascending=False)
            .reset_index(drop=True)
        )
        pred_df.to_parquet(output_dir / "predicate_counts.parquet", index=False)
        logger.info(
            "  predicate_counts -> predicate_counts.parquet  (%d rows)", len(pred_df)
        )


def export_schema_edge_predicates(
    schema_edge_predicates: dict[tuple[str, str], Counter],
    graphs_dir: Path,
) -> None:
    """Write schema_edge_predicates.parquet from the per-edge predicate counts."""
    import pandas as pd

    rows = [
        {"dataset_a": a, "dataset_b": b, "predicate": pred, "count": cnt}
        for (a, b), counter in schema_edge_predicates.items()
        for pred, cnt in counter.items()
    ]
    if rows:
        df = pd.DataFrame(rows, columns=["dataset_a", "dataset_b", "predicate", "count"])
        df.sort_values(
            ["dataset_a", "dataset_b", "count"],
            ascending=[True, True, False],
            inplace=True,
        )
        out = graphs_dir / "schema_edge_predicates.parquet"
        df.to_parquet(out, index=False)
        logger.info("  schema_edge_predicates -> %s  (%d rows)", out.name, len(df))
    else:
        logger.warning("  schema_edge_predicates: empty — nothing to write")


# ---------------------------------------------------------------------------
# Benchmark logger
# ---------------------------------------------------------------------------


class BenchmarkLog:
    """Append-mode JSONL benchmark logger."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("a", encoding="utf-8")

    def record(self, **kwargs: object) -> None:
        kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        self._fh.write(json.dumps(kwargs, ensure_ascii=False) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


# ---------------------------------------------------------------------------
# Full pipeline entry point
# ---------------------------------------------------------------------------


def run_graph_pipeline(
    schemas_dir: str | Path,
    mappings_dir: str | Path,
    output_dir: str | Path,
    *,
    datasets: list[str] | None = None,
    schema_only: bool = False,
    copy_schemas: bool = True,
) -> dict[str, Any]:
    """Run the full graph-building pipeline (step 4b + 12).

    Parameters
    ----------
    schemas_dir:
        Root directory containing ``*_schema.jsonld`` files.
    mappings_dir:
        Root directory containing ``sssom/``, ``semra/``,
        ``instance_matching/``, ``inferenced/`` subdirectories.
    output_dir:
        Output root for ``graphs/``, ``schemas/``, Parquet, etc.
    datasets:
        Optional list of dataset name globs to restrict processing.
    schema_only:
        If ``True``, run only schema selection (step 4b) and skip
        graph construction (step 12).
    copy_schemas:
        Copy selected schema files to ``output_dir/schemas/``.

    Returns
    -------
    dict with ``metadata`` key (summary) and ``benchmarks_path``.
    """
    import shutil
    import subprocess

    from rdfsolve.version import VERSION

    schemas_dir = Path(schemas_dir)
    mappings_dir = Path(mappings_dir)
    output_dir = Path(output_dir)
    graphs_dir = output_dir / "graphs"
    schemas_out = output_dir / "schemas"

    graphs_dir.mkdir(parents=True, exist_ok=True)
    schemas_out.mkdir(parents=True, exist_ok=True)

    bench = BenchmarkLog(output_dir / "benchmarks.jsonl")

    # ── Step 4b: Schema selection ──────────────────────────────────
    logger.info("═══ Step 4b: Schema selection ═══")
    t0 = time.perf_counter()

    by_dataset = collect_schemas(schemas_dir, dataset_filter=datasets or None)
    if not by_dataset:
        logger.error("No schemas found under %s", schemas_dir)
        raise FileNotFoundError(f"No schemas found under {schemas_dir}")

    selected: dict[str, Any] = {}
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
    bench.record(
        step="schema_selection_total",
        datasets=len(selected),
        elapsed_s=round(t_sel, 2),
    )

    # Copy selected schemas
    if copy_schemas:
        copied = 0
        for ds in selected:
            matches = list(schemas_dir.rglob("*_schema.jsonld"))
            for candidate_path in matches:
                if candidate_path.parent.name == ds or candidate_path.stem.startswith(ds):
                    dest = schemas_out / f"{ds}_schema.jsonld"
                    if not dest.exists():
                        shutil.copy2(candidate_path, dest)
                        copied += 1
                    break
        logger.info("Copied %d schema files -> %s", copied, schemas_out)

    if schema_only:
        logger.info("--schema-only: stopping after selection.")
        bench.close()
        return {"metadata": {"datasets": sorted(selected.keys())}}

    # ── Step 12: Graph construction ────────────────────────────────
    logger.info("═══ Step 12: Graph construction ═══")
    schemas_list = list(selected.values())

    # G_schema
    logger.info("Building G_schema …")
    t0 = time.perf_counter()
    G_schema, class_to_datasets, schema_edge_preds = build_schema_graph(schemas_list)
    t_sch = time.perf_counter() - t0
    logger.info(
        "G_schema — nodes: %d  edges: %d  (%.1fs)",
        G_schema.number_of_nodes(),
        G_schema.number_of_edges(),
        t_sch,
    )
    bench.record(
        step="build_graphs",
        graph="G_schema",
        nodes=G_schema.number_of_nodes(),
        edges=G_schema.number_of_edges(),
        elapsed_s=round(t_sch, 2),
    )

    # G_raw
    SSSOM_DIR = mappings_dir / "sssom"
    SEMRA_DIR = mappings_dir / "semra"
    INST_DIR = mappings_dir / "instance_matching"
    INF_DIR = mappings_dir / "inferenced"

    raw_paths = (
        sorted(SSSOM_DIR.glob("*.jsonld")) if SSSOM_DIR.exists() else []
    ) + (
        sorted(SEMRA_DIR.glob("*.jsonld")) if SEMRA_DIR.exists() else []
    ) + (
        sorted(INST_DIR.glob("*.jsonld")) if INST_DIR.exists() else []
    )

    if raw_paths:
        logger.info("Building G_raw from %d mapping files …", len(raw_paths))
        t0 = time.perf_counter()
        G_raw = build_mapping_graph(
            raw_paths,
            class_to_datasets,
            G_schema.copy(),
            {"sssom_import", "semra_import", "instance_matcher"},
        )
        t_raw = time.perf_counter() - t0
        new_raw = {frozenset(e) for e in G_raw.edges()} - {
            frozenset(e) for e in G_schema.edges()
        }
        logger.info(
            "G_raw — nodes: %d  edges: %d  (+%d new)  (%.1fs)",
            G_raw.number_of_nodes(),
            G_raw.number_of_edges(),
            len(new_raw),
            t_raw,
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
        G_inferred = build_mapping_graph(
            inf_paths,
            class_to_datasets,
            G_raw.copy(),
            {"inferenced"},
        )
        t_inf = time.perf_counter() - t0
        new_inf = {frozenset(e) for e in G_inferred.edges()} - {
            frozenset(e) for e in G_raw.edges()
        }
        logger.info(
            "G_inferred — nodes: %d  edges: %d  (+%d new)  (%.1fs)",
            G_inferred.number_of_nodes(),
            G_inferred.number_of_edges(),
            len(new_inf),
            t_inf,
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
        logger.warning("No inferenced mapping files — G_inferred = G_raw")
        G_inferred = G_raw.copy()

    # ── Export ─────────────────────────────────────────────────────
    logger.info("Exporting Parquet …")
    export_graphs_to_parquet(
        G_schema=G_schema,
        G_raw=G_raw,
        G_inferred=G_inferred,
        graphs_dir=graphs_dir,
    )
    export_schema_edge_predicates(schema_edge_preds, graphs_dir)
    export_mapping_counts(mappings_dir, output_dir)

    # ── metadata.json ──────────────────────────────────────────────
    try:
        git_sha = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        git_sha = "unknown"

    metadata = {
        "rdfsolve_version": VERSION,
        "git_sha": git_sha,
        "run_date": datetime.now(timezone.utc).isoformat(),
        "datasets": sorted(selected.keys()),
        "n_datasets": len(selected),
        "G_schema": {
            "nodes": G_schema.number_of_nodes(),
            "edges": G_schema.number_of_edges(),
        },
        "G_raw": {
            "nodes": G_raw.number_of_nodes(),
            "edges": G_raw.number_of_edges(),
        },
        "G_inferred": {
            "nodes": G_inferred.number_of_nodes(),
            "edges": G_inferred.number_of_edges(),
        },
    }
    meta_path = output_dir / "metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    logger.info("metadata.json written.")

    bench.record(
        step="build_graphs_complete",
        **{k: v for k, v in metadata.items() if k != "datasets"},
    )
    bench.close()

    logger.info("═══ Done ═══  Output: %s", output_dir)
    return {"metadata": metadata, "benchmarks_path": str(bench.path)}
