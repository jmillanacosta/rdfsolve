#!/usr/bin/env python
r"""Unified mining script with four routes.

Routes
------
- **discover**          — find existing VoID descriptions from remote endpoints
- **mine**              — mine schemas from remote SPARQL endpoints
- **local-mine**        — mine schemas from a local QLever endpoint
- **generate-qleverfile** — auto-generate Qleverfiles for local mining

Usage
-----

.. code-block:: bash

    # Discover existing VoID descriptions from remote endpoints
    python scripts/mine_local.py discover

    # Mine schemas from remote endpoints (standard workflow)
    python scripts/mine_local.py mine

    # Generate Qleverfiles for all downloadable sources
    python scripts/mine_local.py generate-qleverfile --data-dir /data/rdf

    # Generate only for 3 smallest (test mode)
    python scripts/mine_local.py generate-qleverfile --data-dir /data/rdf --test

    # Mine schemas from a local QLever endpoint
    python scripts/mine_local.py local-mine \
        --endpoint http://localhost:7001 \
        --name drugbank

    # Local-mine with VoID discovery first
    python scripts/mine_local.py local-mine \
        --endpoint http://localhost:7001 \
        --name drugbank \
        --discover-first

    # Test mode: mine only the 3 smallest sources
    python scripts/mine_local.py local-mine --test

    # Filter sources by name (regex)
    python scripts/mine_local.py mine --filter "chembl|drugbank"

See ``docs/notes/local_mining_plan.md`` for full design.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

# Ensure ``src/`` is importable when running directly.
_repo_root = Path(__file__).resolve().parent.parent
_src = _repo_root / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

_DEFAULT_SOURCES = _repo_root / "data" / "sources.yaml"

# Default base URI template for VoID partition IRIs.
# ``{name}`` is replaced by the dataset name.
_VOID_URI_DEFAULT = (
    "https://jmillanacosta.com/rdfsolve/{name}/mined/"
)

logger = logging.getLogger(__name__)


def _resolve_void_uri_base(
    name: str,
    cli_override: str | None = None,
    entry: Any = None,
) -> str:
    """Return the VoID base URI for a dataset.

    Resolution order:
      1. Explicit CLI ``--void-uri-base`` value.
      2. ``void_uri_base`` field in the sources.yaml entry.
      3. Default template
         ``https://jmillanacosta.com/rdfsolve/{name}/mined/``.
    """
    if cli_override:
        return cli_override.rstrip("/") + "/"
    if entry and entry.get("void_uri_base"):
        return str(entry["void_uri_base"]).rstrip("/") + "/"
    return _VOID_URI_DEFAULT.format(name=name)

# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════


def _build_parser() -> argparse.ArgumentParser:
    top = argparse.ArgumentParser(
        description="Unified mining script: discover / mine / local-mine.",
    )
    top.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )
    sub = top.add_subparsers(dest="route", required=True)

    # ── shared options ────────────────────────────────────────────
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--sources", default=str(_DEFAULT_SOURCES),
        help=(
            "Path to sources YAML / JSON-LD / CSV "
            "(default: data/sources.yaml)"
        ),
    )
    common.add_argument(
        "--output-dir",
        default=str(_repo_root / "mined_schemas"),
        help="Directory for output files (default: mined_schemas/)",
    )
    common.add_argument(
        "--format", choices=["jsonld", "void", "all"],
        default="all", dest="fmt",
        help="Export format (default: all)",
    )
    common.add_argument(
        "--timeout", type=float, default=120.0,
        help="HTTP timeout per SPARQL request (seconds, default: 120)",
    )
    common.add_argument(
        "--filter", default=None, dest="name_filter",
        help=(
            "Regex pattern to select sources by name "
            "(e.g. 'chembl|drugbank')"
        ),
    )
    common.add_argument(
        "--benchmark", action="store_true",
        help="Collect per-run benchmarks (timing, memory, CPU, disk)",
    )

    # ── mining options (shared by mine + local-mine) ──────────────
    mining = argparse.ArgumentParser(add_help=False)
    mining.add_argument(
        "--chunk-size", type=int, default=10_000,
        help="SPARQL pagination page size (default: 10000)",
    )
    mining.add_argument(
        "--class-batch-size", type=int, default=15,
        help="Classes per VALUES query in two-phase mining (default: 15)",
    )
    mining.add_argument(
        "--no-counts", action="store_true",
        help="Skip triple-count queries (faster)",
    )

    # ── Route: discover ───────────────────────────────────────────
    sub.add_parser(
        "discover", parents=[common],
        help="Discover VoID descriptions from remote endpoints",
    )

    # ── Route: mine ───────────────────────────────────────────────
    sub.add_parser(
        "mine", parents=[common, mining],
        help="Mine schemas from remote SPARQL endpoints",
    )

    # ── Route: local-mine ─────────────────────────────────────────
    lm = sub.add_parser(
        "local-mine", parents=[common, mining],
        help="Mine schemas from a local QLever endpoint",
    )
    lm.add_argument(
        "--endpoint", default="http://localhost:7001",
        help="Local QLever SPARQL endpoint URL (default: http://localhost:7001)",
    )
    lm.add_argument(
        "--name", default=None,
        help="Dataset name (required for single-dataset mode, ignored in batch)",
    )
    lm.add_argument(
        "--discover-first", action="store_true",
        help="Run VoID discovery before mining; save as *_discovered_void.ttl",
    )
    lm.add_argument(
        "--void-uri-base", default=None,
        help=(
            "Base URI for generated VoID partition IRIs "
            "(default: first graph_uri + /void, or endpoint + /void)"
        ),
    )
    lm.add_argument(
        "--test", action="store_true",
        help="Test mode: process only the 3 smallest downloadable sources",
    )

    # ── Route: generate-qleverfile ────────────────────────────────
    gq = sub.add_parser(
        "generate-qleverfile", parents=[common],
        help="Generate Qleverfiles for local QLever mining",
    )
    gq.add_argument(
        "--data-dir", required=True,
        help="Root directory where RDF dumps live (required)",
    )
    gq.add_argument(
        "--base-port", type=int, default=7019,
        help="First port number for allocation (default: 7019)",
    )
    gq.add_argument(
        "--test", action="store_true",
        help="Generate only for 3 smallest downloadable sources",
    )
    gq.add_argument(
        "--runtime", choices=["docker", "native"], default="docker",
        help="QLever runtime: docker or native (default: docker)",
    )

    return top


# ═══════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════


def _load_entries(sources_path: str, name_filter: str | None = None):
    """Load source entries, optionally filtered by name regex."""
    from rdfsolve.sources import load_sources

    entries = load_sources(sources_path)
    if name_filter:
        pat = re.compile(name_filter, re.IGNORECASE)
        entries = [e for e in entries if pat.search(e.get("name", ""))]
    return entries


def _get_bench(args: argparse.Namespace):
    """Return a BenchmarkCollector if --benchmark is set, else None."""
    if not getattr(args, "benchmark", False):
        return None
    from rdfsolve.tools.benchmark import BenchmarkCollector
    return BenchmarkCollector(output_dir=Path(args.output_dir))


def _embed_benchmark_in_report(
    run,   # RunMetrics — import-free signature
    bench,  # BenchmarkCollector
    report_path: Path,
) -> None:
    """Merge benchmark data into the mining report JSON.

    Reads the report written by the miner, adds ``benchmark``
    and ``machine`` top-level keys, and rewrites the file.
    """
    if not report_path.exists():
        return
    try:
        from dataclasses import asdict
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report["benchmark"] = asdict(run)
        report["machine"] = asdict(bench.machine_info)
        report_path.write_text(
            json.dumps(report, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("Could not embed benchmark in report: %s", exc)


def _progress(name: str, idx: int, total: int, status: str) -> None:
    """Print a progress line."""
    symbol = {"OK": "✓", "FAIL": "✗", "SKIP": "–", "DISCOVER": "◈"}.get(
        status.split(":")[0], "·"
    )
    print(f"  [{idx}/{total}] {symbol}  {name}  {status}")


# ═══════════════════════════════════════════════════════════════════
# Route: discover
# ═══════════════════════════════════════════════════════════════════


def _route_discover(args: argparse.Namespace) -> dict[str, Any]:
    """Discover VoID graphs from remote endpoints."""
    from rdfsolve.api import discover_void_graphs
    from rdfsolve.parser import VoidParser
    from rdfsolve.tools.benchmark import (
        collect_machine_info, _read_proc_io, _get_rusage,
    )
    import time as _time

    entries = _load_entries(args.sources, args.name_filter)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    bench = _get_bench(args)
    machine = collect_machine_info()

    discovered: list[str] = []
    empty: list[str] = []
    failed: list[dict[str, str]] = []
    skipped: list[str] = []

    total = len(entries)
    for idx, entry in enumerate(entries, 1):
        name = entry.get("name", "")
        endpoint = entry.get("endpoint", "")

        if not endpoint:
            _progress(name, idx, total, "SKIP: no endpoint")
            skipped.append(name)
            continue

        def _do_discover():
            # Capture resource usage around discovery
            t0 = _time.monotonic()
            cpu0_u, cpu0_s, _ = _get_rusage()
            io0 = _read_proc_io()

            result = discover_void_graphs(
                endpoint, exclude_graphs=False,
            )
            partitions = result.get("partitions", [])

            # Resource snapshot after
            t1 = _time.monotonic()
            cpu1_u, cpu1_s, peak_rss = _get_rusage()
            io1 = _read_proc_io()

            resource_usage = {
                "wall_time_s": round(t1 - t0, 3),
                "cpu_user_s": round(cpu1_u - cpu0_u, 3),
                "cpu_system_s": round(cpu1_s - cpu0_s, 3),
                "peak_rss_mb": round(peak_rss, 2),
                "read_bytes": (
                    io1.get("read_bytes", 0)
                    - io0.get("read_bytes", 0)
                ),
                "write_bytes": (
                    io1.get("write_bytes", 0)
                    - io0.get("write_bytes", 0)
                ),
            }

            if partitions:
                found_graphs = result.get("found_graphs", [])
                base_uri = _resolve_void_uri_base(
                    name, entry=entry,
                )
                parser = VoidParser()
                void_graph = (
                    parser.build_void_graph_from_partitions(
                        partitions, base_uri=base_uri,
                    )
                )

                # ── Export: VoID (Turtle) ────────────────────────
                _tag = "discovered"
                void_path = out / f"{name}_{_tag}_void.ttl"
                void_graph.serialize(
                    destination=str(void_path), format="turtle",
                )

                # ── Export: JSON-LD ──────────────────────────────
                from rdfsolve.api import graph_to_jsonld
                jsonld_doc = graph_to_jsonld(
                    void_graph,
                    endpoint_url=endpoint,
                    dataset_name=name,
                )
                jsonld_path = out / f"{name}_{_tag}_schema.jsonld"
                with open(jsonld_path, "w", encoding="utf-8") as jf:
                    json.dump(jsonld_doc, jf, indent=2)

                # ── Export: LinkML ───────────────────────────────
                export_parser = VoidParser(void_source=void_graph)
                try:
                    linkml_yaml = export_parser.to_linkml_yaml(
                        filter_void_nodes=True,
                        schema_name=name,
                    )
                    linkml_path = out / f"{name}_{_tag}_linkml.yaml"
                    with open(linkml_path, "w", encoding="utf-8") as lf:
                        lf.write(linkml_yaml)
                except Exception as exc:
                    logger.debug("LinkML export failed for %s: %s", name, exc)

                # ── Export: SHACL ────────────────────────────────
                try:
                    shacl_ttl = export_parser.to_shacl(
                        filter_void_nodes=True,
                        schema_name=name,
                    )
                    shacl_path = out / f"{name}_{_tag}_shacl.ttl"
                    with open(shacl_path, "w", encoding="utf-8") as sf:
                        sf.write(shacl_ttl)
                except Exception as exc:
                    logger.debug("SHACL export failed for %s: %s", name, exc)

                # ── Export: RDF-config ───────────────────────────
                try:
                    rdfconfig = export_parser.to_rdfconfig(
                        filter_void_nodes=True,
                        endpoint_url=endpoint,
                        endpoint_name=name,
                    )
                    config_dir = out / f"{name}_{_tag}_config"
                    config_dir.mkdir(parents=True, exist_ok=True)
                    for fname, content in rdfconfig.items():
                        with open(config_dir / f"{fname}.yaml", "w") as rf:
                            rf.write(content)
                except Exception as exc:
                    logger.debug("RDF-config export failed for %s: %s", name, exc)

                meta_path = (
                    out / f"{name}_{_tag}_report.json"
                )
                from dataclasses import asdict
                meta = {
                    "dataset": name,
                    "endpoint": endpoint,
                    "source": "discovered",
                    "graphs_found": len(found_graphs),
                    "partitions_found": len(partitions),
                    "void_file": str(void_path),
                    "machine": asdict(machine),
                    "benchmark": resource_usage,
                }
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2)

                _progress(
                    name, idx, total,
                    f"DISCOVER: {len(partitions)} partitions "
                    f"in {len(found_graphs)} graphs "
                    f"→ {void_path.name}",
                )
                discovered.append(name)
                return len(partitions)
            else:
                _progress(
                    name, idx, total,
                    "OK: no VoID partitions found",
                )
                empty.append(name)
                return 0

        try:
            if bench:
                with bench.track(
                    name, method="discover", endpoint=endpoint,
                ) as run:
                    n = _do_discover()
                    run.add_extra("partitions_found", n)
            else:
                _do_discover()

        except Exception as exc:
            msg = str(exc)[:120]
            _progress(name, idx, total, f"FAIL: {msg}")
            failed.append({"dataset": name, "error": msg})

    return {
        "discovered": discovered,
        "empty": empty,
        "failed": failed,
        "skipped": skipped,
    }


# ═══════════════════════════════════════════════════════════════════
# Route: mine
# ═══════════════════════════════════════════════════════════════════


def _route_mine(args: argparse.Namespace) -> dict[str, Any]:
    """Mine schemas from remote endpoints (wraps mine_all_sources)."""
    from rdfsolve.api import mine_all_sources

    def _on_progress(
        name: str, idx: int, total: int, error: str | None,
    ) -> None:
        if error == "skipped":
            _progress(name, idx, total, "SKIP: no endpoint")
        elif error:
            _progress(name, idx, total, f"FAIL: {error[:120]}")
        else:
            _progress(name, idx, total, "OK")

    # If a name filter is given, we need to create a filtered sources
    # file. mine_all_sources accepts a path, not entries, so we write
    # a temp YAML when filtering.
    sources_path = args.sources
    if args.name_filter:
        entries = _load_entries(args.sources, args.name_filter)
        if not entries:
            print("No sources match the filter.")
            return {"succeeded": [], "failed": [], "skipped": []}

        import tempfile
        import yaml

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, prefix="rdfsolve_filtered_",
        )
        yaml.dump(
            [dict(e) for e in entries], tmp,
            default_flow_style=False, allow_unicode=True,
        )
        tmp.close()
        sources_path = tmp.name

    result = mine_all_sources(
        sources=sources_path,
        output_dir=args.output_dir,
        fmt=args.fmt,
        chunk_size=args.chunk_size,
        class_batch_size=args.class_batch_size,
        timeout=args.timeout,
        counts=not args.no_counts,
        reports=True,
        on_progress=_on_progress,
    )
    return result


# ═══════════════════════════════════════════════════════════════════
# Route: local-mine
# ═══════════════════════════════════════════════════════════════════


def _select_test_sources(entries: list) -> list:
    """Pick the 3 smallest downloadable sources for --test mode.

    Selects sources that have any download_* field.  Since we don't
    know actual file sizes without downloading, we use a heuristic:
    prefer sources whose download URLs point to single files (not
    multi-file lists) and sort alphabetically as a tie-breaker.
    """
    downloadable = []
    for e in entries:
        has_download = any(
            k.startswith("download_") and e.get(k)
            for k in e
        )
        if has_download:
            # Heuristic: single-file downloads (string, not list) are likely smaller
            download_fields = [
                k for k in e if k.startswith("download_") and e.get(k)
            ]
            is_single = all(isinstance(e[k], str) for k in download_fields)
            downloadable.append((is_single, e.get("name", ""), e))

    # Sort: single-file first, then alphabetically
    downloadable.sort(key=lambda t: (not t[0], t[1]))
    selected = [t[2] for t in downloadable[:3]]

    if not selected:
        logger.warning("No downloadable sources found for --test mode")
    else:
        names = [s.get("name", "?") for s in selected]
        logger.info("Test mode: selected %s", names)

    return selected


def _discover_void_for_local(
    endpoint: str,
    name: str,
    out: Path,
    timeout: float,
    void_uri_base: str | None = None,
) -> dict[str, Any] | None:
    """Run VoID discovery against a local endpoint.

    Returns the discovery result dict, or None if no partitions found.

    Args:
        void_uri_base: Resolved VoID base URI (from
            ``_resolve_void_uri_base``).
    """
    from rdfsolve.api import discover_void_graphs
    from rdfsolve.parser import VoidParser

    result = discover_void_graphs(endpoint, exclude_graphs=False)
    partitions = result.get("partitions", [])

    if not partitions:
        return None

    found_graphs = result.get("found_graphs", [])
    base_uri = void_uri_base or _VOID_URI_DEFAULT.format(name=name)
    parser = VoidParser()
    void_graph = parser.build_void_graph_from_partitions(
        partitions, base_uri=base_uri,
    )

    # ── Export: VoID (Turtle) ────────────────────────────
    _tag = "discovered_qlever"
    void_path = out / f"{name}_{_tag}_void.ttl"
    void_graph.serialize(destination=str(void_path), format="turtle")

    # ── Export: JSON-LD ──────────────────────────────────
    from rdfsolve.api import graph_to_jsonld
    jsonld_doc = graph_to_jsonld(
        void_graph, endpoint_url=endpoint, dataset_name=name,
    )
    jsonld_path = out / f"{name}_{_tag}_schema.jsonld"
    with open(jsonld_path, "w", encoding="utf-8") as jf:
        json.dump(jsonld_doc, jf, indent=2)

    # ── Export: LinkML ───────────────────────────────────
    export_parser = VoidParser(void_source=void_graph)
    try:
        linkml_yaml = export_parser.to_linkml_yaml(
            filter_void_nodes=True, schema_name=name,
        )
        linkml_path = out / f"{name}_{_tag}_linkml.yaml"
        with open(linkml_path, "w", encoding="utf-8") as lf:
            lf.write(linkml_yaml)
    except Exception:
        pass

    # ── Export: SHACL ────────────────────────────────────
    try:
        shacl_ttl = export_parser.to_shacl(
            filter_void_nodes=True, schema_name=name,
        )
        shacl_path = out / f"{name}_{_tag}_shacl.ttl"
        with open(shacl_path, "w", encoding="utf-8") as sf:
            sf.write(shacl_ttl)
    except Exception:
        pass

    # ── Export: RDF-config ───────────────────────────────
    try:
        rdfconfig = export_parser.to_rdfconfig(
            filter_void_nodes=True,
            endpoint_url=endpoint, endpoint_name=name,
        )
        config_dir = out / f"{name}_{_tag}_config"
        config_dir.mkdir(parents=True, exist_ok=True)
        for fname, content in rdfconfig.items():
            with open(config_dir / f"{fname}.yaml", "w") as rf:
                rf.write(content)
    except Exception:
        pass

    meta_path = out / f"{name}_{_tag}_report.json"
    meta = {
        "dataset": name,
        "endpoint": endpoint,
        "source": "discovered",
        "graphs_found": len(found_graphs),
        "partitions_found": len(partitions),
        "void_file": str(void_path),
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(
        "%s: discovered %d partitions in %d graphs",
        name, len(partitions), len(found_graphs),
    )
    return result


def _mine_single_local(
    endpoint: str,
    name: str,
    out: Path,
    args: argparse.Namespace,
    entry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Mine a single dataset from a local endpoint.

    Returns a dict with ``classes``, ``properties``, and output
    file paths — useful for populating benchmark run metrics.
    """
    from rdfsolve.miner import mine_schema as _mine

    _tag = "mined_qlever"
    rpt_path = out / f"{name}_{_tag}_report.json"

    schema = _mine(
        endpoint_url=endpoint,
        dataset_name=name,
        chunk_size=args.chunk_size,
        class_batch_size=args.class_batch_size,
        timeout=args.timeout,
        counts=not args.no_counts,
        two_phase=True,
        report_path=rpt_path,
        filter_service_namespaces=True,
    )

    # Override the endpoint used for VoID / JSON-LD export URIs
    # so localhost:PORT doesn't leak into the output.
    resolved = _resolve_void_uri_base(
        name,
        cli_override=getattr(args, "void_uri_base", None),
        entry=entry,
    )
    schema.about.endpoint = resolved.rstrip("/")

    result_files: dict[str, str] = {
        "report": str(rpt_path),
    }

    if args.fmt in ("jsonld", "all"):
        jsonld_path = out / f"{name}_{_tag}_schema.jsonld"
        with open(jsonld_path, "w", encoding="utf-8") as f:
            json.dump(schema.to_jsonld(), f, indent=2)
        logger.info("  → %s", jsonld_path)
        result_files["schema_jsonld"] = str(jsonld_path)

    if args.fmt in ("void", "all"):
        void_path = out / f"{name}_{_tag}_void.ttl"
        void_g = schema.to_void_graph()
        void_g.serialize(
            destination=str(void_path), format="turtle",
        )
        logger.info(
            "  → %s (%d triples)", void_path, len(void_g),
        )
        result_files["void_ttl"] = str(void_path)

    return {
        "classes": len(schema.get_classes()),
        "properties": len(schema.get_properties()),
        "files": result_files,
    }


def _route_local_mine(args: argparse.Namespace) -> dict[str, Any]:
    """Mine schemas from a local QLever endpoint."""
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    bench = _get_bench(args)

    def _do_mine_one(
        name: str, endpoint: str,
        idx: int, total: int,
        entry: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run discover + mine for one dataset. Returns mine result."""
        if args.discover_first:
            print(f"  Discovering VoID at {endpoint} …")
            void_uri = _resolve_void_uri_base(
                name,
                cli_override=getattr(args, "void_uri_base", None),
                entry=entry,
            )
            disc = _discover_void_for_local(
                endpoint, name, out, args.timeout,
                void_uri_base=void_uri,
            )
            if disc:
                n = len(disc.get("partitions", []))
                _progress(
                    name, idx, total,
                    f"DISCOVER: {n} partitions",
                )
            else:
                _progress(
                    name, idx, total,
                    "DISCOVER: no VoID found",
                )

        mine_result = _mine_single_local(
            endpoint, name, out, args, entry=entry,
        )
        _progress(name, idx, total, "OK")
        return mine_result

    # ── Single-dataset mode (--name given) ────────────────────────
    if args.name and not args.test:
        name = args.name
        endpoint = args.endpoint

        print(f"Local-mine: {name} @ {endpoint}")

        succeeded: list[str] = []
        failed: list[dict[str, str]] = []

        try:
            if bench:
                with bench.track(
                    name, method="local-mine",
                    endpoint=endpoint,
                ) as run:
                    mr = _do_mine_one(name, endpoint, 1, 1)
                    run.classes_found = mr["classes"]
                    run.properties_found = mr["properties"]
                    run.output_files = mr["files"]
                rpt = out / f"{name}_mined_qlever_report.json"
                _embed_benchmark_in_report(run, bench, rpt)
            else:
                _do_mine_one(name, endpoint, 1, 1)
            succeeded.append(name)

        except Exception as exc:
            msg = str(exc)[:120]
            _progress(name, 1, 1, f"FAIL: {msg}")
            failed.append({"dataset": name, "error": str(exc)})

        return {
            "succeeded": succeeded,
            "failed": failed,
            "skipped": [],
        }

    # ── Batch mode (iterate sources, optionally --test) ───────────
    entries = _load_entries(args.sources, args.name_filter)

    if args.test:
        entries = _select_test_sources(entries)
        if not entries:
            print("No downloadable sources for test mode.")
            return {
                "succeeded": [], "failed": [], "skipped": [],
            }

    batch_entries = []
    for e in entries:
        ep = e.get("local_endpoint", "")
        if ep:
            batch_entries.append((e, ep))
        elif args.test:
            batch_entries.append((e, args.endpoint))

    if not batch_entries:
        print(
            "No sources with 'local_endpoint' found.\n"
            "Hint: use --name + --endpoint for single mode,\n"
            "      or add 'local_endpoint' to sources.yaml."
        )
        return {
            "succeeded": [], "failed": [], "skipped": [],
        }

    succeeded = []
    failed = []
    total = len(batch_entries)

    for idx, (entry, endpoint) in enumerate(batch_entries, 1):
        name = entry.get("name", "")
        print(f"\n{'─'*60}")
        print(f"[{idx}/{total}] {name} @ {endpoint}")

        try:
            if bench:
                with bench.track(
                    name, method="local-mine",
                    endpoint=endpoint,
                ) as run:
                    mr = _do_mine_one(
                        name, endpoint, idx, total,
                        entry=entry,
                    )
                    run.classes_found = mr["classes"]
                    run.properties_found = mr["properties"]
                    run.output_files = mr["files"]
                rpt = out / f"{name}_mined_qlever_report.json"
                _embed_benchmark_in_report(run, bench, rpt)
            else:
                _do_mine_one(
                    name, endpoint, idx, total,
                    entry=entry,
                )
            succeeded.append(name)

        except Exception as exc:
            msg = str(exc)[:120]
            _progress(name, idx, total, f"FAIL: {msg}")
            failed.append({"dataset": name, "error": str(exc)})

    # Write benchmark summary CSV if collecting
    if bench:
        csv_path = bench.write_summary_csv()
        print(f"\nBenchmark summary → {csv_path}")

    return {
        "succeeded": succeeded,
        "failed": failed,
        "skipped": [],
    }


# ═══════════════════════════════════════════════════════════════════
# Route: generate-qleverfile
# ═══════════════════════════════════════════════════════════════════

# Qleverfile template — INI-style config consumed by `qlever` CLI.
_QLEVERFILE_TEMPLATE = """\
# Qleverfile for {name}
# Auto-generated by rdfsolve  (scripts/mine_local.py generate-qleverfile)
#
# Usage:
#   cd {workdir}
#   qlever index          # build index from the RDF files
#   qlever start          # start SPARQL endpoint on port {port}
#   qlever stop           # stop the endpoint
#   # then:
#   python scripts/mine_local.py local-mine \\
#       --endpoint http://localhost:{port} --name {name}

[data]
NAME              = {name}
GET_DATA_CMD      = {get_data_cmd}
FORMAT            = {rdf_format}
DESCRIPTION       = {name} — local RDF dataset for rdfsolve schema mining

[index]
INPUT_FILES       = {input_files}
CAT_INPUT_FILES   = {cat_input_files}
SETTINGS_JSON     = {settings_json}
PARALLEL_PARSING  = false

[server]
PORT              = {port}
ACCESS_TOKEN      = {access_token}
MEMORY_FOR_QUERIES = 5G
TIMEOUT           = 120s

[runtime]
SYSTEM = {runtime}
IMAGE  = docker.io/adfreiburg/qlever:latest

[ui]
UI_CONFIG = default
"""


def _detect_data_format(entry: Any) -> str | None:
    """Return ``True``-ish format string if the entry has any download field.

    The return value is a short label used for logging only.  The actual
    Qleverfile construction is handled generically by
    ``_build_qleverfile``.

    Returns ``None`` when the entry has no ``download_*`` fields at all.
    """
    dl_keys = [k for k in entry if k.startswith("download_") and entry.get(k)]
    if not dl_keys:
        return None
    # Return the most descriptive key (prefer data over schema files)
    _PRIORITY = [
        "download_nq", "download_nquads", "download_nt", "download_n3",
        "download_ttl", "download_rdf", "download_rdfxml", "download_owl",
        "download_jsonld", "download_zip", "download_tar_gz", "download_tgz",
        "download_ftp",
    ]
    for k in _PRIORITY:
        if k in dl_keys:
            return k.removeprefix("download_")
    return dl_keys[0].removeprefix("download_")


# ── Generic download helpers ─────────────────────────────────────

def _urls_from_field(entry: dict, field: str) -> list[str]:
    """Extract a flat URL list from a YAML field (string or list)."""
    raw = entry.get(field, "")
    if not raw:
        return []
    urls = raw if isinstance(raw, list) else [raw]
    return [u for u in urls if u]


# Map download_* suffix → (QLever FORMAT, input glob, cat command)
# for formats that QLever can consume directly.
_DIRECT_FORMATS: dict[str, tuple[str, str, str]] = {
    # N-Quads
    "nq":      ("nq",  "*.nq.gz *.nq",    "zcat *.nq.gz 2>/dev/null; cat *.nq 2>/dev/null"),
    "nquads":  ("nq",  "*.nq.gz *.nq",    "zcat *.nq.gz 2>/dev/null; cat *.nq 2>/dev/null"),
    # Turtle
    "ttl":     ("ttl", "*.ttl",            "cat *.ttl"),
    # N-Triples
    "nt":      ("nt",  "*.nt.gz *.nt",     "zcat *.nt.gz 2>/dev/null; cat *.nt 2>/dev/null"),
    # N3 (QLever reads as turtle superset)
    "n3":      ("ttl", "*.n3",             "cat *.n3"),
    # OWL (typically RDF/XML)
    "owl":     ("ttl", "*.ttl",            "cat *.ttl"),
    # RDF/XML (.rdf, .rdf.xz)
    "rdf":     ("ttl", "*.ttl",            "cat *.ttl"),
    "rdfxml":  ("ttl", "*.ttl",            "cat *.ttl"),
    # JSON-LD
    "jsonld":  ("ttl", "*.ttl",            "cat *.ttl"),
}


def _build_qleverfile(
    entry: Any,
    data_dir: Path,
    port: int,
    runtime: str,
) -> str:
    """Build Qleverfile content string for one source entry.

    Handles ALL ``download_*`` fields generically:
      - Collects URLs from every ``download_*`` field
      - Downloads with ``wget -c -q``
      - Decompresses ``.gz`` / ``.xz`` archives
      - Extracts ``.tar.gz`` / ``.tgz`` / ``.zip`` archives
      - Converts RDF/XML and OWL → Turtle (via rapper)
      - Converts JSON-LD → Turtle (via Python rdflib)
    """
    name = entry.get("name", "unknown")

    workdir = (data_dir / "qlever_workdirs" / name).resolve()
    rdf_subdir = "rdf"  # relative to workdir
    src_data_dir = f"{workdir}/{rdf_subdir}"  # absolute, for cmds

    settings_json = (
        '{ "ascii-prefixes-only": false, '
        '"num-triples-per-batch": 1000000 }'
    )

    # ── Collect ALL download URLs, grouped by type ────────────────
    dl_keys = sorted(k for k in entry if k.startswith("download_") and entry.get(k))
    if not dl_keys:
        raise ValueError(f"Source '{name}' has no download_* fields")

    all_urls: list[str] = []
    has_gz = False          # any .gz files to decompress
    has_xz = False          # any .xz files to decompress
    has_archive = False     # any .zip / .tar.gz / .tgz to extract
    has_rdfxml = False      # need RDF/XML → Turtle conversion
    has_jsonld = False      # need JSON-LD → Turtle conversion
    has_nq = False          # primary format is N-Quads
    has_nt = False          # primary format is N-Triples
    has_n3 = False          # primary format is N3

    for dk in dl_keys:
        suffix = dk.removeprefix("download_")
        urls = _urls_from_field(entry, dk)
        all_urls.extend(urls)

        for u in urls:
            low = u.lower()
            if low.endswith(".gz") and not low.endswith(".tar.gz"):
                has_gz = True
            if low.endswith(".xz"):
                has_xz = True

        if suffix in ("tar_gz", "tgz", "zip"):
            has_archive = True
        if suffix in ("rdf", "rdfxml", "owl"):
            has_rdfxml = True
        if suffix == "jsonld":
            has_jsonld = True
        if suffix in ("nq", "nquads"):
            has_nq = True
        if suffix == "nt":
            has_nt = True
        if suffix == "n3":
            has_n3 = True

    # ── Decide QLever FORMAT and INPUT_FILES / CAT_INPUT_FILES ────
    if has_nq:
        rdf_format = "nq"
        input_files = f"{rdf_subdir}/*.nq*"
        cat_input_files = (
            "( zcat ${INPUT_FILES} 2>/dev/null || "
            "cat ${INPUT_FILES} 2>/dev/null ) | "
            "grep -v '^$$' || true"
        )
    elif has_nt:
        rdf_format = "nt"
        input_files = f"{rdf_subdir}/*.nt*"
        cat_input_files = (
            "( zcat ${INPUT_FILES} 2>/dev/null || "
            "cat ${INPUT_FILES} 2>/dev/null ) | "
            "grep -v '^$$' || true"
        )
    elif has_n3:
        rdf_format = "ttl"
        input_files = f"{rdf_subdir}/*.n3"
        cat_input_files = "cat ${INPUT_FILES}"
    else:
        # Everything else is converted / decompressed to .ttl
        rdf_format = "ttl"
        input_files = f"{rdf_subdir}/*.ttl"
        cat_input_files = "cat ${INPUT_FILES}"

    # ── Build GET_DATA_CMD ────────────────────────────────────────
    # Build wget commands.  For URLs that don't end with a
    # recognisable RDF filename (e.g. Zenodo API "/content"
    # endpoints) we use --content-disposition so wget saves
    # the file under the server-suggested name, or fall back
    # to -O with a name derived from the URL path.
    wget_parts: list[str] = []
    _RDF_EXTS = (
        ".ttl", ".ttl.gz", ".nt", ".nt.gz", ".nq", ".nq.gz",
        ".n3", ".owl", ".rdf", ".rdf.gz", ".rdf.xz", ".owl.xz",
        ".xml.gz", ".jsonld", ".tar.gz", ".tgz", ".zip",
    )
    for u in all_urls:
        fname = u.rsplit("/", 1)[-1]
        if any(fname.lower().endswith(ext) for ext in _RDF_EXTS):
            wget_parts.append(f'wget -c -q "{u}"')
        else:
            # Try to derive a filename from the URL path
            # e.g. .../files/Hsa-u.c4-0.n3/content → Hsa-u.c4-0.n3
            parts = u.rstrip("/").split("/")
            derived = next(
                (p for p in reversed(parts)
                 if any(p.lower().endswith(e) for e in _RDF_EXTS)),
                None,
            )
            if derived:
                wget_parts.append(f'wget -c -q -O "{derived}" "{u}"')
            else:
                wget_parts.append(
                    f'wget -c -q --content-disposition "{u}"'
                )

    wget_lines = " && ".join(wget_parts)
    steps: list[str] = [
        f"mkdir -p {src_data_dir}",
        f"cd {src_data_dir}",
        wget_lines,
    ]

    # Extract archives (.zip / .tar.gz / .tgz)
    if has_archive:
        steps.append("echo 'Extracting archives …'")
        # tar.gz / tgz
        steps.append(
            'for f in *.tar.gz *.tgz; do '
            '[ -f "$f" ] || continue; '
            'echo "  extracting $f"; '
            'tar xzf "$f"; '
            'done'
        )
        # zip
        steps.append(
            'for f in *.zip; do '
            '[ -f "$f" ] || continue; '
            'echo "  extracting $f"; '
            'python3 -c "import zipfile; z=zipfile.ZipFile(\'$f\'); z.extractall(\'.\'); '
            "print(f'Extracted {len(z.namelist())} files'); z.close()\"; "
            'done'
        )
        # After extraction, find RDF files in subdirectories and move them up
        steps.append(
            "echo 'Collecting RDF files from subdirectories …' "
            "&& find . -mindepth 2 \\( "
            '-name "*.ttl" -o -name "*.ttl.gz" -o -name "*.nt" -o '
            '-name "*.nt.gz" -o -name "*.nq" -o -name "*.nq.gz" -o '
            '-name "*.n3" -o -name "*.owl" -o -name "*.rdf" -o '
            '-name "*.rdf.gz" -o -name "*.jsonld" '
            "\\) -exec mv -n {} . \\; 2>/dev/null || true"
        )

    # Decompress .xz files
    if has_xz:
        steps.append("echo 'Decompressing .xz files …'")
        steps.append(
            'for f in *.xz; do '
            '[ -f "$f" ] || continue; '
            'xz -dk "$f" 2>/dev/null || true; '
            'done'
        )

    # Decompress .gz files (but not .tar.gz)
    if has_gz:
        steps.append("echo 'Decompressing .gz files …'")
        # For N-Quads and N-Triples, QLever can read .gz directly,
        # so only decompress TTL/OWL/RDF/N3/JSONLD .gz files.
        if has_nq or has_nt:
            # Leave .nq.gz / .nt.gz compressed — QLever reads them.
            # Only decompress other .gz files (ttl, owl, rdf, etc.)
            steps.append(
                'for f in *.ttl.gz *.owl.gz *.rdf.gz *.n3.gz *.jsonld.gz; do '
                '[ -f "$f" ] || continue; '
                'gunzip -fk "$f" 2>/dev/null || true; '
                'done'
            )
        else:
            steps.append(
                'for f in *.gz; do '
                '[ -f "$f" ] || continue; '
                'case "$f" in *.tar.gz) continue;; esac; '
                'gunzip -fk "$f" 2>/dev/null || true; '
                'done'
            )

    # Convert RDF/XML and OWL → Turtle
    if has_rdfxml:
        steps.append("echo 'Converting RDF/XML → Turtle …'")
        steps.append(
            'for f in *.rdf *.owl *.xml; do '
            '[ -f "$f" ] || continue; '
            'out="${f%.*}.ttl"; '
            '[ -f "$out" ] && continue; '
            'echo "  $f → $out"; '
            'rapper -i rdfxml -o turtle "$f" > "$out" 2>/dev/null || '
            'echo "  WARNING: rapper failed on $f"; '
            'done'
        )

    # Convert JSON-LD → Turtle
    if has_jsonld:
        steps.append("echo 'Converting JSON-LD → Turtle …'")
        steps.append(
            'for f in *.jsonld; do '
            '[ -f "$f" ] || continue; '
            'out="${f%.*}.ttl"; '
            '[ -f "$out" ] && continue; '
            'echo "  $f → $out"; '
            "python3 -c \""
            "from rdflib import Graph; "
            "g = Graph(); "
            "g.parse('$f', format='json-ld'); "
            "g.serialize('$out', format='turtle')"
            '" 2>/dev/null || '
            'echo "  WARNING: rdflib conversion failed on $f"; '
            'done'
        )

    get_data_cmd = " && ".join(steps)

    # QLever's INI parser treats $ as variable reference.
    # Escape bare $ (but not ${...} which are QLever variables)
    # by replacing $X with $$X where X is not '{'.
    get_data_cmd = re.sub(r'\$(?!\{)', '$$', get_data_cmd)

    return _QLEVERFILE_TEMPLATE.format(
        name=name,
        workdir=workdir,
        port=port,
        rdf_format=rdf_format,
        input_files=input_files,
        cat_input_files=cat_input_files,
        get_data_cmd=get_data_cmd,
        settings_json=settings_json,
        access_token=name,
        runtime=runtime,
    )


def _route_generate_qleverfile(args: argparse.Namespace) -> dict:
    """Generate Qleverfiles for sources with download URLs."""
    data_dir = Path(args.data_dir).resolve()
    base_port = args.base_port
    runtime = args.runtime

    entries = _load_entries(args.sources, args.name_filter)

    # Keep only sources that have a recognised download field.
    downloadable = [
        e for e in entries if _detect_data_format(e) is not None
    ]

    if args.test:
        downloadable = _select_test_sources(downloadable)

    if not downloadable:
        print("No downloadable sources found.")
        return {"generated": [], "skipped": [], "failed": []}

    generated: list[str] = []
    skipped: list[str] = []
    failed: list[dict[str, str]] = []

    total = len(downloadable)
    print(
        f"Generating Qleverfiles for {total} sources "
        f"(data-dir: {data_dir})\n"
    )

    # Port-assignment manifest — handy for the user.
    port_map: dict[str, int] = {}

    for idx, entry in enumerate(downloadable):
        name = entry.get("name", "unknown")
        port = base_port + idx
        port_map[name] = port

        workdir = data_dir / "qlever_workdirs" / name
        qleverfile_path = workdir / "Qleverfile"

        try:
            content = _build_qleverfile(
                entry, data_dir, port, runtime,
            )
            workdir.mkdir(parents=True, exist_ok=True)
            qleverfile_path.write_text(content)

            fmt = _detect_data_format(entry)
            _progress(
                name, idx + 1, total,
                f"OK: port {port}, format={fmt} → {qleverfile_path}",
            )
            generated.append(name)

        except Exception as exc:
            msg = str(exc)[:120]
            _progress(name, idx + 1, total, f"FAIL: {msg}")
            failed.append({"dataset": name, "error": str(exc)})

    # ── Write port-assignment manifest ────────────────────────────
    manifest_path = data_dir / "qlever_workdirs" / "ports.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(port_map, f, indent=2)
    print(f"\nPort manifest → {manifest_path}")

    return {"generated": generated, "skipped": skipped, "failed": failed}


# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════

_ROUTES = {
    "discover": _route_discover,
    "mine": _route_mine,
    "local-mine": _route_local_mine,
    "generate-qleverfile": _route_generate_qleverfile,
}


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # ── Logging ───────────────────────────────────────────────────
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        force=True,
    )

    route_fn = _ROUTES[args.route]
    print(f"\n{'═'*60}")
    print(f"  Route: {args.route}")
    print(f"  Output: {args.output_dir}")
    print(f"{'═'*60}\n")

    t0 = time.monotonic()
    result = route_fn(args)
    elapsed = time.monotonic() - t0

    # ── Summary ───────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"  Route: {args.route}  |  Elapsed: {elapsed:.1f}s")
    print(f"{'─'*60}")

    for key in (
        "succeeded", "generated", "discovered",
        "empty", "failed", "skipped",
    ):
        val = result.get(key)
        if val is not None:
            count = len(val)
            label = key.capitalize()
            print(f"  {label:12s}: {count}")
            if key == "failed" and val:
                for entry in val[:10]:
                    ds = entry["dataset"] if isinstance(entry, dict) else entry
                    err = entry.get("error", "")[:80] if isinstance(entry, dict) else ""
                    print(f"    • {ds}: {err}")

    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
