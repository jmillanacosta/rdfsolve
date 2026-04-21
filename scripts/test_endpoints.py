#!/usr/bin/env python3
"""Test SPARQL connectivity for all sources with endpoints.

Sends one ASK { ?s ?p ?o } per unique (source, endpoint) pair using
SparqlHelper.from_source_entry() — which reads sparql_engine and
sparql_strategy from sources.yaml.  Reports success/failure and the
winning strategy for each, then flushes strategy updates back to
sources.yaml.

Usage:
    python scripts/test_endpoints.py [--timeout 30] [--workers 12] [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml

# Ensure src is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rdfsolve.sparql_helper import SparqlHelper  # noqa: E402

SOURCES_YAML = Path(__file__).resolve().parent.parent / "data" / "sources.yaml"
ASK_QUERY = "ASK { ?s ?p ?o }"
SELECT_PROBE = "SELECT * WHERE { ?s ?p ?o } LIMIT 1"


def _test_one(entry: dict, timeout: float) -> dict:
    """Test a single source endpoint.  Returns a result dict."""
    name = entry["name"]
    endpoint = entry.get("endpoint", "")
    engine = entry.get("sparql_engine", "")

    # QLever doesn't support ASK — use SELECT LIMIT 1 instead
    use_select = engine == "qlever"
    configured_strategy = entry.get("sparql_strategy", "") or ""

    result = {
        "name": name,
        "endpoint": endpoint,
        "engine": engine,
        "configured_strategy": configured_strategy,
        "ok": False,
        "winning_strategy": "",
        "error": "",
        "elapsed_s": 0.0,
    }

    if not endpoint:
        result["error"] = "no_endpoint"
        return result

    if entry.get("endpoint_down"):
        result["error"] = "marked_down"
        return result

    try:
        helper = SparqlHelper.from_source_entry(entry, timeout=timeout, max_retries=2)
        t0 = time.perf_counter()
        if use_select:
            helper.select(SELECT_PROBE)
        else:
            helper.ask(ASK_QUERY)
        result["elapsed_s"] = round(time.perf_counter() - t0, 2)
        result["ok"] = True
        result["winning_strategy"] = helper._last_winning_strategy
    except Exception as exc:
        result["error"] = str(exc)[:200]
        result["winning_strategy"] = getattr(helper, "_last_winning_strategy", "")

    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", default=str(SOURCES_YAML))
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't flush strategy updates to sources.yaml")
    args = parser.parse_args()

    with open(args.sources) as f:
        sources = yaml.safe_load(f)

    # Deduplicate: only test each (name, endpoint) once
    to_test = [s for s in sources if s.get("endpoint")]
    print(f"Testing {len(to_test)} sources with endpoints (timeout={args.timeout}s, workers={args.workers})")

    results = []
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_test_one, s, args.timeout): s["name"] for s in to_test}
        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            status = "✓" if r["ok"] else "✗"
            strat = r["winning_strategy"] or r["error"][:60]
            print(f"  {status} {r['name']:40s}  {r['engine']:12s}  {strat}  ({r['elapsed_s']:.1f}s)")

    elapsed = time.perf_counter() - t0

    # Summary
    ok = [r for r in results if r["ok"]]
    fail = [r for r in results if not r["ok"] and r["error"] != "marked_down" and r["error"] != "no_endpoint"]
    down = [r for r in results if r["error"] == "marked_down"]
    no_ep = [r for r in results if r["error"] == "no_endpoint"]

    print(f"\n{'='*70}")
    print(f"Total: {len(results)}  OK: {len(ok)}  Failed: {len(fail)}  Marked-down: {len(down)}  No-endpoint: {len(no_ep)}")
    print(f"Elapsed: {elapsed:.1f}s")

    # Strategy changes detected
    updates = SparqlHelper.get_strategy_updates()
    if updates:
        print(f"\nStrategy updates discovered ({len(updates)}):")
        for name, strat in sorted(updates.items()):
            src = next((s for s in sources if s["name"] == name), {})
            old = src.get("sparql_strategy", "") or "(none)"
            print(f"  {name:40s}  {old:20s} -> {strat}")

        if not args.dry_run:
            n = SparqlHelper.flush_strategy_updates(args.sources)
            print(f"\nFlushed {n} strategy updates to {args.sources}")
        else:
            print("\n(dry-run: not flushing)")

    # List failures
    if fail:
        print(f"\nFailed endpoints ({len(fail)}):")
        for r in sorted(fail, key=lambda x: x["name"]):
            print(f"  {r['name']:40s}  {r['endpoint'][:60]}")
            print(f"    {r['error'][:120]}")

    # Write results YAML
    out_path = Path(args.sources).parent.parent / "endpoint_test_results.yaml"
    with open(out_path, "w") as f:
        yaml.dump(results, f, default_flow_style=False, sort_keys=False, width=200)
    print(f"\nDetailed results: {out_path}")


if __name__ == "__main__":
    main()
