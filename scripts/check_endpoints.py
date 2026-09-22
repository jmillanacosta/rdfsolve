#!/usr/bin/env python
"""Check SPARQL endpoint availability with rate limiting."""

import argparse
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import yaml

from rdfsolve.endpoint_health import check_endpoint_health


def get_hostname(url: str) -> str:
    return urlparse(url).hostname or url


def check_endpoint(url: str) -> dict:
    health = check_endpoint_health(url)
    result = {
        "endpoint": url,
        "hostname": get_hostname(url),
        "status": health.status,
        "response_time_ms": int(health.response_time * 1000)
        if health.response_time is not None
        else None,
    }
    if health.error_message:
        result["error"] = health.error_message[:100]
    return result


def check_server_group(endpoints: list[tuple[str, str]], delay: float = 1.5) -> dict:
    results = {}
    for i, (name, url) in enumerate(endpoints):
        if i > 0:
            time.sleep(delay)
        results[name] = check_endpoint(url)
        status = results[name]["status"]
        print(f"  {name}: {status}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Check SPARQL endpoint health")
    parser.add_argument("--output", type=Path, help="Output JSON file path")
    parser.add_argument("--sources", type=Path, help="Sources YAML file")
    args = parser.parse_args()

    repo = Path(__file__).parent.parent
    sources_file = args.sources or (repo / "data" / "sources.yaml")

    with open(sources_file) as f:
        sources = yaml.safe_load(f) or []

    endpoints = {s["name"]: s["endpoint"] for s in sources if s.get("endpoint")}

    by_host = defaultdict(list)
    for name, url in endpoints.items():
        host = get_hostname(url)
        by_host[host].append((name, url))

    print(f"Checking {len(endpoints)} endpoints across {len(by_host)} servers...")
    print("Strategy: parallel across servers, sequential within server\n")

    results = {}
    with ThreadPoolExecutor(max_workers=max(1, len(by_host))) as ex:
        futures = {ex.submit(check_server_group, eps): host for host, eps in by_host.items()}

        for future in as_completed(futures):
            host = futures[future]
            print(f"{host}:")
            host_results = future.result()
            results.update(host_results)

    report = {"checked_at": datetime.now(timezone.utc).isoformat(), "endpoints": results}

    output = args.output or (repo / "output" / "endpoint_status.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2))

    print(f"\nReport: {output}")

    up = sum(1 for r in results.values() if r["status"] == "up")
    down = sum(1 for r in results.values() if r["status"] != "up")
    print(f"Summary: {up} up, {down} down")


if __name__ == "__main__":
    main()
