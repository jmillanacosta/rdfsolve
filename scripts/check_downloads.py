#!/usr/bin/env python
"""Check RDF download URL availability with rate limiting."""

import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml


def get_hostname(url: str) -> str:
    return urlparse(url).hostname or url


def check_url(name: str, url: str) -> dict:
    try:
        resp = requests.head(url, timeout=3, allow_redirects=True)
        length = resp.headers.get("content-length")
        status = "accessible" if resp.status_code == 200 else \
                 "redirect" if resp.status_code in (301, 302) else "broken"
        return {
            "download_url": url,
            "hostname": get_hostname(url),
            "status": status,
            "http_status": resp.status_code,
            "content_length": int(length) if length else None
        }
    except requests.Timeout:
        return {
            "download_url": url,
            "hostname": get_hostname(url),
            "status": "timeout",
            "http_status": None
        }
    except Exception as e:
        return {
            "download_url": url,
            "hostname": get_hostname(url),
            "status": "error",
            "http_status": None,
            "error": str(e)[:100]
        }


def check_server_group(downloads: list[tuple[str, str]], delay: float = 2.5) -> dict:
    results = {}
    for i, (name, url) in enumerate(downloads):
        if i > 0:
            time.sleep(delay)
        results[name] = check_url(name, url)
        status = results[name]["status"]
        print(f"  {name}: {status}")
    return results


def main():
    repo = Path(__file__).parent.parent
    sources_file = repo / "data" / "sources.yaml"

    with open(sources_file) as f:
        sources = yaml.safe_load(f) or []

    downloads = {}
    for s in sources:
        for key in ["download_ttl", "download_nt", "download_nq", "download_rdf"]:
            urls = s.get(key, [])
            if isinstance(urls, str):
                urls = [urls]
            if urls:
                downloads[s["name"]] = urls[0]
                break

    by_host = defaultdict(list)
    for name, url in downloads.items():
        host = get_hostname(url)
        by_host[host].append((name, url))

    print(f"Checking {len(downloads)} downloads across {len(by_host)} servers...")
    print(f"Strategy: parallel across servers, sequential within server\n")

    results = {}
    with ThreadPoolExecutor(max_workers=len(by_host)) as ex:
        futures = {
            ex.submit(check_server_group, dls): host
            for host, dls in by_host.items()
        }

        for future in as_completed(futures):
            host = futures[future]
            print(f"{host}:")
            host_results = future.result()
            results.update(host_results)

    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "downloads": results
    }

    output = repo / "output" / "download_status.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2))

    print(f"\nReport: {output}")

    accessible = sum(1 for r in results.values() if r["status"] in ("accessible", "redirect"))
    broken = sum(1 for r in results.values() if r["status"] not in ("accessible", "redirect"))
    print(f"Summary: {accessible} accessible, {broken} broken/error")


if __name__ == "__main__":
    main()
