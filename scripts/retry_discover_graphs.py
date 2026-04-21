#!/usr/bin/env python3
"""
Retry failed graph discovery with multiple strategies:
1. POST/GET with JSON/XML Accept headers
2. Virtuoso-style: default-graph-uri + format=application/sparql-results+json
3. Longer timeout (90s)

Also retries entries where error was cleared (e.g. endpoint URL was fixed).
"""
import yaml
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT = Path("discovered_graphs.yaml")
QUERY = "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 200"
TIMEOUT = 90


def _parse_json(resp):
    data = resp.json()
    return [b["g"]["value"] for b in data.get("results", {}).get("bindings", [])]


def _parse_xml(resp):
    root = ET.fromstring(resp.text)
    ns = {"s": "http://www.w3.org/2005/sparql-results#"}
    return [
        uri.text
        for result in root.findall(".//s:result", ns)
        for uri in result.findall("s:binding[@name='g']/s:uri", ns)
    ]


def _strategies(endpoint, timeout):
    """Yield (name, callable) strategies in priority order."""

    def post_json():
        r = requests.post(endpoint, data={"query": QUERY},
                          headers={"Accept": "application/sparql-results+json",
                                   "Content-Type": "application/x-www-form-urlencoded"},
                          timeout=timeout)
        r.raise_for_status(); return _parse_json(r)
    yield "POST+JSON", post_json

    def get_json():
        r = requests.get(endpoint, params={"query": QUERY},
                         headers={"Accept": "application/sparql-results+json"}, timeout=timeout)
        r.raise_for_status(); return _parse_json(r)
    yield "GET+JSON", get_json

    def post_xml():
        r = requests.post(endpoint, data={"query": QUERY},
                          headers={"Accept": "application/sparql-results+xml",
                                   "Content-Type": "application/x-www-form-urlencoded"},
                          timeout=timeout)
        r.raise_for_status(); return _parse_xml(r)
    yield "POST+XML", post_xml

    def get_xml():
        r = requests.get(endpoint, params={"query": QUERY},
                         headers={"Accept": "application/sparql-results+xml"}, timeout=timeout)
        r.raise_for_status(); return _parse_xml(r)
    yield "GET+XML", get_xml

    # Virtuoso-style: default-graph-uri + format param
    def virtuoso_get():
        r = requests.get(endpoint,
                         params={"default-graph-uri": "", "query": QUERY,
                                 "format": "application/sparql-results+json", "timeout": "0"},
                         timeout=timeout)
        r.raise_for_status(); return _parse_json(r)
    yield "Virtuoso-GET", virtuoso_get

    def virtuoso_post():
        r = requests.post(endpoint,
                          data={"default-graph-uri": "", "query": QUERY,
                                "format": "application/sparql-results+json", "timeout": "0"},
                          timeout=timeout)
        r.raise_for_status(); return _parse_json(r)
    yield "Virtuoso-POST", virtuoso_post

    def get_json_alt():
        r = requests.get(endpoint, params={"query": QUERY},
                         headers={"Accept": "application/json"}, timeout=timeout)
        r.raise_for_status(); return _parse_json(r)
    yield "GET+JSON-alt", get_json_alt

    # Raw SPARQL body (Fuseki/DSMZ style): Content-Type: application/sparql-query
    def post_raw_json():
        r = requests.post(endpoint, data=QUERY,
                          headers={"Accept": "application/sparql-results+json",
                                   "Content-Type": "application/sparql-query"},
                          timeout=timeout)
        r.raise_for_status(); return _parse_json(r)
    yield "POST-raw+JSON", post_raw_json

    def post_raw_xml():
        r = requests.post(endpoint, data=QUERY,
                          headers={"Accept": "application/sparql-results+xml",
                                   "Content-Type": "application/sparql-query"},
                          timeout=timeout)
        r.raise_for_status(); return _parse_xml(r)
    yield "POST-raw+XML", post_raw_xml

    def post_raw_tsv():
        r = requests.post(endpoint, data=QUERY,
                          headers={"Accept": "text/tab-separated-values",
                                   "Content-Type": "application/sparql-query"},
                          timeout=timeout)
        r.raise_for_status()
        # Parse TSV: first line is header, rest are values
        lines = r.text.strip().split("\n")
        return sorted(set(line.strip().strip("<>") for line in lines[1:] if line.strip()))
    yield "POST-raw+TSV", post_raw_tsv


def query_with_retries(endpoint: str) -> dict:
    errors = []
    for name, fn in _strategies(endpoint, TIMEOUT):
        try:
            graphs = fn()
            return {"graphs": sorted(graphs), "error": None, "strategy": name}
        except Exception as e:
            errors.append(f"{name}: {str(e)[:80]}")
    return {"graphs": [], "error": " | ".join(errors), "strategy": None}


def main():
    with open(INPUT) as f:
        data = yaml.safe_load(f)

    # Retry: entries with errors OR entries with cleared error but no graphs
    to_retry = [d for d in data
                if d.get("error") or (not d.get("error") and not d.get("discovered_graphs"))]
    already_ok = [d for d in data if d not in to_retry]
    print(f"{len(already_ok)} already OK, {len(to_retry)} to retry")

    ep_to_entries: dict[str, list] = {}
    for d in to_retry:
        ep_to_entries.setdefault(d["endpoint"], []).append(d)

    print(f"Retrying {len(ep_to_entries)} unique endpoints (timeout={TIMEOUT}s) ...")

    def _query(ep):
        return ep, query_with_retries(ep)

    ep_results = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(_query, ep): ep for ep in ep_to_entries}
        for i, fut in enumerate(as_completed(futs), 1):
            ep, result = fut.result()
            ep_results[ep] = result
            if result["error"]:
                status = "STILL FAILED"
            else:
                status = f"{len(result['graphs'])} graphs via {result['strategy']}"
            print(f"  [{i}/{len(ep_to_entries)}] {ep}  → {status}")

    fixed = 0
    for d in to_retry:
        res = ep_results[d["endpoint"]]
        if not res["error"]:
            d["discovered_graphs"] = res["graphs"]
            d.pop("error", None)
            fixed += 1
        else:
            d["error"] = res["error"]

    all_data = already_ok + to_retry
    name_order = {d["name"]: i for i, d in enumerate(data)}
    all_data.sort(key=lambda d: name_order.get(d["name"], 999))

    with open(INPUT, "w") as f:
        yaml.dump(all_data, f, default_flow_style=False, sort_keys=False, width=200)

    still_err = sum(1 for d in all_data if d.get("error"))
    has_graphs = sum(1 for d in all_data if d.get("discovered_graphs"))
    print(f"\nFixed {fixed} this run. {has_graphs} total with graphs. {still_err} still have errors.")


if __name__ == "__main__":
    main()
