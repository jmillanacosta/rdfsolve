#!/usr/bin/env python3
"""
Probe ALL SPARQL endpoints in sources.yaml to detect:
  1. Whether the endpoint is up (endpoint_up: true/false)
  2. The SPARQL engine (sparql_engine: qlever/virtuoso/blazegraph/fuseki/oxigraph/...)
  3. Whether it supports GRAPH queries (supports_graph: true/false)
  4. The working query strategy (sparql_strategy: get+json / post+form+json / post+raw+json / virtuoso / ...).

Outputs endpoint_probes.yaml for review, then apply_endpoint_metadata.py
updates sources.yaml.

Usage:
    python scripts/probe_endpoints.py [--timeout 30] [--workers 10] [--output endpoint_probes.yaml]
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import yaml

SOURCES_YAML = Path(__file__).resolve().parent.parent / "data" / "sources.yaml"

ASK_QUERY = "ASK { ?s ?p ?o }"
GRAPH_QUERY = "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 5"

ENGINE_PATTERNS = {
    "qlever": [
        lambda h, b: "qlever" in h.get("Server", "").lower(),
        lambda h, b: isinstance(b, dict) and "metadata" in b and "query" in b,
        lambda h, b: (
            isinstance(b, dict) and "exception" in b and "QLever" in b.get("exception", "")
        ),
    ],
    "virtuoso": [
        lambda h, b: "Virtuoso" in h.get("Server", ""),
        lambda h, b: "X-SPARQL-default-graph" in h,
    ],
    "blazegraph": [
        lambda h, b: (
            "Blazegraph" in h.get("Server", "") or "blazegraph" in h.get("Server", "").lower()
        ),
        lambda h, b: "X-BIGDATA-MAX-QUERY-MILLIS" in h,
    ],
    "fuseki": [
        lambda h, b: "Fuseki" in h.get("Server", "") or "fuseki" in h.get("Server", "").lower(),
        lambda h, b: "Jena" in h.get("Server", ""),
    ],
    "oxigraph": [
        lambda h, b: "oxigraph" in h.get("Server", "").lower(),
    ],
    "graphdb": [
        lambda h, b: "GraphDB" in h.get("Server", ""),
    ],
    "stardog": [
        lambda h, b: "Stardog" in h.get("Server", ""),
        lambda h, b: "SD-Connection-ID" in h,
    ],
    "comunica": [
        lambda h, b: "comunica" in h.get("Server", "").lower(),
    ],
    "rdf4j": [
        lambda h, b: "RDF4J" in h.get("Server", "") or "rdf4j" in str(h).lower(),
    ],
}


def detect_engine(headers, body) -> str:
    """Try to identify the SPARQL engine from response headers and body."""
    for engine, checks in ENGINE_PATTERNS.items():
        for check in checks:
            try:
                if check(headers, body):
                    return engine
            except Exception:
                pass
    server = headers.get("Server", "")
    if server:
        return f"unknown ({server})"
    return "unknown"


def try_query(endpoint, query, method, content_type, accept, timeout):
    """Execute a SPARQL query and return (response, parsed_body_or_None)."""
    if method == "GET":
        r = requests.get(
            endpoint, params={"query": query}, headers={"Accept": accept}, timeout=timeout
        )
    elif content_type == "application/x-www-form-urlencoded":
        r = requests.post(
            endpoint,
            data={"query": query},
            headers={"Accept": accept, "Content-Type": content_type},
            timeout=timeout,
        )
    elif content_type == "application/sparql-query":
        r = requests.post(
            endpoint,
            data=query,
            headers={"Accept": accept, "Content-Type": content_type},
            timeout=timeout,
        )
    else:
        raise ValueError(f"Unknown content_type: {content_type}")
    return r


STRATEGIES = [
    ("get+json", "GET", None, "application/sparql-results+json"),
    (
        "post+form+json",
        "POST",
        "application/x-www-form-urlencoded",
        "application/sparql-results+json",
    ),
    ("post+raw+json", "POST", "application/sparql-query", "application/sparql-results+json"),
    ("get+xml", "GET", None, "application/sparql-results+xml"),
    (
        "post+form+xml",
        "POST",
        "application/x-www-form-urlencoded",
        "application/sparql-results+xml",
    ),
    ("post+raw+xml", "POST", "application/sparql-query", "application/sparql-results+xml"),
    ("virtuoso", "GET", None, "application/sparql-results+json"),  # special handling below
]


def probe_endpoint(endpoint: str, timeout: int) -> dict:
    result = {
        "endpoint_up": False,
        "sparql_engine": "unknown",
        "supports_graph": None,
        "sparql_strategy": None,
        "error": None,
    }

    # Try each strategy with ASK query to find one that works
    working_strategy = None
    engine = "unknown"
    last_error = None

    for strat_name, method, ct, accept in STRATEGIES:
        try:
            if strat_name == "virtuoso":
                # Virtuoso-style with default-graph-uri param
                r = requests.get(
                    endpoint,
                    params={
                        "default-graph-uri": "",
                        "query": ASK_QUERY,
                        "format": "application/sparql-results+json",
                        "timeout": "0",
                    },
                    timeout=timeout,
                )
            else:
                r = try_query(endpoint, ASK_QUERY, method, ct, accept, timeout)

            body = None
            try:
                body = r.json()
            except Exception:
                pass

            eng = detect_engine(r.headers, body)
            if eng != "unknown":
                engine = eng

            if r.status_code < 400:
                result["endpoint_up"] = True
                working_strategy = strat_name
                break
            elif r.status_code == 400 and body and isinstance(body, dict):
                # Got a structured error — endpoint is up but query format wrong
                result["endpoint_up"] = True
                eng2 = detect_engine(r.headers, body)
                if eng2 != "unknown":
                    engine = eng2
                last_error = f"{strat_name}: {r.status_code}"
                continue
            else:
                last_error = f"{strat_name}: {r.status_code}"
        except requests.exceptions.Timeout:
            last_error = f"{strat_name}: timeout"
        except Exception as e:
            last_error = f"{strat_name}: {str(e)[:80]}"

    result["sparql_engine"] = engine
    result["sparql_strategy"] = working_strategy

    if not result["endpoint_up"]:
        result["error"] = last_error
        return result

    # Now test GRAPH support with the working strategy
    if working_strategy:
        try:
            strat = next(s for s in STRATEGIES if s[0] == working_strategy)
            strat_name, method, ct, accept = strat
            if strat_name == "virtuoso":
                r = requests.get(
                    endpoint,
                    params={
                        "default-graph-uri": "",
                        "query": GRAPH_QUERY,
                        "format": "application/sparql-results+json",
                        "timeout": "0",
                    },
                    timeout=timeout,
                )
            else:
                r = try_query(endpoint, GRAPH_QUERY, method, ct, accept, timeout)

            if r.status_code < 400:
                body = None
                try:
                    body = r.json()
                except Exception:
                    pass
                result["supports_graph"] = True
            else:
                # Check if it's a "not supported" error
                try:
                    body = r.json()
                    err_msg = str(body)
                    if "not supported" in err_msg.lower() or "GRAPH" in err_msg:
                        result["supports_graph"] = False
                    else:
                        result["supports_graph"] = False
                except Exception:
                    result["supports_graph"] = False
        except Exception:
            result["supports_graph"] = None

    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--output", type=str, default="endpoint_probes.yaml")
    args = parser.parse_args()

    with open(SOURCES_YAML) as f:
        sources = yaml.safe_load(f)

    # All sources with endpoints (including those that already have graph_uris)
    with_ep = [s for s in sources if s.get("endpoint")]

    # Deduplicate by endpoint
    ep_to_names: dict[str, list[str]] = {}
    for s in with_ep:
        ep_to_names.setdefault(s["endpoint"], []).append(s["name"])

    ep_results = {}

    def _probe(ep):
        return ep, probe_endpoint(ep, args.timeout)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(_probe, ep): ep for ep in ep_to_names}
        for _i, fut in enumerate(as_completed(futs), 1):
            ep, res = fut.result()
            ep_results[ep] = res
            ep_to_names[ep]
            up = "UP" if res["endpoint_up"] else "DOWN"
            eng = res["sparql_engine"]
            "graph:yes" if res["supports_graph"] else (
                "graph:no" if res["supports_graph"] is False else "graph:?"
            )
            res["sparql_strategy"] or "none"

    # Build output: one entry per source
    output = []
    for s in with_ep:
        ep = s["endpoint"]
        res = ep_results[ep]
        entry = {
            "name": s["name"],
            "endpoint": ep,
            "endpoint_up": res["endpoint_up"],
            "sparql_engine": res["sparql_engine"],
            "supports_graph": res["supports_graph"],
            "sparql_strategy": res["sparql_strategy"],
        }
        if res.get("error"):
            entry["error"] = res["error"]
        output.append(entry)

    out_path = Path(args.output)
    with open(out_path, "w") as f:
        yaml.dump(output, f, default_flow_style=False, sort_keys=False, width=200)

    up = sum(1 for e in output if e["endpoint_up"])
    len(output) - up
    engines = {}
    for e in output:
        eng = e["sparql_engine"]
        engines[eng] = engines.get(eng, 0) + 1


if __name__ == "__main__":
    main()
