#!/usr/bin/env python
"""Test the routes across datasets on the data, for the links that verify_links.py found.

A route is a path in the source dataset, a link, and a path in the target dataset
(rdfsolve.mappings.routes). For each link with found values, each schema step and each tested
path that ends at the class of the link is combined with each schema step and each tested path
that starts at the class that the link reaches. Between two local indexes every start instance
is read and a matched route is confirmed; when a dataset is read at its endpoint, --sample
pairs of a start instance and a value are read and a matched route is tested.

Schemas and servers are taken as in verify_links.py. Writes OUTPUT/routes.json (the matched
routes, the number of routes tested, the links that failed) after each link, and
OUTPUT/routes_done.json, with which a stopped run continues.
"""

import argparse
import json
import time
from pathlib import Path

from rdfsolve import MinedSchema
from rdfsolve.api import Client
from rdfsolve.mappings import read_links, read_replacements
from rdfsolve.mappings.routes import check_routes, propose_segments, read_routes, write_routes
from rdfsolve.qlever.lifecycle import ServerPool
from scripts.verify_links import schemas_of


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--links", type=Path, required=True, help="links.tsv of verify_links.py")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample", type=int, default=500)
    parser.add_argument("--budget", type=float, default=600.0, help="Seconds for each link")
    parser.add_argument("--total-budget", type=float, default=6 * 3600.0, help="Seconds in all")
    parser.add_argument("--replacements", nargs="*", type=Path, default=[])
    parser.add_argument("--servers", type=int, default=2)
    parser.add_argument("--base-port", type=int, default=19800)
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    found = schemas_of(args.runs)
    schemas = {name: MinedSchema.from_json(path) for name, (_, path) in found.items()}
    local = {name for name, (channel, _) in found.items() if channel == "local"}
    endpoints = {name: schema.about.endpoint for name, schema in schemas.items()}
    replacements = {}
    for path in args.replacements:
        replacements.update(read_replacements(path))
    links = [e for e in read_links(args.links) if e.found]
    links = [e for e in links if e.link.source in schemas and e.link.target in schemas]
    links.sort(key=lambda e: (e.link.source, e.link.target))

    out = args.output
    out.mkdir(parents=True, exist_ok=True)
    routes_path, done_path = out / "routes.json", out / "routes_done.json"
    state = json.loads(done_path.read_text()) if done_path.exists() else {}
    done, failed = set(state.get("done", [])), state.get("failed", [])
    tested, stop_reason = state.get("tested", 0), None
    routes = read_routes(routes_path) if routes_path.exists() else []
    print(len(links), "links with found values;", len(done), "already done", flush=True)
    pool = ServerPool(args.data_dir, size=args.servers, base_port=args.base_port)

    def client(name):
        """Return a client of a schema at its endpoint, starting its local server when needed."""
        endpoint = pool.endpoint(name) if name in local else endpoints[name]
        return Client(schemas[name], endpoint, timeout=args.timeout)

    def save():
        """Write the routes and the state of the run."""
        write_routes(
            routes_path, routes, links=links, endpoints=endpoints, tested=tested,
            stop_reason=stop_reason, failed=failed,
        )
        done_path.write_text(json.dumps({"done": sorted(done), "failed": failed, "tested": tested}))

    start = time.monotonic()
    try:
        for i, evidence in enumerate(links):
            link = evidence.link
            if repr(link) in done:
                continue
            if time.monotonic() - start >= args.total_budget:
                stop_reason = "budget"
                break
            both_local = link.source in local and link.target in local
            befores, afters = propose_segments(link, schemas[link.source], schemas[link.target])
            # A server that does not start stops the run: it is not a result of the link.
            source, target = client(link.source), client(link.target)
            try:
                result = check_routes(
                    link, befores, afters, source, target,
                    sample=None if both_local else args.sample,
                    replacements=replacements, read_target=both_local, budget_s=args.budget,
                )
            except Exception as error:  # noqa: BLE001 - a failed link is reported, not hidden
                message = f"{type(error).__name__}: {error}"[:300]
                failed.append({"link": repr(link), "error": message})
                done.add(repr(link))
                save()
                print(i + 1, len(links), link.source, link.target, message, flush=True)
                continue
            routes.extend(result.routes)
            tested += result.tested
            if result.stop_reason:
                failed.append({"link": repr(link), "error": "budget of the link: not every route was tested"})
            done.add(repr(link))
            save()
            print(
                i + 1, len(links), link.source, link.target, link.property,
                f"{len(result.routes)} of {result.tested} routes matched", flush=True,
            )
    finally:
        pool.close()
    save()
    print("written", routes_path)


if __name__ == "__main__":
    main()
