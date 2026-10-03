#!/usr/bin/env python
"""Compare schemas using explicit class mappings and verified links between datasets.

Verified links come from rdfsolve.mappings.signatures (infer_links, then verify) and are
read from the tables that write_links makes. The links kept are also written as SSSOM, and
the direct ones (the source writes the terms of the target) as VoID linksets.
"""

import argparse
import json
from collections import Counter
from pathlib import Path

from networkx import node_link_data

from rdfsolve.analysis import build_connectivity, compare_schemas, load_schemas, read_class_mappings
from rdfsolve.analysis.io import load_channel_schemas
from rdfsolve.config import mint
from rdfsolve.analysis.schema import extract_class_set
from rdfsolve.mappings.routes import read_routes
from rdfsolve.mappings.signatures import Link, read_links
from rdfsolve.mappings.sssom import links_to_sssom, write_sssom_tsv
from rdfsolve.mappings.void import links_to_void


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "schemas",
        nargs="+",
        type=Path,
        help="Releases (for example the local release and the remote run); one schema is taken "
        "for each dataset, the local one when there are both, as in the link stage",
    )
    parser.add_argument("--links", nargs="*", type=Path, default=[])
    parser.add_argument("--min-share", type=float, default=0.5)
    parser.add_argument("--routes", nargs="*", type=Path, default=[], help="routes.json files")
    parser.add_argument("--class-mappings", nargs="*", type=Path, default=[])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--creator-id", help="Creator IRI (for example an ORCID) for SSSOM output")
    parser.add_argument("--extraction-mode", choices=["remote", "local", "grouped", "unknown"])
    args = parser.parse_args()
    if args.extraction_mode is None:
        schemas = load_channel_schemas(args.schemas)
    else:
        schemas = {}
        for directory in args.schemas:
            for name, schema in load_schemas(directory, extraction_mode=args.extraction_mode).items():
                if name in schemas:
                    parser.error(f"Select one extraction for {name}")
                schemas[name] = schema
    if not schemas:
        parser.error("Supply canonical schema snapshots")
    links, looked_up, proposed, reports = [], [], [], {}
    for path in args.links:
        read = read_links(path)
        kept = [e for e in read if e.share is not None and e.share >= args.min_share]
        reports[str(path)] = {"verified": len(read), "kept": len(kept), "min_share": args.min_share}
        links.extend(kept)
        looked_up.extend(read)
        # Links whose lookup failed are proposed links: plausible edges of the graph.
        failed = path.parent / "failed.json"
        if failed.exists():
            rows = [row["fields"] for row in json.loads(failed.read_text()) if "fields" in row]
            proposed.extend(Link(**fields) for fields in rows)
            reports[str(path)]["lookup_failed"] = len(rows)
    mappings = []
    for path in args.class_mappings:
        edges, reports[str(path)] = read_class_mappings(path, schemas)
        mappings.extend(edges)
    # The graph takes every link that was looked up; each edge has its level of evidence
    # (confirmed, tested or plausible). The SSSOM and VoID outputs keep the threshold.
    known = {(name, cls) for name, schema in schemas.items() for cls in extract_class_set(schema)}
    proposed = [
        link
        for link in proposed
        if (link.source, link.source_class) in known and (link.target, link.target_class) in known
    ]
    # Routes across datasets that were tested on the data (check_routes.py): one edge each.
    routes = [
        route
        for path in args.routes
        for route in read_routes(path)
        if (route.route.link.source, route.route.start_class) in known
        and (route.route.link.target, route.route.end_class) in known
    ]
    reports["routes"] = {"tested_routes_in_graph": len(routes)}
    graph = build_connectivity(
        schemas,
        class_mappings=mappings,
        links=looked_up,
        candidates=proposed,
        routes=routes,
        min_share=args.min_share,
    )
    levels = Counter(data["evidence"] for *_, data in graph.edges(data=True))
    reports["connectivity"] = {"edges_by_evidence": dict(levels)}
    args.output.mkdir(parents=True, exist_ok=True)
    for name, value in (
        ("class_connectivity", node_link_data(graph)),
        ("schema_overlaps", compare_schemas(schemas)),
        ("evidence_report", reports),
    ):
        (args.output / f"{name}.json").write_text(json.dumps(value, indent=2, default=sorted))
    if links:
        mapping_set = links_to_sssom(
            links,
            schemas,
            mint("mappings", args.output.name),
            min_share=args.min_share,
            **({"creator_id": args.creator_id} if args.creator_id else {}),
        )
        write_sssom_tsv(mapping_set, args.output / "verified_links.sssom.tsv")
        linksets = links_to_void(links, min_share=args.min_share)
        linksets.serialize(args.output / "verified_links.void.ttl", format="turtle")
    print(f"{len(schemas)} schemas, {len(mappings)} explicit class links, {len(links)} verified links")


if __name__ == "__main__":
    main()
