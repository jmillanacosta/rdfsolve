#!/usr/bin/env python
"""Compare schemas using explicit class mappings and verified links between datasets.

Verified links come from rdfsolve.mappings.signatures (infer_links, then verify) and are
read from the tables that write_links makes. The links kept are also written as SSSOM.
"""

import argparse
import json
from pathlib import Path

from networkx import node_link_data

from rdfsolve.analysis import build_connectivity, compare_schemas, load_schemas, read_class_mappings
from rdfsolve.config import mint
from rdfsolve.mappings.signatures import read_links
from rdfsolve.mappings.sssom import links_to_sssom, write_sssom_tsv


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("schemas", type=Path)
    parser.add_argument("--links", nargs="*", type=Path, default=[])
    parser.add_argument("--min-share", type=float, default=0.5)
    parser.add_argument("--class-mappings", nargs="*", type=Path, default=[])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--creator-id", help="Creator IRI (for example an ORCID) for SSSOM output")
    parser.add_argument("--extraction-mode", choices=["remote", "local", "grouped", "unknown"])
    args = parser.parse_args()
    schemas = load_schemas(args.schemas, extraction_mode=args.extraction_mode)
    if not schemas:
        parser.error("Supply canonical schema snapshots")
    links, reports = [], {}
    for path in args.links:
        read = read_links(path)
        kept = [e for e in read if e.share is not None and e.share >= args.min_share]
        reports[str(path)] = {"verified": len(read), "kept": len(kept), "min_share": args.min_share}
        links.extend(kept)
    mappings = []
    for path in args.class_mappings:
        edges, reports[str(path)] = read_class_mappings(path, schemas)
        mappings.extend(edges)
    graph = build_connectivity(schemas, class_mappings=mappings, links=links)
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
    print(f"{len(schemas)} schemas, {len(mappings)} explicit class links, {len(links)} verified links")


if __name__ == "__main__":
    main()
