#!/usr/bin/env python3
"""Read the real AOPWiki graph through Graph Store; optionally mine it."""

import argparse
import json
import logging
from pathlib import Path

from rdfsolve import SchemaMiner
from rdfsolve.graph_store import download_graphs, load_downloads
from rdfsolve.schema_models.void_schema import VoidSchema

ENDPOINT = "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql"
STORE = "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql-graph-crud/"
GRAPH = "http://aopwiki.org/"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store-url", default=STORE)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-mb", type=int, default=64)
    parser.add_argument("--mine", action="store_true",
                        help="Run the existing queries locally; requires RAM for parsed RDF")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.mine:
        miner = SchemaMiner(
            ENDPOINT, graph_uris=[GRAPH], get_graphs_from_store=True,
            graph_store_url=args.store_url, graph_store_dir=args.output_dir,
            graph_store_max_bytes=args.max_mb * 1024 * 1024,
            report_path=args.output_dir / "mining-report.json", delay=0,
            enrich=True, examples_per_pattern=1,
        )
        schema = miner.mine("aopwikirdf")
        (args.output_dir / "schema.json").write_text(
            json.dumps(schema.to_dict(), indent=2) + "\n", encoding="utf-8"
        )
        (args.output_dir / "shapes.ttl").write_text(
            schema.to_shacl(trim_descriptions=20), encoding="utf-8"
        )
        print(f"Mined {len(schema.patterns)} patterns from {GRAPH}")
    else:
        downloads = download_graphs(
            args.store_url, [GRAPH], args.output_dir,
            max_bytes=args.max_mb * 1024 * 1024,
        )
        dataset = load_downloads(downloads, endpoint_url=ENDPOINT)
        document = VoidSchema(dataset.graph(GRAPH), ENDPOINT, "aopwikirdf", [GRAPH])
        schema = document.to_mined_schema()
        print(json.dumps({
            "graph": GRAPH, "triples": len(document.graph),
            "void_datasets": len(document.datasets), "void_patterns": len(schema.patterns),
            "note": "Instance RDF is not a VoID description. Use --mine to mine instances.",
        }, indent=2))


if __name__ == "__main__":
    main()
