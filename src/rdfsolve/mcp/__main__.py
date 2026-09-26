"""Start the rdfsolve tool server for one RDF source over MCP stdio."""

import argparse
import asyncio
import json
import logging
from pathlib import Path

from rdfsolve.client.api import Client
from rdfsolve.mcp.server import run_server
from rdfsolve.mcp.tools import Toolbox


def main() -> None:
    """Open the source and serve its tools."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--source-id", default="rdf")
    parser.add_argument("--endpoint")
    parser.add_argument("--data-file", type=Path)
    parser.add_argument("--graphs", type=json.loads, help="JSON list; [] for all data")
    parser.add_argument("--output-variables", type=json.loads, default=[])
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument("--ontology-provider", choices=["ols", "ontobee"])
    parser.add_argument("--ontology-cache", type=Path)
    parser.add_argument("--ontology-offline", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    ontology: object = False
    if args.ontology_provider:
        from rdfsolve.client.ontology import OntologyLookup

        ontology = OntologyLookup(
            args.ontology_provider, cache=args.ontology_cache, offline=args.ontology_offline
        )
    scope = {} if args.graphs is None else {"graph_uris": args.graphs}
    with Client.open(
        args.schema,
        source=args.endpoint,
        data_file=args.data_file,
        timeout=args.timeout,
        ontology_grounding=ontology,
        source_id=args.source_id,
        **scope,
    ) as client:
        toolbox = Toolbox(
            client, output_variables=args.output_variables, artifact_dir=args.artifact_dir
        )
        asyncio.run(run_server(toolbox))


if __name__ == "__main__":
    main()
