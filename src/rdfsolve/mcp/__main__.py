"""Start a dedicated RDF investigation over MCP stdio."""

import argparse
import asyncio
import json
import logging
from pathlib import Path
from tempfile import mkdtemp

from rdfsolve.client_api import Client
from rdfsolve.mcp.server import run_server


def main():
    """Load the configured source and run its tool server."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--source-id", default="rdf")
    parser.add_argument("--endpoint")
    parser.add_argument("--data-file", type=Path)
    parser.add_argument("--graphs", type=json.loads)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--max-paths", type=int, default=100)
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--mapping", type=Path)
    parser.add_argument("--related-registry", type=Path, action="append", default=[])
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("rdfsolve.hydration").setLevel(logging.INFO)
    from rdfsolve.registry import Registry

    mappings = []
    if args.mapping:
        from rdfsolve.mapping_models import Mapping

        mappings = Mapping.from_jsonld(args.mapping).edges
    peers = [Registry.read(path) for path in args.related_registry]
    artifacts = args.artifact_dir or Path(mkdtemp(prefix="rdfsolve-results-"))
    log = args.log or artifacts / "investigation.json"
    scope = {} if args.graphs is None else {"graph_uris": args.graphs}
    with Client.open(
        args.schema, source=args.endpoint, data_file=args.data_file, timeout=args.timeout, **scope
    ) as client:
        try:
            asyncio.run(
                run_server(
                    client,
                    args.source_id,
                    max_paths=args.max_paths,
                    artifact_dir=artifacts,
                    log_path=log,
                    class_mappings=mappings,
                    related_registries=peers,
                )
            )
        finally:
            log.parent.mkdir(parents=True, exist_ok=True)
            client.save_session(log, incremental=True)


if __name__ == "__main__":
    main()
