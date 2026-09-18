"""Start a dedicated RDF investigation over MCP stdio."""

import argparse
import asyncio
import json
import logging
from pathlib import Path
from tempfile import mkdtemp

from rdfsolve.client.api import Client
from rdfsolve.mappings.models.core import MappingEdge
from rdfsolve.mcp.server import run_server


def main() -> None:
    """Load the configured source and run its tool server."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--source-id", default="rdf")
    parser.add_argument("--endpoint")
    parser.add_argument("--data-file", type=Path)
    parser.add_argument("--graphs", type=json.loads)
    parser.add_argument("--output-variables", type=json.loads, default=[])
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--max-paths", type=int, default=100)
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--mapping", type=Path, help="SSSOM class mappings")
    parser.add_argument("--related-registry", type=Path, action="append", default=[])
    parser.add_argument("--ontology-provider", choices=["ols", "ontobee"])
    parser.add_argument("--ontology-cache", type=Path)
    parser.add_argument("--ontology-offline", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("rdfsolve.client.hydration").setLevel(logging.INFO)
    logging.getLogger("rdfsolve.client.ontology").setLevel(logging.INFO)
    from rdfsolve.client.registry import Registry

    peers = [Registry.read(path) for path in args.related_registry]
    mappings: list[MappingEdge] = []
    if args.mapping:
        from rdfsolve.mappings.sssom import project_mappings
        from rdfsolve.schema_models.core import MinedSchema

        identities = {args.source_id: set(MinedSchema.from_json(args.schema).get_classes())}
        identities.update({peer.source_id: {t.id for t in peer.types} for peer in peers})
        mappings, _ = project_mappings(args.mapping, identities)
    artifacts = args.artifact_dir or Path(mkdtemp(prefix="rdfsolve-results-"))
    log = args.log or artifacts / "investigation.json"
    scope = {} if args.graphs is None else {"graph_uris": args.graphs}
    from rdfsolve.client.ontology import OntologyLookup

    ontology = (
        OntologyLookup(
            args.ontology_provider, cache=args.ontology_cache, offline=args.ontology_offline
        )
        if args.ontology_provider
        else False
    )
    with Client.open(
        args.schema,
        source=args.endpoint,
        data_file=args.data_file,
        timeout=args.timeout,
        ontology_grounding=ontology,
        source_id=args.source_id,
        class_mappings=mappings,
        related_registries=peers,
        **scope,
    ) as client:
        try:
            asyncio.run(
                run_server(
                    client,
                    max_paths=args.max_paths,
                    output_variables=args.output_variables,
                    artifact_dir=artifacts,
                    log_path=log,
                )
            )
        finally:
            log.parent.mkdir(parents=True, exist_ok=True)
            client.save_session(log, incremental=True)


if __name__ == "__main__":
    main()
