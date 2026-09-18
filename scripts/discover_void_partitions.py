#!/usr/bin/env python
"""Discover VoID descriptions from SPARQL endpoints.

This script queries endpoints for their published VoID (Vocabulary of
Interlinked Datasets) metadata and exports schema artifacts in multiple
formats: VoID Turtle (.ttl) and JSON-LD (.jsonld).
"""

import argparse
import logging
from pathlib import Path

from rdfsolve import discover_void_source, load_sources
from rdfsolve.sparql_helper import SparqlHelper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


def publishes_void(endpoint, graph_uris, timeout):
    """Ask whether a void:Dataset is declared before running the full retrieval.

    The retrieval query matches unbound predicates, which is a whole-graph scan
    on a large endpoint. This probe is answered from the type index instead.
    """
    query = (
        "PREFIX void: <http://rdfs.org/ns/void#> ASK { %s ?s a void:Dataset %s }"
    )
    scopes = [f"GRAPH <{g}> {{" for g in graph_uris] if graph_uris else [""]
    with SparqlHelper(endpoint, timeout=timeout, max_retries=1) as helper:
        for opening in scopes:
            closing = "}" if opening else ""
            try:
                if helper.select(query % (opening, closing), purpose="void/probe").get("boolean"):
                    return True
            except Exception as error:
                log.warning("  ! VoID probe failed: %s", str(error)[:120])
                return False
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Discover VoID descriptions from SPARQL endpoints"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/void"),
        help="Output directory for VoID artifacts",
    )
    parser.add_argument(
        "--sources",
        type=Path,
        default=Path("data/sources.yaml"),
        help="Path to sources.yaml file",
    )
    parser.add_argument(
        "--source-names",
        nargs="+",
        help="Specific source names to process (default: all with endpoints)",
    )
    parser.add_argument(
        "--graph-batch",
        type=int,
        default=1000,
        help="Graph names requested per page when a source configures none",
    )
    parser.add_argument(
        "--max-graphs",
        type=int,
        default=10000,
        help="Give up on graph discovery beyond this many names",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Seconds allowed per endpoint request",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger("rdfsolve").setLevel(logging.DEBUG)

    # Load sources
    log.info("Loading sources from %s", args.sources)
    sources_list = load_sources(args.sources)
    # Convert list to dict keyed by name
    sources = {src["name"]: src for src in sources_list if src.get("name")}

    # Filter sources with endpoints
    endpoint_sources = {
        name: src
        for name, src in sources.items()
        if src.get("endpoint")
    }

    if args.source_names:
        endpoint_sources = {
            name: src
            for name, src in endpoint_sources.items()
            if name in args.source_names
        }

    log.info("Found %d sources with endpoints", len(endpoint_sources))

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Discover VoID for each source
    success_count = 0
    fail_count = 0

    for name, source in endpoint_sources.items():
        endpoint = source["endpoint"]
        log.info("Discovering VoID for %s: %s", name, endpoint)

        graph_uris = source.get("graph_uris") or None
        if graph_uris:
            log.info("  scope: %d named graphs", len(graph_uris))
        if not publishes_void(endpoint, graph_uris, args.timeout):
            log.warning("  ! No void:Dataset declared by %s", name)
            fail_count += 1
            continue
        try:
            result = discover_void_source(
                endpoint=endpoint,
                name=name,
                output_dir=args.output_dir / name,
                graph_uris=graph_uris,
                timeout=args.timeout,
                batch_size=args.graph_batch,
                max_pages=max(1, args.max_graphs // args.graph_batch),
            )

            if len(result.graph):
                partitions_count = len(result.to_mined_schema().patterns)
                files = result.files
                log.info(
                    "  ✓ Found %d partitions for %s",
                    partitions_count,
                    name,
                )
                if files:
                    log.info("    Exported:")
                    for fmt, path in files.items():
                        log.info("      - %s: %s", fmt, Path(path).name)
                success_count += 1
            else:
                log.warning("  ! No VoID partitions found for %s", name)
                fail_count += 1

        except Exception as e:
            log.error("  ✗ Failed to discover VoID for %s: %s", name, e)
            fail_count += 1

    log.info("=" * 60)
    log.info("VoID Discovery Complete")
    log.info("=" * 60)
    log.info("Success: %d", success_count)
    log.info("Failed:  %d", fail_count)
    log.info("Output:  %s", args.output_dir)


if __name__ == "__main__":
    main()
