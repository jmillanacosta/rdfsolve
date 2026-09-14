"""Launch the current MCP server with the kernel's installed Python."""
from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from rdfsolve.client_api import Client
from rdfsolve.openai import resolve_schema
from rdfsolve.mcp.server import run_server


def main() -> None:
    parser = argparse.ArgumentParser(description="rdfsolve four-tool MCP server (stdio)")
    parser.add_argument("--schema", type=Path, help="Saved schema JSON. Defaults to the checkout's AOPWiki schema.")
    parser.add_argument("--source-id", default="aopwikirdf")
    parser.add_argument("--timeout", type=float, default=900, help="Endpoint request timeout in seconds")
    parser.add_argument("--total-ceiling", type=int, default=2000, help="Maximum retained class routes per requirement")
    parser.add_argument("--data-file", type=Path, help="Local RDF instead of the endpoint, for testing")
    parser.add_argument("--log", type=Path, help="Write package query history on shutdown")
    args = parser.parse_args()
    if args.timeout <= 0 or args.total_ceiling < 1:
        parser.error("Timeout and total-ceiling must be positive")
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("rdfsolve.sparql_helper").setLevel(logging.INFO)
    client = Client.open(resolve_schema(args.schema), timeout=args.timeout, data_file=args.data_file)
    try:
        asyncio.run(run_server(client, args.source_id, total_ceiling=args.total_ceiling))
    finally:
        if args.log:
            args.log.parent.mkdir(parents=True, exist_ok=True)
            client.save_session(args.log)
        client.close()


if __name__ == "__main__":
    main()