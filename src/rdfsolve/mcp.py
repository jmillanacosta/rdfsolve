"""Serve a saved RDF schema and its data through MCP stdio."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from functools import partial, wraps
from pathlib import Path
from typing import Any

import anyio
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from rdfsolve.client_api import Client
from rdfsolve.client_session import ClientSession
from rdfsolve.session_tools import INSTRUCTIONS, SessionTools
from rdfsolve.sparql_helper import EndpointError


def create_server(session: ClientSession, *, log_path: str | Path | None = None) -> MCPServer:
    """Serve six tools. The caller owns the client; calls run one at a time.

    The optional log contains source data. Choose its path outside agent control.
    This server opens no HTTP port and loads no executable registry content.
    """
    server = MCPServer("rdfsolve", instructions=INSTRUCTIONS)
    tools = SessionTools(session)
    lock = anyio.Lock()

    def register(function: Callable[..., Any]) -> None:
        """Register a shared tool with its signature and read-only annotations."""

        @wraps(function)
        async def run(**arguments: Any) -> Any:
            """Run one call and save its outcome before accepting the next."""
            async with lock:
                try:
                    return await anyio.to_thread.run_sync(
                        partial(tools.invoke, function.__name__, arguments)
                    )
                except (ValueError, LookupError, EndpointError) as error:
                    raise ToolError(str(error)) from error
                finally:
                    if log_path is not None:
                        session.client.save_session(log_path)

        local = function.__name__ in {"find", "describe", "release"}
        server.add_tool(
            run,
            annotations=ToolAnnotations(
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=function.__name__ in {"find", "describe"},
                open_world_hint=not local,
            ),
        )

    for function in tools.functions.values():
        register(function)
    return server


def main() -> None:
    """Open a saved schema and serve it until the MCP client disconnects."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, required=True, help="Saved schema file")
    parser.add_argument("--format", choices=["json", "shacl", "void"], help="Schema format")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--endpoint", help="Override the schema's SPARQL endpoint")
    source.add_argument("--data", type=Path, help="Read a local RDF file instead of an endpoint")
    parser.add_argument("--graph", action="append", help="Named graph; repeat to select several")
    parser.add_argument("--source-id", default="rdf", help="Registry source identifier")
    parser.add_argument("--log", type=Path, help="Save tool calls, queries, and returned data")
    arguments = parser.parse_args()
    if arguments.data and arguments.graph:
        parser.error("--graph applies to endpoints, not a single local RDF graph")
    if arguments.log:
        arguments.log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.WARNING)
    with Client.open(
        arguments.schema,
        arguments.endpoint,
        format=arguments.format,
        data_file=arguments.data,
        graph_uris=arguments.graph,
    ) as client:
        session = client.session(source_id=arguments.source_id)
        try:
            create_server(session, log_path=arguments.log).run()
        finally:
            if arguments.log:
                client.save_session(arguments.log)


if __name__ == "__main__":
    main()
