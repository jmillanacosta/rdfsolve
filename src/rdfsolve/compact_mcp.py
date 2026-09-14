"""Compact MCP server with minimal decision-relevant interface.

The package handles query construction internally. The model provides
semantic interpretation and chooses between ambiguous options.
"""

from __future__ import annotations

import argparse
import json
import logging
from functools import partial, wraps
from pathlib import Path
from typing import Any

import anyio
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ToolError
from mcp.types import TextResourceContents, ToolAnnotations
from pydantic import BaseModel, Field

from rdfsolve.client_api import Client
from rdfsolve.compact_session import COMPACT_INSTRUCTIONS, CompactSession
from rdfsolve.sparql_helper import EndpointError


class InterpretInput(BaseModel):
    """Input schema for interpret tool."""

    source: str = Field(description="Starting entity class name")
    targets: list[str] = Field(
        min_length=1, max_length=5, description="Other entity classes to retrieve"
    )
    selection: str = Field(
        min_length=1, max_length=500, description="What the question asks for"
    )
    filters: list[dict[str, Any]] = Field(
        default_factory=list,
        max_length=10,
        description="Value conditions: [{kind, terms}] or [{kind, iris}]",
    )
    via: list[str] = Field(
        default_factory=list,
        max_length=3,
        description="Required intermediate classes",
    )


class DecideInput(BaseModel):
    """Input schema for decide tool."""

    decision_id: str = Field(description="Decision ID from current state")
    option_id: str = Field(description="Selected option ID")


class MoreInput(BaseModel):
    """Input schema for show_more tool."""

    decision_id: str = Field(description="Decision to expand")


class RejectInput(BaseModel):
    """Input schema for reject_all tool."""

    decision_id: str = Field(description="Decision to reject")


class InspectInput(BaseModel):
    """Input schema for inspect tool."""

    decision_id: str = Field(description="Decision to inspect")
    option_id: str | None = Field(default=None, description="Specific option")


class SchemaInput(BaseModel):
    """Input schema for schema tool."""

    text: str = Field(default="", description="Search text")
    kind: str | None = Field(default=None, description="Specific class name")


def create_compact_server(
    session: CompactSession, *, log_path: str | Path | None = None
) -> MCPServer:
    """Create MCP server with compact interface.

    The server exposes only decision-relevant operations:
    - schema: Find classes (read-only)
    - interpret: Submit question interpretation
    - decide: Choose between options
    - show_more: Request more options
    - reject_all: Reject all options
    - inspect: Examine option details
    - execute: Run compiled query
    - status: Get current state
    """
    server = MCPServer("rdfsolve-compact", instructions=COMPACT_INSTRUCTIONS)
    lock = anyio.Lock()

    def _tool(
        func: Any,
        input_model: type[BaseModel] | None,
        *,
        idempotent: bool = False,
        queries_data: bool = False,
    ) -> None:
        """Register a tool with proper error handling."""

        @wraps(func)
        async def run(**arguments: Any) -> Any:
            async with lock:
                try:
                    if input_model:
                        # Validate input
                        parsed = input_model.model_validate(arguments)
                        result = await anyio.to_thread.run_sync(
                            partial(func, **parsed.model_dump())
                        )
                    else:
                        result = await anyio.to_thread.run_sync(partial(func))
                    return result
                except (ValueError, LookupError) as error:
                    raise ToolError(str(error)) from error
                except Exception as error:
                    return {
                        "status": "failed",
                        "failure": {
                            "category": type(error).__name__,
                            "message": str(error),
                            "kind": "endpoint"
                            if isinstance(error, EndpointError)
                            else "execution",
                        },
                        "retryable_by_agent": False,
                    }
                finally:
                    if log_path is not None:
                        await anyio.to_thread.run_sync(
                            partial(
                                session.client.save_session, log_path, incremental=True
                            )
                        )

        server.add_tool(
            run,
            annotations=ToolAnnotations(
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=idempotent,
                open_world_hint=queries_data,
            ),
        )

    # Register tools

    @wraps(session.schema)
    def schema_tool(text: str = "", kind: str | None = None) -> dict[str, Any]:
        """Find classes and fields by name, without querying records.

        Use text to search class names and descriptions. Use kind to
        see a specific class with all its fields.
        """
        return session.schema(text=text, kind=kind)

    _tool(schema_tool, SchemaInput, idempotent=True)

    @wraps(session.interpret)
    def interpret_tool(
        source: str,
        targets: list[str],
        selection: str,
        filters: list[dict[str, Any]] | None = None,
        via: list[str] | None = None,
    ) -> dict[str, Any]:
        """Submit interpretation of what the question asks for.

        source is the starting entity class. targets are other entity
        classes to retrieve. selection describes what to find.
        filters restrict values: [{kind: class, terms: [values]}].
        via names required intermediate classes.

        Returns current state: ready to execute, or a decision to make.
        """
        return session.interpret(
            source=source,
            targets=targets,
            selection=selection,
            filters=filters or [],
            via=via or [],
        )

    _tool(interpret_tool, InterpretInput)

    @wraps(session.decide)
    def decide_tool(decision_id: str, option_id: str) -> dict[str, Any]:
        """Choose an option for a pending decision.

        Use the decision_id and option_id from the current state.
        Returns updated state: next decision, ready, or complete.
        """
        return session.decide(decision_id=decision_id, option_id=option_id)

    _tool(decide_tool, DecideInput)

    @wraps(session.show_more)
    def show_more_tool(decision_id: str) -> dict[str, Any]:
        """Request more options for a decision.

        Use when none of the displayed options fit. The decision
        state indicates if more options are available.
        """
        return session.show_more(decision_id=decision_id)

    _tool(show_more_tool, MoreInput)

    @wraps(session.reject_all)
    def reject_all_tool(decision_id: str) -> dict[str, Any]:
        """Reject all displayed options and request alternatives.

        Use when all current options are wrong. Returns new options
        or blocked state if no alternatives remain.
        """
        return session.reject_all(decision_id=decision_id)

    _tool(reject_all_tool, RejectInput)

    @wraps(session.inspect)
    def inspect_tool(decision_id: str, option_id: str | None = None) -> dict[str, Any]:
        """Examine details of a decision or specific option.

        Use to understand what an option means before choosing.
        Shows route steps, predicates, and available evidence.
        """
        return session.inspect(decision_id=decision_id, option_id=option_id)

    _tool(inspect_tool, InspectInput, idempotent=True)

    @wraps(session.execute)
    def execute_tool() -> dict[str, Any]:
        """Execute the compiled query and return results.

        Only available when state is 'ready'. Returns result
        reference and row count.
        """
        return session.execute()

    _tool(execute_tool, None, queries_data=True)

    @wraps(session.status)
    def status_tool() -> dict[str, Any]:
        """Get current solver state without side effects.

        Shows plan revision, state, pending decisions, and
        selected routes.
        """
        return session.status()

    _tool(status_tool, None, idempotent=True)

    # Register resources

    @server.resource("rdfsolve://schema", mime_type="application/json")
    async def schema_resource() -> str:
        """Schema context: class names and brief descriptions."""
        async with lock:
            return json.dumps(session._schema_context)

    @server.resource("rdfsolve://status", mime_type="application/json")
    async def status_resource() -> str:
        """Current solver state."""
        async with lock:
            return json.dumps(session.status())

    @server.resource("rdfsolve://log", mime_type="application/json")
    async def log_resource() -> str:
        """Solver execution log for debugging."""
        async with lock:
            return json.dumps(session.get_log())

    @server.resource("rdfsolve://results", mime_type="application/json")
    async def results_resource() -> str:
        """List of result references."""
        async with lock:
            return json.dumps(
                [
                    {"reference": ref, "state": data.get("state", {}).get("state")}
                    for ref, data in session._results.items()
                ]
            )

    @server.resource("rdfsolve://results/{reference}", mime_type="application/json")
    async def result_resource(reference: str) -> str:
        """Full result data by reference."""
        async with lock:
            try:
                return json.dumps(session.get_result(reference))
            except ValueError as error:
                raise ResourceError(str(error)) from error

    return server


def main() -> None:
    """Open schema and serve compact MCP interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, required=True, help="Saved schema file")
    parser.add_argument(
        "--format", choices=["json", "shacl", "void"], help="Schema format"
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--endpoint", help="Override SPARQL endpoint")
    source.add_argument("--data", type=Path, help="Local RDF file")
    parser.add_argument("--graph", action="append", help="Named graph")
    parser.add_argument("--source-id", default="rdf", help="Registry source ID")
    parser.add_argument("--log", type=Path, help="Save execution log")
    parser.add_argument(
        "--max-candidates", type=int, default=50, help="Max route candidates"
    )
    parser.add_argument(
        "--display-options", type=int, default=4, help="Options per decision"
    )
    args = parser.parse_args()

    if args.data and args.graph:
        parser.error("--graph applies to endpoints, not local files")

    if args.log:
        args.log.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(level=logging.WARNING)

    with Client.open(
        args.schema,
        args.endpoint,
        format=args.format,
        data_file=args.data,
        graph_uris=args.graph,
    ) as client:
        session = CompactSession(
            client,
            source_id=args.source_id,
            max_candidates=args.max_candidates,
            display_options=args.display_options,
        )
        try:
            create_compact_server(session, log_path=args.log).run()
        finally:
            if args.log:
                client.save_session(args.log, incremental=True)


if __name__ == "__main__":
    main()
