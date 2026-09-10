"""Serve a saved RDF schema and its data through MCP stdio."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from functools import partial, wraps
from pathlib import Path
from typing import Any, Literal

import anyio
import pandas as pd
from mcp import Client as MCPClient
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ToolError
from mcp.types import TextResourceContents, ToolAnnotations
from pydantic import BaseModel

from rdfsolve.answer_plan import plan_status
from rdfsolve.answer_query import QueryAnswer
from rdfsolve.client_api import Client
from rdfsolve.client_session import INSTRUCTIONS, ClientSession
from rdfsolve.client_table import record_table
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.sparql_helper import EndpointError


def create_server(session: ClientSession, *, log_path: str | Path | None = None) -> MCPServer:
    """Serve read tools. The caller owns the client; calls run one at a time.

    The optional log contains source data. Choose its path outside agent control.
    This server opens no HTTP port and loads no executable registry content.
    """
    server = MCPServer("rdfsolve", instructions=INSTRUCTIONS)
    lock = anyio.Lock()

    def register(function: Callable[..., Any]) -> None:
        """Register a shared tool with its signature and read-only annotations."""

        @wraps(function)
        async def run(**arguments: Any) -> Any:
            """Run one call and save its outcome before accepting the next."""
            async with lock:
                try:
                    return await anyio.to_thread.run_sync(partial(function, **arguments))
                except (ValueError, LookupError) as error:
                    raise ToolError(str(error)) from error
                except Exception as error:
                    return {
                        "status": "failed",
                        "failure": {
                            "category": type(error).__name__,
                            "message": str(error),
                            "kind": "endpoint" if isinstance(error, EndpointError) else "execution",
                        },
                        "retryable_by_agent": False,
                    }
                finally:
                    if log_path is not None:
                        await anyio.to_thread.run_sync(
                            partial(session.client.save_session, log_path, incremental=True)
                        )

        local = function.__name__ in {"schema", "paths", "plan"}
        server.add_tool(
            run,
            annotations=ToolAnnotations(
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=local,
                open_world_hint=not local,
            ),
        )

    for function in session.functions.values():
        register(function)

    @server.resource("rdfsolve://classes", mime_type="application/json")
    async def classes() -> str:
        """List class names and identifiers from the saved schema, without field data."""
        return json.dumps([{"label": item.label, "id": item.id} for item in session.registry.types])

    @server.resource("rdfsolve://plan", mime_type="application/json")
    async def plan() -> str:
        """Read the answer plan and its observed coverage without source queries."""
        async with lock:
            return json.dumps(plan_status(session))

    @server.resource("rdfsolve://results", mime_type="application/json")
    async def results() -> str:
        """List current result references without reading source data."""
        async with lock:
            return json.dumps(
                [
                    {
                        "reference": reference,
                        "records": None if reference in session.final_queries else len(result),
                        "rows": len(session.final_queries[reference]["bindings"])
                        if reference in session.final_queries
                        else None,
                        "source_references": session.final_queries.get(reference, {}).get(
                            "references", []
                        ),
                        "classes": sorted(
                            {
                                session.client.type_name(session.client.model(kind))
                                for kind in session._kinds(reference)
                            }
                        ),
                    }
                    for reference, result in session.results.items()
                ]
            )

    @server.resource("rdfsolve://results/{reference}", mime_type="application/json")
    async def result(reference: str) -> str:
        """Read retained records and evidence outside the tool preview."""
        async with lock:
            try:
                payload = await anyio.to_thread.run_sync(session.export_result, reference)
            except ValueError as error:
                raise ResourceError(str(error)) from error
            return json.dumps(payload, ensure_ascii=False)

    return server


async def read_plan(server: MCPClient) -> dict[str, Any]:
    """Read chosen classes, routes, topic searches and gaps while the server is open."""
    response = await server.read_resource("rdfsolve://plan", cache_mode="bypass")
    if len(response.contents) != 1 or not isinstance(response.contents[0], TextResourceContents):
        raise ValueError("Expected an answer plan from the RDF server")
    value: dict[str, Any] = json.loads(response.contents[0].text)
    return value


async def read_result(
    server: MCPClient,
    reference: str,
    *,
    output: Literal["table", "records", "connections"] = "table",
) -> pd.DataFrame | list[BaseModel]:
    """Read a server result as a typed table or records, without querying the RDF source.

    Call while the server is open. The returned data remains usable after it closes.
    Tables retain source terms, generated records, and original session query bindings.
    """
    if output not in {"table", "records", "connections"}:
        raise ValueError("Use output='table', 'records' or 'connections'")
    payload = await _result_payload(server, reference)
    if "answer" in payload:
        final = QueryAnswer(payload)
        if output == "records":
            return final.records()
        if output == "connections":
            return final.connections()
        table = final.table()
        table.attrs["records"] = final.records()
        return table
    if output == "connections":
        from rdfsolve.connection_table import connection_table

        return connection_table(payload)
    models = MinedSchema.from_dict(payload["schema"]).to_pydantic_classes()
    by_type = {str(getattr(model, "rdf_class_iri", "")): model for model in models.values()}
    records = [by_type[item["type"]].model_validate(item["data"]) for item in payload["records"]]
    if output == "records":
        return records
    return record_table(
        records,
        labels=payload["labels"],
        context={
            key: payload[key]
            for key in (
                "reference",
                "registry_revision",
                "queries",
                "links",
                "operations",
                "scope",
                "evidence",
                "coverage",
            )
        },
    )


async def _result_payload(server: MCPClient, reference: str) -> dict[str, Any]:
    if len(reference) != 32 or any(char not in "0123456789abcdef" for char in reference):
        raise ValueError("Use a result reference returned by this server")
    response = await server.read_resource(f"rdfsolve://results/{reference}", cache_mode="bypass")
    if len(response.contents) != 1 or not isinstance(response.contents[0], TextResourceContents):
        raise ValueError("Expected one RDF result document")
    payload: dict[str, Any] = json.loads(response.contents[0].text)
    return payload


async def read_answer(server: MCPClient, references: list[str]) -> pd.DataFrame:
    """Return one named table of observed answer rows, metadata and annotation types.

    Use the final answer's references while the server is open. This reads retained
    results only. It does not query missing descriptions or infer relationships.
    """
    from rdfsolve.connection_table import answer_table

    return answer_table([await _result_payload(server, ref) for ref in dict.fromkeys(references)])


async def query_answer(
    server: MCPClient,
    references: list[str],
    *,
    name: str = "Answer",
    fields: dict[str, list[str]] | None = None,
) -> QueryAnswer:
    """Run the final SELECT, then keep its table, query example and RDF subset locally.

    Call while the server is open. A single already-final reference is read without
    executing it again. Omitted classes include their name and description fields.
    """
    if not references:
        raise ValueError(
            "No answer result was supplied. The agent must execute a path query before exporting a table."
        )
    if len(references) == 1 and fields is None:
        payload = await _result_payload(server, references[0])
        if "answer" in payload:
            return QueryAnswer(payload)
    response = await server.call_tool(
        "answer", {"references": references, "name": name, "fields": fields or {}}
    )
    if response.is_error:
        raise ValueError("\n".join(item.text for item in response.content if item.type == "text"))
    if response.structured_content is None:
        raise ValueError("Expected a final query reference")
    if response.structured_content.get("status") == "failed":
        raise EndpointError(str(response.structured_content["failure"]))
    return QueryAnswer(await _result_payload(server, response.structured_content["reference"]))


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
                client.save_session(arguments.log, incremental=True)


if __name__ == "__main__":
    main()
