"""Four-tool MCP v2 server. No legacy planner or fabricated backend interface."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.server import Server
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rdfsolve.client_api import Client
from rdfsolve.mcp.query_service import Intent, QueryService

logger = logging.getLogger(__name__)


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class StartArgs(_Args):
    operation_id: str = Field(min_length=1)
    question: str = Field(min_length=1)
    intent: Intent
    scope: str | None = None


class DecisionAction(_Args):
    type: Literal["choose", "more", "reject", "expand", "revise"]
    decision: str | None = None
    option: str | None = None
    intent: Intent | None = None


class DecideArgs(_Args):
    session: str
    revision: int = Field(ge=1)
    operation_id: str = Field(min_length=1)
    action: DecisionAction


class InspectArgs(_Args):
    target: str = Field(description="schema:<words>, type:<class IRI>, status, decision:<id>, option:<decision>:<option>, or requirement:<id>")
    session: str | None = None
    revision: int | None = Field(default=None, ge=1)


class FinishAction(_Args):
    type: Literal["emit_query", "execute"]
    operation_id: str | None = None


class FinishArgs(_Args):
    session: str
    revision: int = Field(ge=1)
    action: FinishAction


CONTRACTS = {
    "query_start": (StartArgs, "Start from the original question and a typed intent. Use schema inspection to obtain class labels/IRIs. A simple class listing uses where={op:'all',args:[]}. Do not supply SPARQL."),
    "query_decide": (DecideArgs, "Choose an offered option, show more retained choices, reject, or revise intent. Use the current revision and a fresh operation_id. expand does not extend the configured search scope."),
    "query_inspect": (InspectArgs, "Read retained schema or state. Before starting: schema:<short keywords> lists class IRIs, and type:<IRI> lists fields. State targets require session and revision. No endpoint query is run."),
    "query_finish": (FinishArgs, "emit_query returns compiled SPARQL; execute runs that exact query and returns row count, five-row preview and a readable result_ref. execute requires action.operation_id. The preview is not the full result."),
}


def dispatch(service: QueryService, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Validate actual calls, not just the schemas advertised to the model."""
    if name not in CONTRACTS:
        return {"error": {"code": "unknown_tool", "message": f"Unknown tool: {name}"}}
    try:
        args = CONTRACTS[name][0].model_validate(arguments).model_dump(mode="json", by_alias=True, exclude_none=True, exclude_unset=True)
        if "session" in args:
            args["session_id"] = args.pop("session")
        return getattr(service, name)(**args)
    except ValidationError as exc:
        details = [{"path": ".".join(map(str, e["loc"])), "message": e["msg"]} for e in exc.errors(include_input=False)[:5]]
        return {"error": {"code": "invalid_input", "message": "Arguments do not match the tool contract", "details": details}}
    except (ValueError, TypeError, KeyError) as exc:
        return {"error": {"code": "invalid_input", "message": str(exc)}}
    except Exception as exc:
        logger.exception("%s failed", name)
        return {"error": {"code": "internal_error", "message": f"{type(exc).__name__}: {exc}"}}


def create_server(client: Client, source_id: str | None = None, *, total_ceiling: int = 2000) -> Server:
    from mcp.server import Server
    from mcp.types import (
        CallToolResult, ListResourcesResult, ListToolsResult, ReadResourceResult,
        Resource, TextContent, TextResourceContents, Tool,
    )
    service = QueryService(client, source_id=source_id, total_ceiling=total_ceiling)

    async def list_tools(ctx, params):
        return ListToolsResult(tools=[Tool(name=name, description=description,
                                          input_schema=model.model_json_schema(by_alias=True))
                                      for name, (model, description) in CONTRACTS.items()])

    async def call_tool(ctx, params):
        # Blocking endpoint I/O must not occupy the event loop. The service lock
        # makes concurrent replay deterministic, including a cancelled client call.
        result = await asyncio.to_thread(dispatch, service, params.name, params.arguments or {})
        return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, ensure_ascii=False))],
                              structured_content=result, is_error="error" in result)

    async def list_resources(ctx, params):
        with service._lock:
            resources = [Resource(uri=f"rdfsolve://sessions/{owner}/artifacts/{identifier}",
                                  name=identifier, mime_type="application/json")
                         for identifier, owner in service._artifact_owners.items()]
        return ListResourcesResult(resources=resources)

    async def read_resource(ctx, params):
        result = await asyncio.to_thread(service.read_resource, str(params.uri))
        return ReadResourceResult(contents=[TextResourceContents(uri=str(params.uri), mime_type="application/json",
                                                                 text=json.dumps(result, ensure_ascii=False))])

    return Server(name="rdfsolve", on_list_tools=list_tools, on_call_tool=call_tool,
                  on_list_resources=list_resources, on_read_resource=read_resource)


async def run_server(client: Client, source_id: str | None = None, *, total_ceiling: int = 2000) -> None:
    from mcp.server.stdio import stdio_server
    server = create_server(client, source_id, total_ceiling=total_ceiling)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())
