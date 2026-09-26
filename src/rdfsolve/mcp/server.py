"""Serve the investigation tools over MCP stdio."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_core import to_jsonable_python

from rdfsolve.client.hydration import HydrationLimitError
from rdfsolve.client.query_fragments import QuerySyntaxError
from rdfsolve.client.retrieval import QueryValidationError
from rdfsolve.sparql_helper import EndpointError

if TYPE_CHECKING:
    from mcp.server import Server, ServerRequestContext
    from mcp.types import (
        CallToolRequestParams,
        PaginatedRequestParams,
        ReadResourceRequestParams,
    )

    from rdfsolve.mcp.tools import Toolbox


class Args(BaseModel):
    """Refuse unknown tool arguments."""

    model_config = ConfigDict(extra="forbid")


class SchemaArgs(Args):
    """Show the schema. Give classes to see their properties, value types, counts, example values and the links to them. Give search words to find classes and properties by name. Give nothing to see all classes."""

    classes: list[str] = Field(
        default_factory=list,
        max_length=6,
        description="Classes as CURIE, IRI or name, for example aopo:KeyEvent.",
    )
    search: list[str] = Field(
        default_factory=list, max_length=6, description="Words to find in names of terms."
    )


class FindArgs(Args):
    """Find resources in the data by name, words of their text, IRI, or identifier (for example CAS number or hgnc:5). Gives IRIs with their classes."""

    text: str = Field(min_length=1, max_length=200)
    in_class: str | None = Field(default=None, description="Optional class to search in.")
    limit: int = Field(default=10, ge=1, le=30)


class PathsArgs(Args):
    """Show how two classes are linked. Gives triple patterns from ?source to ?target with counts, shortest first."""

    source: str = Field(description="Class as CURIE, IRI or name.")
    target: str = Field(description="Class as CURIE, IRI or name.")
    max_hops: int = Field(default=3, ge=1, le=4)


class RunArgs(Args):
    """Run a SPARQL SELECT query on the source and see the first rows. Missing PREFIX declarations of known prefixes are added. Notes tell about terms that are not in the schema and about the cause of an empty result."""

    sparql: str = Field(min_length=1, max_length=20000)
    limit: int = Field(default=10, ge=1, le=50)


class AnswerArgs(Args):
    """Run the final SPARQL SELECT query on all data and end the task. Select the output variables that the question gives."""

    sparql: str = Field(min_length=1, max_length=20000)


CONTRACTS: dict[str, type[Args]] = {
    "schema": SchemaArgs,
    "find": FindArgs,
    "paths": PathsArgs,
    "run": RunArgs,
    "answer": AnswerArgs,
}


def dispatch(toolbox: Toolbox, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Check the arguments, run one tool, and give a JSON result or a JSON error."""
    if name not in CONTRACTS:
        return {
            "error": {"code": "unknown_tool", "message": f"Use one of: {', '.join(CONTRACTS)}."}
        }
    try:
        values = CONTRACTS[name].model_validate(arguments).model_dump(exclude_none=True)
        with toolbox.lock:
            value = getattr(toolbox, name)(**values)
    except ValidationError as exc:
        details = exc.errors(include_input=False, include_url=False, include_context=False)
        value = {"error": {"code": "invalid_arguments", "details": details}}
    except QuerySyntaxError as exc:
        value = {"error": exc.detail}
    except QueryValidationError as exc:
        value = {"error": {"code": exc.code, "message": str(exc)}}
    except HydrationLimitError as exc:
        value = {
            "error": {"code": "too_many_matches", "message": f"{exc} Give a class or more words."}
        }
    except EndpointError as exc:
        value = {"error": {"code": "source_error", "message": str(exc)[:1500]}}
    except ValueError as exc:
        value = {"error": {"code": "invalid_request", "message": str(exc)[:1500]}}
    except Exception:
        logging.getLogger(__name__).exception("Tool %s failed", name)
        value = {"error": {"code": "tool_error", "message": "The tool failed. See the server log."}}
    value = dict(to_jsonable_python(value))
    if len(json.dumps(value, ensure_ascii=False)) > 16000 and "text" in value:
        value["text"] = value["text"][:15000] + "\n… (cut; ask for fewer classes)"
    return value


def create_server(toolbox: Toolbox) -> Server[Any]:
    """Register the tools and the overview and diagnostics resources."""
    from mcp.server import Server
    from mcp.types import (
        CallToolResult,
        ListToolsResult,
        ReadResourceResult,
        TextContent,
        TextResourceContents,
        Tool,
    )

    async def list_tools(
        ctx: ServerRequestContext[Any], params: PaginatedRequestParams | None
    ) -> ListToolsResult:
        """List the tool contracts."""
        return ListToolsResult(
            tools=[
                Tool(name=name, description=model.__doc__, input_schema=model.model_json_schema())
                for name, model in CONTRACTS.items()
            ]
        )

    async def call_tool(
        ctx: ServerRequestContext[Any], params: CallToolRequestParams
    ) -> CallToolResult:
        """Run one tool call in a worker thread."""
        value = await asyncio.to_thread(dispatch, toolbox, params.name, params.arguments or {})
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(value, ensure_ascii=False))],
            structured_content=value,
            is_error="error" in value,
        )

    async def read_resource(
        ctx: ServerRequestContext[Any], params: ReadResourceRequestParams
    ) -> ReadResourceResult:
        """Give the overview text or the diagnostics JSON."""
        uri = str(params.uri)
        if uri == "rdfsolve://overview":
            text, mime = toolbox.overview(), "text/plain"
        elif uri == "rdfsolve://diagnostics":
            text, mime = json.dumps(toolbox.diagnostics(), default=str), "application/json"
        else:
            raise ValueError("Use rdfsolve://overview or rdfsolve://diagnostics.")
        return ReadResourceResult(
            contents=[TextResourceContents(uri=params.uri, mime_type=mime, text=text)]
        )

    return Server(
        name="rdfsolve",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
        on_read_resource=read_resource,
    )


async def run_server(toolbox: Toolbox) -> None:
    """Serve one investigation over stdio."""
    from mcp.server.stdio import stdio_server

    server = create_server(toolbox)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())
