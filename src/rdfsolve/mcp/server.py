"""Static MCP contracts and summary-only stdio transport."""

from __future__ import annotations

import asyncio
import json
import logging
from time import perf_counter
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_core import to_jsonable_python

from rdfsolve.hydration import HydrationLimitError
from rdfsolve.mcp.session import Session
from rdfsolve.query_fragments import QuerySyntaxError
from rdfsolve.retrieval import QueryValidationError, Requirement
from rdfsolve.sparql_helper import EndpointError


class Args(BaseModel):
    """Reject unknown or coerced tool arguments."""

    model_config = ConfigDict(extra="forbid", strict=True)


class SchemaArgs(Args):
    """Declare question clauses and retrieve connected schema evidence."""

    concepts: list[str] = Field(default_factory=list, max_length=8)
    owners: list[str] = Field(default_factory=list, max_length=8)
    targets: list[str] = Field(default_factory=list, max_length=8)
    question: str = Field(
        default="",
    )
    goals: list[Requirement] | None = Field(
        default=None,
        max_length=16,
        description="First call: atomic outputs, relations, entity restrictions and text conditions. Each needs its own clause and schema-independent concept. owner identifies the subject concept; entity/text goals also declare value. These commitments persist across repairs.",
    )
    offset: int = Field(default=0, ge=0)


class FindArgs(Args):
    """Ground a name within a discovered class."""

    text: str = Field(min_length=1, max_length=200)
    kind: str
    fields: list[str] = Field(default_factory=list, max_length=12)


class PathsArgs(Args):
    """Retrieve bounded paths between grounded endpoints."""

    source: str
    target: str
    max_hops: int = Field(default=3, ge=1, le=6)
    offset: int = Field(default=0, ge=0)


class InspectArgs(Args):
    """Read a definition or search a particular field for grounding evidence."""

    ref: str
    text: str = Field(default="", max_length=200)


class Grounding(Args):
    """Bind a retained clause to its RDF pattern and requested outputs."""

    project: list[str] = Field(
        default_factory=list,
        max_length=20,
        description="Variables to return for this clause, including requested resource identities.",
    )
    evidence: list[str] = Field(
        default_factory=list,
        max_length=8,
        description="Selected retained classes, fields or paths representing this goal. The package checks their actual bindings and outputs.",
    )
    entity: str = Field(
        default="",
        description="For an entity goal: exact retained term reference returned by discovery.",
    )


class PrepareArgs(Args):
    """Compose a retrieval query and check every declared clause."""

    grounding: dict[str, Grounding] = Field(
        description="One entry per active goal ID: selected schema evidence, requested output variables, and entity reference where required."
    )
    sparql: str = Field(
        description="One complete ordinary SELECT. Use the returned insert strings for retained paths and terms. Preserve the declared requirements; the package checks bindings, scope and field constraints."
    )


class ProbeArgs(Args):
    """Profile a bounded sample of a prepared query."""

    query_ref: str
    limit: int = Field(default=5, ge=1, le=20)


class FinishArgs(Args):
    """Explicitly execute the selected artifact."""

    query_ref: str


CONTRACTS = {
    "rdf_schema": (
        SchemaArgs,
        "schema",
        "Start with the original question, all its clauses as goals, and separate concepts. Retrieve connected retained schema. Use owners and targets for structural lookup; zero endpoint requests.",
    ),
    "rdf_find": (
        FindArgs,
        "find",
        "Find typed entities with Client.find/search. Returns a few distinguishable identities. Use the selected identity on its actual relationship.",
    ),
    "rdf_paths": (
        PathsArgs,
        "paths",
        "Let the package find full SHACL and mined paths between discovered classes or entities. Retains alternatives and exact entity anchors.",
    ),
    "rdf_inspect": (
        InspectArgs,
        "inspect",
        "Read a retained field/path definition or query checks. Optional text searches one field for grounding candidates. Returns summaries; complete data stays in Python artifacts.",
    ),
    "rdf_prepare": (
        PrepareArgs,
        "prepare",
        "Submit one ordinary SELECT and its goal evidence. Package expands retained paths and checks outputs, entity restrictions, owners and scopes. Preparation makes no data request.",
    ),
    "rdf_probe": (
        ProbeArgs,
        "probe",
        "Profile an explicit uncertainty using a bounded prepared query through Client/SparqlHelper. A successful sample does not establish completeness or correct meaning.",
    ),
    "rdf_finish": (
        FinishArgs,
        "finish",
        "Execute the final validated artifact once through Client/SparqlHelper. Returns row count, strategy and warnings. Summarize those facts briefly for the user; complete RDF data stays outside model context.",
    ),
}


def dispatch(session: Session, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Validate, execute and journal a tool call with JSON-safe failures."""
    if len(json.dumps(arguments).encode()) > 65536:
        return {
            "error": {
                "code": "input_budget",
                "message": "Tool arguments exceed 64 KiB. Ground fewer fields per clause.",
            }
        }
    if name not in CONTRACTS:
        return {"error": {"code": "unknown_tool", "message": name}}
    model, method, _ = CONTRACTS[name]
    started, before = perf_counter(), len(session.client.queries)
    try:
        args = model.model_validate(arguments).model_dump(exclude_none=True)
        with session.lock:
            value = getattr(session, method)(**args)
    except ValidationError as exc:
        value = {
            "error": {
                "code": "invalid_arguments",
                "details": exc.errors(
                    include_input=False, include_url=False, include_context=False
                ),
            }
        }
    except QuerySyntaxError as exc:
        value = {"error": exc.detail}
    except QueryValidationError as exc:
        value = {"error": {"code": exc.code, "message": str(exc)}}
    except HydrationLimitError as exc:
        value = {
            "error": {
                "code": "search_budget",
                "message": str(exc),
                "retry": "Narrow endpoints or increase path budget.",
            }
        }
    except EndpointError as exc:
        value = {
            "error": {
                "code": "endpoint_error",
                "message": str(exc),
                "retry": "The shared helper exhausted its recovery strategy.",
            }
        }
    except (ValueError, LookupError) as exc:
        value = {"error": {"code": "invalid_request", "message": str(exc)}}
    except Exception:
        logging.getLogger(__name__).exception("Package operation %s failed", method)
        value = {
            "error": {
                "code": "package_error",
                "message": "A package operation failed. Inspect the correlated server log.",
            }
        }
    value = to_jsonable_python(value)
    if len(json.dumps(value).encode()) > 12000:
        value = {
            "error": {
                "code": "observation_budget",
                "message": "Refine the selected concepts or owners. Full evidence is retained.",
            }
        }
    session.events.append(
        {
            "operation": method,
            "seconds": perf_counter() - started,
            "source_queries": len(session.client.queries) - before,
            "status": value.get("state", "error" if "error" in value else "complete"),
        }
    )
    return value


def create_server(client, source_id="rdf", **kwargs):
    """Register tools and bounded diagnostic resources."""
    from mcp.server import Server
    from mcp.types import (
        CallToolResult,
        ListToolsResult,
        ReadResourceResult,
        TextContent,
        TextResourceContents,
        Tool,
    )

    session = Session(client, source_id, **kwargs)

    async def list_tools(ctx, params):
        return ListToolsResult(
            tools=[
                Tool(name=n, description=d, input_schema=m.model_json_schema())
                for n, (m, _, d) in CONTRACTS.items()
            ]
        )

    async def call_tool(ctx, params):
        value = await asyncio.to_thread(dispatch, session, params.name, params.arguments or {})
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(value, ensure_ascii=False))],
            structured_content=value,
            is_error="error" in value,
        )

    async def read_resource(ctx, params):
        if str(params.uri) != "rdfsolve://diagnostics":
            raise ValueError(
                "Full result resources are private. The caller receives a local artifact after execution."
            )
        value = session.diagnostics()
        return ReadResourceResult(
            contents=[
                TextResourceContents(
                    uri=params.uri, mime_type="application/json", text=json.dumps(value)
                )
            ]
        )

    return Server(
        name="rdfsolve",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
        on_read_resource=read_resource,
    )


async def run_server(client, source_id="rdf", **kwargs):
    """Run a dedicated stdio investigation."""
    from mcp.server.stdio import stdio_server

    server = create_server(client, source_id, **kwargs)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())
