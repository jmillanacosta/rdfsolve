"""Static MCP contracts and summary-only stdio transport."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_core import to_jsonable_python

from rdfsolve.client.hydration import HydrationLimitError
from rdfsolve.client.query_fragments import QueryPattern, QuerySyntaxError
from rdfsolve.client.retrieval import QueryValidationError, Requirement
from rdfsolve.mcp.session import Session
from rdfsolve.sparql_helper import EndpointError

if TYPE_CHECKING:
    from mcp.server import Server, ServerRequestContext
    from mcp.types import (
        CallToolRequestParams,
        PaginatedRequestParams,
        ReadResourceRequestParams,
    )

    from rdfsolve.client.api import Client


class Args(BaseModel):
    """Reject unknown or coerced tool arguments."""

    model_config = ConfigDict(extra="forbid", strict=True)


class GoalCorrection(Args):
    """Correct grounding while retaining the original clause and named value."""

    concept: str | None = None
    owner: str | None = None
    kind: Literal["output", "relation", "entity_filter", "text_filter"] | None = None
    required: bool | None = None


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
        description="Declare outputs, connecting relationships and restrictions once. Membership and intermediate objects need separate relation goals even when not projected. Each needs a clause and a short concept; filters need value. Later discovery omits goals. Additional goals are merged with retained clauses; use corrections for existing goals.",
    )
    corrections: dict[str, GoalCorrection] = Field(
        default_factory=dict,
        max_length=16,
        description="Existing goal ID to changed grounding fields, e.g. {'g2': {'concept': 'applicable taxon', 'kind': 'entity_filter'}}. Original clauses and values remain retained.",
    )
    offset: int = Field(default=0, ge=0)


class FindArgs(Args):
    """Search typed resources with Client.find/search.

    Optional target evaluates paths for the whole matching set in the same call.
    A selection reference preserves all matches; an entity reference selects one.
    """

    text: str = Field(
        min_length=1,
        max_length=200,
        description="A particular name or phrase to find. To list all members, use rdf_schema and prepare a SELECT; no name search is needed.",
    )
    kind: str | None = Field(
        default=None,
        description="Optional class name or discovered class reference; omission searches across typed classes.",
    )
    target: str | None = Field(
        default=None,
        description="Optional target class to evaluate paths from the whole matching set in the same call.",
    )
    max_hops: int = Field(default=2, ge=1, le=6)
    fields: list[str] = Field(default_factory=list, max_length=12)
    offset: int = Field(default=0, ge=0)


class PathsArgs(Args):
    """Use Client.paths_between on classes or retained selections.

    Class endpoints read the schema; selected resources evaluate the generated
    paths. Returns evidence summaries with exact scope and query references.
    """

    source: str = Field(description="Source class, retained entity or entire selection reference.")
    target: str = Field(
        description="Target class, retained entity or entire selection reference. Field references already supply their own insertable path."
    )
    max_hops: int = Field(default=3, ge=1, le=6)
    meaning: str = Field(
        default="",
        max_length=300,
        description="Requested relationship; ranks paths using retained labels.",
    )
    via: list[str] = Field(
        default_factory=list,
        max_length=5,
        description="Required intermediate classes in traversal order.",
    )
    offset: int = Field(default=0, ge=0)


class FollowArgs(Args):
    """Follow a named generated field from a retained set of records."""

    source: str
    target: str
    via: str | None = None
    value: str | None = None
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
    """Compile one connected retrieval from discovered references.

    Every class, route and field is a pattern. For example a class uses
    {reference: type_ref, bindings: [record]}, and its name field uses
    {reference: field_ref, bindings: [record, Name], optional: true}.
    outputs=[record, Name] returns both. The client writes all SPARQL.
    """

    patterns: list[QueryPattern] = Field(min_length=1, max_length=30)
    outputs: list[str] = Field(
        min_length=1,
        max_length=30,
        description="Requested column names. Each must appear in a pattern's bindings.",
    )
    values: dict[str, str] = Field(
        default_factory=dict,
        description="Required role to an exact retained RDF term reference; use patterns for fields.",
    )
    text: dict[str, str] = Field(
        default_factory=dict,
        description="Required literal field role to text explicitly requested in that field.",
    )
    distinct: bool = True
    grounding: dict[str, Grounding] = Field(
        default_factory=dict,
        description="Usually omit. Select evidence here only to resolve a reported semantic ambiguity.",
    )


class ProbeArgs(Args):
    """Profile a bounded sample of a prepared query."""

    query_ref: str
    limit: int = Field(default=5, ge=1, le=20)


class FinishArgs(Args):
    """Execute the current prepared query once through Client/SparqlHelper.

    Returns a receipt, strategy, warnings and trace; data stays in caller artifacts.
    Report those facts briefly, preserving unresolved interpretation warnings.
    """

    query_ref: str


CONTRACTS: dict[str, tuple[type[Args], str]] = {
    "rdf_schema": (SchemaArgs, "schema"),
    "rdf_find": (FindArgs, "find"),
    "rdf_paths": (PathsArgs, "paths"),
    "rdf_follow": (FollowArgs, "follow"),
    "rdf_inspect": (InspectArgs, "inspect"),
    "rdf_prepare": (PrepareArgs, "prepare"),
    "rdf_probe": (ProbeArgs, "probe"),
    "rdf_finish": (FinishArgs, "finish"),
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
    model, method = CONTRACTS[name]
    step_index = len(session.client._steps)
    try:
        with session.lock, session.client.step(f"MCP {method}"):
            args = model.model_validate(arguments).model_dump(exclude_none=True)
            value: dict[str, Any] = getattr(session, method)(**args)
    except ValidationError as exc:
        value = {
            "error": {
                "code": "invalid_arguments",
                "details": exc.errors(
                    include_input=False, include_url=False, include_context=False
                ),
            }
        }
        if name == "rdf_find" and arguments.get("text") == "":
            value["repair"] = (
                "To list every member, use rdf_schema to select its class and fields, then rdf_prepare with their references and result roles. Entity discovery needs a particular name."
            )
    except QuerySyntaxError as exc:
        value = {"error": exc.detail}
    except QueryValidationError as exc:
        value = {"error": {"code": exc.code, "message": str(exc)}}
        if exc.goal:
            value.update(goal=exc.goal, requirement=exc.requirement)
        value["selected_evidence"] = session._page(exc.evidence, budget=2500)
    except HydrationLimitError as exc:
        value = {
            "error": {
                "code": "search_budget",
                "message": str(exc),
                "retry": "Choose a discovered class and a more specific name."
                if name == "rdf_find"
                else "Narrow endpoints or increase path budget.",
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
    except ValueError as exc:
        value = {"error": {"code": "invalid_request", "message": str(exc)}}
    except Exception:
        logging.getLogger(__name__).exception("Package operation %s failed", method)
        value = {
            "error": {
                "code": "package_error",
                "message": "A package operation failed. Inspect the correlated server log.",
            }
        }
    if name == "rdf_prepare" and "error" in value:
        grounding = arguments.get("grounding", {})
        grounding = grounding if isinstance(grounding, dict) else {}
        refs = list(
            dict.fromkeys(
                ref
                for goal in grounding.values()
                if isinstance(goal, dict)
                for ref in goal.get("evidence", [])
                if isinstance(ref, str) and ref in session.catalogue.fragments
            )
        )
        if refs or "selected_evidence" not in value:
            value["selected_evidence"] = session._page(refs, budget=2500)
        value["repair"] = (
            "Select the returned class, field or path references with consistent result roles. Use rdf_schema with the goal concept and owner if this evidence means something else."
        )
        if value["error"]["code"] in {"goal_owner", "goal_type", "unresolved_goals"}:
            value["repair"] = (
                "Use the selected field's typed pattern. Inspect the requested owner and select evidence for the original meaning. Correct a mistaken vocabulary guess only when retained metadata supports it; omit goals. A valid field is not evidence for a different requested meaning."
            )
            value["retained_requirements"] = {
                key: requirement.model_dump() for key, requirement in session.requirements.items()
            }
    if name == "rdf_schema" and "error" in value and session.requirements:
        value["retained_requirements"] = {
            key: requirement.model_dump() for key, requirement in session.requirements.items()
        }
    value = to_jsonable_python(value)
    if len(json.dumps(value).encode()) > 12000:
        value = {
            "error": {
                "code": "observation_budget",
                "message": "Refine the selected concepts or owners. Full evidence is retained.",
            }
        }
    if len(session.client._steps) > step_index:
        step = session.client._steps[step_index]
        if "error" in value or value.get("state") == "failed":
            step["status"] = "failed"
        value["trace"] = {
            "step": step_index + 1,
            "operation": method,
            "query_ids": step["query_ids"],
            "status": step["status"],
        }
    session.client._tool_calls.append(
        {
            "id": len(session.client._tool_calls) + 1,
            "tool": name,
            "arguments": arguments,
            "status": value.get("state", "failed" if "error" in value else "complete"),
            "query_ids": value.get("trace", {}).get("query_ids", []),
        }
    )
    if session.log_path:
        session.log_path.parent.mkdir(parents=True, exist_ok=True)
        session.client.save_session(session.log_path, incremental=True)
    return value


def create_server(client: Client, **kwargs: Any) -> Server[Any]:
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

    session = Session(client, **kwargs)

    async def list_tools(
        ctx: ServerRequestContext[Any], params: PaginatedRequestParams | None
    ) -> ListToolsResult:
        """List the tool contracts."""
        return ListToolsResult(
            tools=[
                Tool(name=n, description=m.__doc__, input_schema=m.model_json_schema())
                for n, (m, _) in CONTRACTS.items()
            ]
        )

    async def call_tool(
        ctx: ServerRequestContext[Any], params: CallToolRequestParams
    ) -> CallToolResult:
        """Run one tool call in a worker thread."""
        value = await asyncio.to_thread(dispatch, session, params.name, params.arguments or {})
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(value, ensure_ascii=False))],
            structured_content=value,
            is_error="error" in value,
        )

    async def read_resource(
        ctx: ServerRequestContext[Any], params: ReadResourceRequestParams
    ) -> ReadResourceResult:
        """Return the session diagnostics resource."""
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


async def run_server(client: Client, **kwargs: Any) -> None:
    """Run a dedicated stdio investigation."""
    from mcp.server.stdio import stdio_server

    server = create_server(client, **kwargs)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())
