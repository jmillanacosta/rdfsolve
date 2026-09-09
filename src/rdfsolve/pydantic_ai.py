"""Expose the shared RDF operation session through PydanticAI."""

from __future__ import annotations

import json
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field
from pydantic_ai import Agent, ModelRetry, Tool
from pydantic_ai.models import Model
from pydantic_ai.settings import ModelSettings
from pydantic_ai.toolsets import FunctionToolset

from rdfsolve.client_api import Client
from rdfsolve.client_session import INSTRUCTIONS

if TYPE_CHECKING:
    from mcp import Client as MCPClient


async def mcp_tools(server: MCPClient, *, require_plan: bool = False) -> FunctionToolset[None]:
    """Read tools from a connected MCP client. The caller keeps it open during the run."""
    toolset: FunctionToolset[None] = FunctionToolset()
    planned = False

    def bind(name: str) -> Callable[..., Any]:
        """Bind one remote tool name without changing its argument schema."""

        async def call(**arguments: Any) -> Any:
            """Return structured results or let the model correct a failed call."""
            nonlocal planned
            if require_plan and name in {"search", "read", "answer"} and not planned:
                raise ModelRetry(
                    "Use schema to identify the requested classes, then plan the "
                    "source, targets and topic terms before querying records."
                )
            if require_plan and name == "search" and planned:
                from rdfsolve.mcp import read_plan

                progress = await read_plan(server)
                pending_routes = [
                    item for item in progress["pending"] if item.startswith("Follow a route")
                ]
                if pending_routes:
                    raise ModelRetry(
                        "Source candidates are already retained. Follow the planned "
                        "links with read(reference=..., paths=[...]) before another "
                        "text search. Leave fields empty to read destination names first. "
                        + json.dumps(pending_routes)
                    )
            result = await server.call_tool(name, arguments)
            if result.is_error:
                text = "\n".join(item.text for item in result.content if item.type == "text")
                raise ModelRetry(text or "MCP tool failed")
            if name == "plan":
                planned = True
            if result.structured_content is not None:
                if result.structured_content.get("retryable_by_agent") is False:
                    from rdfsolve.sparql_helper import EndpointError

                    raise EndpointError(str(result.structured_content["failure"]))
                return result.structured_content
            return [item.model_dump(mode="json") for item in result.content]

        return call

    for tool in (await server.list_tools()).tools:
        toolset.add_tool(
            Tool.from_schema(
                bind(tool.name),
                name=tool.name,
                description=tool.description,
                json_schema=tool.input_schema,
                sequential=True,
            )
        )
    return toolset


class QueryProposal(BaseModel):
    """Return a proposed query, not a claim that its answer was verified."""

    query: str | None = Field(description="Complete SELECT with prefixes, or null if unsupported")
    explanation: str = Field(description="Brief reason for the query or why it cannot be supplied")


class ResultReference(BaseModel):
    """Select retrieved records instead of asking a model to write data rows."""

    reference: str = Field(
        pattern=r"^[0-9a-f]{32}$",
        description="An existing result reference from a successful tool call. Read needed fields before returning it.",
    )


class ResearchAnswer(BaseModel):
    """Return an explanation and references to its retrieved data."""

    text: str = Field(
        description="Answer the question with supporting evidence and any unresolved parts."
    )
    results: list[ResultReference] = Field(
        description="Existing result references supporting the answer. Include each relevant record group, not only the starting records. Use an empty list if no data was retrieved."
    )


async def research_agent(
    server: MCPClient,
    model: str | Model,
    *,
    model_settings: ModelSettings | None = None,
    retries: int = 2,
) -> Agent[None, ResearchAnswer]:
    """Create an agent that returns prose and valid references from this MCP session.

    Reference validation reads only the server's result index. It does not validate
    scientific claims or substitute another result when the model chooses a bad ID.
    """
    from mcp.types import TextResourceContents

    agent = Agent(
        model,
        toolsets=[await mcp_tools(server, require_plan=True)],
        instructions=server.instructions,
        output_type=ResearchAnswer,
        model_settings=model_settings,
        retries=retries,
    )

    @agent.output_validator
    async def validate(answer: ResearchAnswer) -> ResearchAnswer:
        """Let the model correct unknown result references before accepting its answer."""
        response = await server.read_resource("rdfsolve://results", cache_mode="bypass")
        if len(response.contents) != 1 or not isinstance(
            response.contents[0], TextResourceContents
        ):
            raise ValueError("Expected a result index from the RDF server")
        available = json.loads(response.contents[0].text)
        known = {item["reference"] for item in available}
        if any(result.reference not in known for result in answer.results):
            raise ModelRetry(
                "Unknown result reference. Choose exact references from these retained results; "
                "do not repeat source queries merely to correct an ID: " + json.dumps(available)
            )
        unique = {result.reference: result for result in answer.results}
        covered = set(unique)
        for item in available:
            if item["reference"] in unique:
                covered.update(item.get("source_references", []))
        from rdfsolve.mcp import read_plan

        progress = await read_plan(server)
        if progress["pending"]:
            raise ModelRetry(
                "The requested answer is not investigated yet: " + json.dumps(progress["pending"])
            )
        missing = [
            row
            for row in progress["coverage"]
            if row["references"] and not set(row["references"]) & covered
        ]
        if missing:
            raise ModelRetry(
                "Include retrieved linked records for these requested classes: "
                + json.dumps(missing)
            )
        return answer.model_copy(update={"results": list(unique.values())})

    return agent


class ClientTools:
    """Register the shared session tools with PydanticAI."""

    def __init__(
        self,
        client: Client,
        *,
        source_id: str | None = None,
        preview_rows: int = 20,
        max_results: int = 50,
        max_records: int = 1000,
    ) -> None:
        """Create a bounded session. The caller owns and closes the client."""
        self.session = client.session(
            source_id=source_id or client._schema.about.dataset_name or "rdf",
            preview_rows=preview_rows,
            max_results=max_results,
            max_records=max_records,
        )
        self.toolset: FunctionToolset[None] = FunctionToolset()
        for function in self.session.functions.values():
            self.toolset.add_function(self._tool(function), sequential=True)

    def _tool(self, function: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(function)
        def run(**arguments: Any) -> Any:
            """Record the call and return validation errors to the model."""
            try:
                return function(**arguments)
            except (ValueError, LookupError) as error:
                raise ModelRetry(str(error)) from error

        return run

    def agent(self, model: str | Model, *, propose_query: bool = False) -> Agent[None, Any]:
        """Create an agent; pass UsageLimits when running it to bound spending."""
        return Agent(
            model,
            toolsets=[self.toolset],
            instructions=INSTRUCTIONS,
            output_type=QueryProposal if propose_query else str,
            model_settings={"max_tokens": 1500},
            retries={"tools": 1, "output": 1},
        )
