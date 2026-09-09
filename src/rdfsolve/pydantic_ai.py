"""Expose the shared RDF operation session through PydanticAI."""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field
from pydantic_ai import Agent, ModelRetry, Tool
from pydantic_ai.models import Model
from pydantic_ai.toolsets import FunctionToolset

from rdfsolve.client_api import Client
from rdfsolve.session_tools import INSTRUCTIONS, SessionTools

if TYPE_CHECKING:
    from mcp import Client as MCPClient


async def mcp_tools(server: MCPClient) -> FunctionToolset[None]:
    """Read tools from a connected MCP client. The caller keeps it open during the run."""
    toolset: FunctionToolset[None] = FunctionToolset()

    def bind(name: str) -> Callable[..., Any]:
        """Bind one remote tool name without changing its argument schema."""

        async def call(**arguments: Any) -> Any:
            """Return structured results or let the model correct a failed call."""
            result = await server.call_tool(name, arguments)
            if result.is_error:
                text = "\n".join(item.text for item in result.content if item.type == "text")
                raise ModelRetry(text or "MCP tool failed")
            if result.structured_content is not None:
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


class ClientTools(SessionTools):
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
        super().__init__(
            client.session(
                source_id=source_id or client._schema.about.dataset_name or "rdf",
                preview_rows=preview_rows,
                max_results=max_results,
                max_records=max_records,
            )
        )
        self.toolset: FunctionToolset[None] = FunctionToolset()
        for function in self.functions.values():
            self.toolset.add_function(self._tool(function), sequential=True)

    def _tool(self, function: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(function)
        def run(**arguments: Any) -> Any:
            """Record the call and return validation errors to the model."""
            try:
                return self.invoke(function.__name__, arguments)
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
