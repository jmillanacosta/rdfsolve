"""Expose the shared RDF operation session through PydanticAI."""

from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pydantic import BaseModel, Field
from pydantic_ai import Agent, ModelRetry, Tool
from pydantic_ai.models import Model
from pydantic_ai.run import AgentRunResult
from pydantic_ai.settings import ModelSettings
from pydantic_ai.toolsets import FunctionToolset
from pydantic_core import to_jsonable_python

from rdfsolve.client_api import Client
from rdfsolve.client_session import INSTRUCTIONS

if TYPE_CHECKING:
    from mcp import Client as MCPClient


def save_answer(
    result: AgentRunResult[Any], path: str | Path, *, question: str | None = None
) -> None:
    """Add the model answer and usage to a session log after closing its server."""
    path = Path(path)
    report = json.loads(path.read_text(encoding="utf-8"))
    report["agent"] = {
        "question": question,
        "output": to_jsonable_python(result.output),
        "usage": to_jsonable_python(result.usage),
        "messages": [
            {"model": message.model_name, "text": part.content}
            for message in result.new_messages()
            if message.kind == "response"
            for part in message.parts
            if part.part_kind == "text"
        ],
    }
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


async def mcp_tools(
    server: MCPClient, *, names: set[str] | None = None, path_first: bool = False
) -> FunctionToolset[None]:
    """Read tools from a connected MCP client. The caller keeps it open during the run."""
    toolset: FunctionToolset[None] = FunctionToolset()

    def bind(name: str) -> Callable[..., Any]:
        """Bind one remote tool name without changing its argument schema."""

        async def call(**arguments: Any) -> Any:
            """Return structured results or let the model correct a failed call."""
            if path_first and name == "answer":
                arguments["expand_links"] = True
            result = await server.call_tool(name, arguments)
            if result.is_error:
                text = "\n".join(item.text for item in result.content if item.type == "text")
                raise ModelRetry(text or "MCP tool failed")
            if result.structured_content is not None:
                if result.structured_content.get("retryable_by_agent") is False:
                    from rdfsolve.sparql_helper import EndpointError

                    failure = result.structured_content["failure"]
                    error = EndpointError if failure.get("kind") == "endpoint" else RuntimeError
                    raise error(f"{name}: {failure['category']}: {failure['message']}")
                return result.structured_content
            return [item.model_dump(mode="json") for item in result.content]

        return call

    for tool in (await server.list_tools()).tools:
        if names is not None and tool.name not in names:
            continue
        schema = deepcopy(tool.input_schema)
        description = tool.description
        if path_first and tool.name == "answer":
            for name in ("references", "where", "expand_links"):
                schema["properties"].pop(name, None)
            schema["additionalProperties"] = False
            description = (
                "Execute chosen path IDs with the plan's filters and return a final table. "
                "fields maps class names to extra fields; omit it for names and descriptions. "
                "Requested object fields extend the route to typed records. Paging is automatic. "
                "Search previews do not restrict this query. Use plan.where with iris for exact records."
            )
        toolset.add_tool(
            Tool.from_schema(
                bind(tool.name),
                name=tool.name,
                description=description,
                json_schema=schema,
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
        description="An exact reference returned by a successful tool call in this session.",
    )


class ResearchAnswer(BaseModel):
    """Return an explanation and references to its retrieved data."""

    text: str = Field(
        description="Answer the question with supporting evidence and any unresolved parts."
    )
    results: list[ResultReference] = Field(
        description="Final table references returned by answer. Usually one table. Use [] when no answer query could be executed."
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

    catalogue = await server.read_resource("rdfsolve://classes")
    classes = "\n".join(
        item.text for item in catalogue.contents if isinstance(item, TextResourceContents)
    )
    class_names = [item["label"] for item in json.loads(classes)]
    agent = Agent(
        model,
        toolsets=[
            await mcp_tools(server, names={"schema", "search", "plan", "answer"}, path_first=True)
        ],
        instructions=(
            f"{server.instructions}\nRequested object fields on the last route class are joined "
            f"to their typed records, keeping the full source route.\n"
            f"First {min(50, len(class_names))} of {len(class_names)} classes; use schema to find others:\n"
            f"{json.dumps(class_names[:50])}"
        ),
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
        final = {item["reference"] for item in available if item["rows"] is not None}
        if any(result.reference not in final for result in answer.results):
            raise ModelRetry(
                "Return a table from answer, not search candidates. Use plan to choose the "
                "requested source and target classes, then answer with the returned path IDs."
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
