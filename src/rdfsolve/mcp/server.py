"""MCP server exposing the 4-operation query contract.

Tools:
- query_start: Begin a query session with intent
- query_decide: Choose/revise/expand/reject decisions
- query_inspect: Read-only state inspection
- query_finish: Emit query or execute
"""

from __future__ import annotations

import json
import logging
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from rdfsolve.client_api import Client
from rdfsolve.mcp.query_service import Intent, QueryService

logger = logging.getLogger(__name__)


def create_server(client: Client, source_id: str | None = None) -> Server:
    """Create MCP server with 4-operation query contract.

    Args:
        client: RDF client for schema and data access
        source_id: Source identifier for the dataset

    Returns:
        Configured MCP server
    """
    server = Server("rdfsolve")
    service = QueryService(client, source_id=source_id)

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="query_start",
                description=(
                    "Begin a new query session with an intent. "
                    "Returns session ID, revision, state (choose/ready/blocked), "
                    "and a decision if routes are ambiguous."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "operation_id": {
                            "type": "string",
                            "description": "Unique operation identifier for replay",
                        },
                        "question": {
                            "type": "string",
                            "description": "Original question text (retained verbatim)",
                        },
                        "intent": {
                            "type": "object",
                            "description": "Typed intent with roles, where, select, distinct",
                            "properties": {
                                "roles": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "class_hint": {"type": "string"},
                                        },
                                        "required": ["id"],
                                    },
                                },
                                "where": {
                                    "type": "object",
                                    "description": "Expression tree with op: all/any/relation/bind/compare/different",
                                },
                                "select": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Role IDs to project",
                                },
                                "distinct": {
                                    "type": "boolean",
                                    "default": True,
                                },
                                "unparsed_requirements": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "text": {"type": "string"},
                                        },
                                    },
                                    "default": [],
                                },
                            },
                            "required": ["roles", "where", "select"],
                        },
                        "scope": {
                            "type": "string",
                            "description": "Optional source scope",
                        },
                    },
                    "required": ["operation_id", "question", "intent"],
                },
            ),
            Tool(
                name="query_decide",
                description=(
                    "Submit a decision action: choose an option, request more, "
                    "reject current options, expand search, or revise intent."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "session": {
                            "type": "string",
                            "description": "Session ID from query_start",
                        },
                        "revision": {
                            "type": "integer",
                            "description": "Current session revision",
                        },
                        "operation_id": {
                            "type": "string",
                            "description": "Unique operation identifier for replay",
                        },
                        "action": {
                            "type": "object",
                            "description": "Action to perform",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["choose", "more", "reject", "expand", "revise"],
                                },
                                "decision": {
                                    "type": "string",
                                    "description": "Decision ID (for choose/more/reject/expand)",
                                },
                                "option": {
                                    "type": "string",
                                    "description": "Option ID (for choose)",
                                },
                                "intent": {
                                    "type": "object",
                                    "description": "New intent (for revise)",
                                },
                            },
                            "required": ["type"],
                        },
                    },
                    "required": ["session", "revision", "operation_id", "action"],
                },
            ),
            Tool(
                name="query_inspect",
                description=(
                    "Read-only inspection of session state. "
                    "Targets: status, decision:ID, option:DID:OID, requirement:ID"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "session": {
                            "type": "string",
                            "description": "Session ID",
                        },
                        "revision": {
                            "type": "integer",
                            "description": "Session revision",
                        },
                        "target": {
                            "type": "string",
                            "description": "What to inspect: status, decision:ID, option:DID:OID, requirement:ID",
                        },
                    },
                    "required": ["session", "revision", "target"],
                },
            ),
            Tool(
                name="query_finish",
                description=(
                    "Emit the compiled query artifact or execute the query. "
                    "Actions: emit_query (read-only), execute (runs query)."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "session": {
                            "type": "string",
                            "description": "Session ID",
                        },
                        "revision": {
                            "type": "integer",
                            "description": "Session revision",
                        },
                        "action": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["emit_query", "execute"],
                                },
                                "operation_id": {
                                    "type": "string",
                                    "description": "Operation ID (for execute)",
                                },
                            },
                            "required": ["type"],
                        },
                    },
                    "required": ["session", "revision", "action"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        try:
            if name == "query_start":
                result = service.query_start(
                    operation_id=arguments["operation_id"],
                    question=arguments["question"],
                    intent=arguments["intent"],
                    scope=arguments.get("scope"),
                )
            elif name == "query_decide":
                result = service.query_decide(
                    session_id=arguments["session"],
                    revision=arguments["revision"],
                    operation_id=arguments["operation_id"],
                    action=arguments["action"],
                )
            elif name == "query_inspect":
                result = service.query_inspect(
                    session_id=arguments["session"],
                    revision=arguments["revision"],
                    target=arguments["target"],
                )
            elif name == "query_finish":
                result = service.query_finish(
                    session_id=arguments["session"],
                    revision=arguments["revision"],
                    action=arguments["action"],
                )
            else:
                result = {"error": {"code": "unknown_tool", "message": f"Unknown tool: {name}"}}

            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        except Exception as e:
            logger.exception("Tool %s failed", name)
            return [
                TextContent(
                    type="text",
                    text=json.dumps({"error": {"code": "internal_error", "message": str(e)}}),
                )
            ]

    return server


async def run_server(client: Client, source_id: str | None = None) -> None:
    """Run the MCP server on stdio."""
    server = create_server(client, source_id)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())
