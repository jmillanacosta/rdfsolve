"""MCP submodule implementing 4-operation query contract.

- query_start: Begin a query session with intent
- query_decide: Choose/revise/expand/reject decisions
- query_inspect: Read-only state inspection
- query_finish: Emit query or execute
"""

from rdfsolve.mcp.query_service import (
    QueryService,
    QueryState,
    BlockedReason,
    ErrorCode,
    Intent,
    RoleDef,
    WhereClause,
    RDFTerm,
    CompareRequirement,
    UnparsedRequirement,
    Session,
    Decision,
    DecisionOption,
    QueryArtifact,
    ResultArtifact,
)
from rdfsolve.mcp.server import create_server, run_server

__all__ = [
    "QueryService",
    "QueryState",
    "BlockedReason",
    "ErrorCode",
    "Intent",
    "RoleDef",
    "WhereClause",
    "RDFTerm",
    "CompareRequirement",
    "UnparsedRequirement",
    "Session",
    "Decision",
    "DecisionOption",
    "QueryArtifact",
    "ResultArtifact",
    "create_server",
    "run_server",
]
