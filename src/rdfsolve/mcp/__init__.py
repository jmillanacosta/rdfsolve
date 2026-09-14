"""MCP submodule implementing 4-operation query contract.

- query_start: Begin a query session with intent
- query_decide: Choose/revise/expand/reject decisions
- query_inspect: Read-only state inspection
- query_finish: Emit query or execute
"""

from rdfsolve.mcp.query_service import (
    BlockedReason,
    CompareRequirement,
    Decision,
    DecisionOption,
    ErrorCode,
    Intent,
    QueryArtifact,
    QueryService,
    QueryState,
    RDFTerm,
    ResultArtifact,
    RoleDef,
    Session,
    UnparsedRequirement,
    WhereClause,
)

def __getattr__(name):
    if name in {"create_server", "run_server"}:
        from rdfsolve.mcp import server
        return getattr(server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BlockedReason",
    "CompareRequirement",
    "Decision",
    "DecisionOption",
    "ErrorCode",
    "Intent",
    "QueryArtifact",
    "QueryService",
    "QueryState",
    "RDFTerm",
    "ResultArtifact",
    "RoleDef",
    "Session",
    "UnparsedRequirement",
    "WhereClause",
    "create_server",
    "run_server",
]
