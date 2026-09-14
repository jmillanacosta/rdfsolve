"""Small MCP adapter over rdfsolve.Client.workspace(). No second query engine."""
from rdfsolve.mcp.server import create_server, run_server

__all__ = ['create_server','run_server']
