"""Grounded RDF discovery, retrieval and model integration.

ask_rdf(..., max_response_tokens=4096) caps each model response, including reasoning.
Pass a positive integer to change the ceiling or None to disable it. Provider
limits and whole-run usage_limits remain independent. External MCP hosts control
their own model generation settings.
"""

from rdfsolve.mcp.workflow import ask_rdf

__all__ = ["ask_rdf"]
