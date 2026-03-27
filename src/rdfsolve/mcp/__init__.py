"""rdfsolve MCP (Model Context Protocol) layer.

Provides natural-language-to-SPARQL grounding, planning, and execution
on top of the core rdfsolve stack.

Sub-packages
------------
ontology
    OLS4 client, OntologyIndex build/load, ontology connectivity graph.
grounding
    Six-tier entity grounding engine (IRI → regex → CURIE → OLS → lexical
    → graph constraint).
intent
    Pydantic IR schema and LLM-backed intent parser.
planning
    Path search, PathScorer, PlanEngine.
execution
    SPARQL compilation and execution via rdfsolve SparqlHelper.
provenance
    Per-row provenance tracking.
templates
    SPARQL path-template registry.
"""

from __future__ import annotations

__all__: list[str] = []
