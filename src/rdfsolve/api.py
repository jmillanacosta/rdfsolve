"""Core API functionality for RDFSolve.

This module provides the main classes and functions for working with RDF datasets:
- RDFSolver: Main class for managing SPARQL endpoints and generating VoID
- VoidParser: Parser for VoID descriptions and schema extraction
"""

from .rdfsolve import RDFSolver
from .void_parser import (
    VoidParser,
    parse_void_file,
    generate_void_from_endpoint,
)

__all__ = [
    "RDFSolver",
    "VoidParser",
    "parse_void_file",
    "generate_void_from_endpoint",
]
