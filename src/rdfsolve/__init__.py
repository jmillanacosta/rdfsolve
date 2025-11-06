"""RDFSolve: A library for RDF schema analysis and VoID generation.

Main modules:
- rdfsolve: Core RDFSolver class for SPARQL endpoints and VoID generation
- void_parser: VoidParser class for parsing VoID descriptions and schemas
- config: RDF configuration tools for YAML model processing (separate module)
- utils: Common utility functions for RDF processing
"""

from .rdfsolve import RDFSolver
from .void_parser import (
    VoidParser,
    parse_void_file,
    generate_void_from_endpoint,
)
from . import utils

# Import version information
from .version import VERSION

__all__ = [
    "RDFSolver",
    "VoidParser",
    "parse_void_file",
    "generate_void_from_endpoint",
    "utils",
    "VERSION",
]
