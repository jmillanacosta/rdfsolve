"""RDFSolve: A library for RDF schema analysis and VoID generation.

Main modules:
- parser: VoidParser class for parsing VoID descriptions and schemas
- config: RDF configuration tools for YAML model processing (separate module)
- utils: Common utility functions for RDF processing
"""

# Import parser and models 
from .parser import VoidParser
from .parser import parse_void_file as parse_void_simple
from .models import VoidSchema, SchemaTriple, LinkMLSchema
from . import utils

# Import version information
from .version import VERSION

__all__ = [
    "VoidParser",
    "parse_void_simple",
    "VoidSchema",
    "SchemaTriple", 
    "LinkMLSchema",
    "utils",
    "VERSION",
]
