"""RDFSolve: A library for RDF schema analysis and VoID generation.

Main modules:
- parser: VoidParser class for parsing VoID descriptions and schemas
- config: RDF configuration tools for YAML model processing (separate module)
- utils: Common utility functions for RDF processing
"""

# Import parser and models
from . import utils
from .models import LinkMLSchema, SchemaTriple, VoidSchema
from .parser import VoidParser
from .parser import parse_void_file as parse_void_simple

# Import version information
from .version import VERSION

__all__ = [
    "VERSION",
    "LinkMLSchema",
    "SchemaTriple",
    "VoidParser",
    "VoidSchema",
    "parse_void_simple",
    "utils",
]
