"""RDFSolve: A library for RDF schema analysis and VoID generation.

Main modules:
- parser: VoidParser class for parsing VoID descriptions and schemas
- miner: SchemaMiner class for direct SPARQL schema mining
- config: RDF configuration tools for YAML model processing (separate module)
- utils: Common utility functions for RDF processing
- query: SPARQL query execution with structured results
- iri: IRI resolution against SPARQL endpoints
- compose: SPARQL query composition from diagram paths
"""

# Import parser and models
from . import utils
from .api import mine_all_sources
from .compose import compose_query_from_paths
from .iri import resolve_iris
from .miner import SchemaMiner
from .models import (
    AboutMetadata,
    LinkMLSchema,
    MinedSchema,
    SchemaPattern,
    SchemaTriple,
    VoidSchema,
)
from .parser import VoidParser
from .parser import parse_void_file as parse_void_simple
from .query import QueryResult, ResultCell, execute_sparql

# Import version information
from .version import VERSION

__all__ = [
    "AboutMetadata",
    "LinkMLSchema",
    "MinedSchema",
    "QueryResult",
    "ResultCell",
    "SchemaMiner",
    "SchemaPattern",
    "SchemaTriple",
    "VERSION",
    "VoidParser",
    "VoidSchema",
    "compose_query_from_paths",
    "execute_sparql",
    "mine_all_sources",
    "parse_void_simple",
    "resolve_iris",
    "utils",
]
