"""RDF Configuration Module.

This module provides functionality for parsing and processing RDF-config YAML
models. It includes tools for handling RDF configuration files following
the Ruby RDFConfig library conventions.
"""

from .rdf_config import (
    RDFConfigParser,
    process_multiple_sources,
    display_schema_sample,
    clean_and_normalize_schema,
    convert_to_target_format,
    normalize_uri,
)

__all__ = [
    "RDFConfigParser",
    "process_multiple_sources",
    "display_schema_sample",
    "clean_and_normalize_schema",
    "convert_to_target_format",
    "normalize_uri",
]
