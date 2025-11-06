"""Wraps several RDF schema solver tools."""

from .api import hello, square
from .shapes_graph import (
    RDFConfigParser,
    process_multiple_sources,
    display_schema_sample,
    clean_and_normalize_schema,
    convert_to_target_format,
    normalize_uri,
)

# being explicit about exports is important!
__all__ = [
    "hello",
    "square",
    "RDFConfigParser",
    "process_multiple_sources",
    "display_schema_sample",
    "clean_and_normalize_schema",
    "convert_to_target_format",
    "normalize_uri",
]
