"""Links between datasets and explicit mapping models."""

from rdfsolve.mappings.models.core import MappingEdge
from rdfsolve.mappings.signatures import (
    Link,
    LinkEvidence,
    infer_links,
    read_links,
    read_replacements,
    verify,
    write_links,
)
from rdfsolve.mappings.void import links_to_void

__all__ = [
    "Link",
    "LinkEvidence",
    "MappingEdge",
    "infer_links",
    "links_to_void",
    "read_links",
    "read_replacements",
    "verify",
    "write_links",
]
