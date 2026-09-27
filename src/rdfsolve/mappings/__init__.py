"""Links between datasets and explicit mapping models."""

from rdfsolve.mappings.models.core import MappingEdge
from rdfsolve.mappings.signatures import (
    Link,
    LinkEvidence,
    infer_links,
    read_links,
    verify,
    write_links,
)

__all__ = [
    "Link",
    "LinkEvidence",
    "MappingEdge",
    "infer_links",
    "read_links",
    "verify",
    "write_links",
]
