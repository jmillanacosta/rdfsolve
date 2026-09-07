"""Read typed RDF values at model boundaries."""

from __future__ import annotations

from rdflib import Literal
from rdflib.term import Node


def optional_count(value: Node | None) -> int | None:
    """Read a nonnegative integer literal. Preserve absent values and zero."""
    if value is None:
        return None
    if not isinstance(value, Literal):
        raise ValueError("A count must be an RDF literal")
    count = int(str(value))
    if count < 0:
        raise ValueError("A count must not be negative")
    return count
