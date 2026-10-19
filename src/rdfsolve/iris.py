"""IRI helpers: a CURIE from given prefixes and back, an IRI's local name and its namespace.

The prefixes are a mapping of prefix to namespace, as a JSON-LD context or a Turtle file
declares them.
"""

from __future__ import annotations

from rdfsolve._uri import curie_from_prefixes, get_local_name, make_expander
from rdfsolve.ontology.terms import namespace

__all__ = ["curie", "expand", "local_name", "namespace"]


def curie(iri: str, prefixes: dict[str, str]) -> tuple[str, str, str] | None:
    """Return ``(curie, prefix, namespace)`` of an IRI, by the longest namespace of *prefixes*
    that it starts with, or None when it starts with none.
    """
    return curie_from_prefixes(iri, prefixes)


def expand(curie: str, prefixes: dict[str, str]) -> str:
    """Return the IRI of a CURIE in *prefixes*; an IRI, or a CURIE of another prefix, is
    returned as it is.
    """
    return make_expander(prefixes)(curie)


def local_name(iri: str) -> str:
    """Return the last part of an IRI: after its ``#``, else after its last ``/``."""
    return get_local_name(iri)
