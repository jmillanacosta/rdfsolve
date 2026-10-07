"""Ontology terms: the namespace a term belongs to, and the record of a term.

There is one rule for the namespace of a term, written twice: in Python, and as a SPARQL
expression for queries that group terms in the endpoint. Tests keep the two equal.
"""

from __future__ import annotations

import re
from typing import Any, TypedDict

from rdfsolve.ontology.vocabulary import OBO

# An OBO term (obo/MONDO_0000001) belongs to its prefix (obo/MONDO_), as written: FBbt, not FBBT.
_OBO_TERM = r"/obo/[A-Za-z][A-Za-z0-9]*_"
# One character at least: Virtuoso refuses a REPLACE pattern that matches the empty string
# (error 22023), and an empty last part leaves the IRI as it is either way.
_LAST_PART = r"[^/#:]+$"


def namespace(iri: str) -> str:
    """Return the namespace of a term: its OBO prefix (``obo/MONDO_``), else the IRI without its
    last part after /, # or :.
    """
    match = re.match(rf"(.*{_OBO_TERM})", iri)
    if match:
        return match.group(1)
    return re.sub(_LAST_PART, "", iri)


def namespace_expression(variable: str) -> str:
    """Return the SPARQL expression of :func:`namespace` for the IRI bound to *variable*."""
    text = f"STR({variable})"
    return (
        f'IF(REGEX({text}, "{_OBO_TERM}"), REPLACE({text}, "^(.*{_OBO_TERM}).*$", "$1"), '
        f'REPLACE({text}, "{_LAST_PART}", ""))'
    )


def obo_prefix(iri: str) -> str | None:
    """Return the OBO prefix of a term (the part before ``_`` of its OBO PURL), or None."""
    space = namespace(iri)
    if space.startswith(OBO) and space.endswith("_"):
        return space[len(OBO) : -1]
    return None


class Term(TypedDict, total=False):
    """One ontology term as a backend gives it, with where it came from.

    iri, label, description (definitions), synonyms and synonym_evidence (text, predicate,
    scope), ontology (the defining ontology), namespace (OBO namespace annotations), obsolete;
    parents (iri and label of direct named parents); provider, source, match, local_iri,
    fetched_at: the provenance of the answer.
    """

    iri: str
    label: str
    description: list[str]
    synonyms: list[str]
    synonym_evidence: list[dict[str, str]]
    ontology: str
    namespace: list[str]
    obsolete: bool
    parents: list[dict[str, Any]]
    provider: str
    source: str
    match: str
    local_iri: str
    fetched_at: float
    name_match: dict[str, str]


__all__ = ["Term", "namespace", "namespace_expression", "obo_prefix"]
