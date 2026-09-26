"""Find the resources that the values of an answer show.

A column of an answer can show a resource as its IRI, as an identifier, as a web page or as
a name (a label, a title or a synonym). These are views of one resource. A value that is a
view of a resource is resolved to that resource, with the field that gives the view. Numbers,
long texts and values that are not views are kept as values.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from rdflib import Literal, URIRef

from rdfsolve.schema_models.enrichment import LABEL_PREDICATES, SYNONYM_PREDICATES

XSD = "http://www.w3.org/2001/XMLSchema#"
NAME_FIELDS = (
    *LABEL_PREDICATES,
    *SYNONYM_PREDICATES,
    "http://purl.org/dc/terms/alternative",
    "http://schema.org/name",
    "https://schema.org/name",
    "http://xmlns.com/foaf/0.1/name",
)
IDENTIFIER_FIELDS = (
    "http://purl.org/dc/elements/1.1/identifier",
    "http://purl.org/dc/terms/identifier",
    "http://www.w3.org/2004/02/skos/core#notation",
    "http://schema.org/identifier",
    "https://schema.org/identifier",
)
PAGE_FIELDS = (
    "http://xmlns.com/foaf/0.1/page",
    "http://xmlns.com/foaf/0.1/isPrimaryTopicOf",
    "http://schema.org/url",
    "https://schema.org/url",
)
# The kind of each view field; "self" is the IRI of the resource.
FIELD_KINDS = {
    **dict.fromkeys(NAME_FIELDS, "name"),
    **dict.fromkeys(IDENTIFIER_FIELDS, "identifier"),
    **dict.fromkeys(PAGE_FIELDS, "page"),
}
NUMBERS = {XSD + t for t in ("integer", "decimal", "double", "float", "long", "int", "short")}
Key = tuple[str, str, str, str]


@dataclass(frozen=True)
class View:
    """A value shows a resource through a field (a property IRI, or "self")."""

    entity: str
    field: str

    @property
    def kind(self) -> str:
        """Give the kind of the field: self, name, identifier or page."""
        return "self" if self.field == "self" else FIELD_KINDS.get(self.field, "other")


def term_key(term: dict[str, str]) -> Key:
    """Give a key of a SPARQL JSON term; a plain literal and an xsd:string literal are equal."""
    kind = "literal" if term["type"] == "typed-literal" else term["type"]
    datatype = term.get("datatype", "")
    if datatype == XSD + "string" or kind != "literal":
        datatype = ""
    return kind, term["value"], term.get("xml:lang", "").lower(), datatype


def resolvable(term: dict[str, str], longest: int = 300) -> bool:
    """Tell whether a value can be a view: an IRI, or a short literal that is not a number."""
    if term["type"] == "uri":
        return True
    key = term_key(term)
    return key[0] == "literal" and key[3] not in NUMBERS and len(key[1]) <= longest


def _n3(key: Key) -> list[str]:
    """Write a key as SPARQL terms; a string literal in its plain and xsd:string forms."""
    kind, value, language, datatype = key
    if kind == "uri":
        return [URIRef(value).n3()]
    if language or datatype:
        return [Literal(value, lang=language or None, datatype=datatype or None).n3()]
    return [Literal(value).n3(), Literal(value, datatype=URIRef(XSD + "string")).n3()]


def resolve(
    terms: Iterable[dict[str, str]],
    select: Callable[[str], list[dict[str, Any]]],
    fields: Iterable[str] = tuple(FIELD_KINDS),
    batch: int = 100,
) -> dict[Key, frozenset[View]]:
    """Give the views of each value; select runs a SPARQL SELECT and gives its bindings.

    An IRI is a view of itself. A value is also a view of each resource that has it as the
    value of a view field. Values that are not resolvable give no entry.
    """
    keys = sorted({term_key(t) for t in terms if resolvable(t)})
    views: dict[Key, set[View]] = {k: {View(k[1], "self")} for k in keys if k[0] == "uri"}
    predicates = " ".join(URIRef(f).n3() for f in fields)
    for start in range(0, len(keys), batch):
        values = " ".join(n3 for key in keys[start : start + batch] for n3 in _n3(key))
        query = (
            f"SELECT ?v ?r ?p WHERE {{ VALUES ?v {{ {values} }} VALUES ?p {{ {predicates} }} "
            "?r ?p ?v . FILTER(isIRI(?r)) }"
        )
        for row in select(query):
            view = View(row["r"]["value"], row["p"]["value"])
            views.setdefault(term_key(row["v"]), set()).add(view)
    return {key: frozenset(found) for key, found in views.items()}
