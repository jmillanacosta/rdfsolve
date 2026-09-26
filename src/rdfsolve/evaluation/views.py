"""Find the resources that the values of an answer show.

A column of an answer can show a resource as its IRI, as a name (a label, a title or a
synonym), as an identifier, as a web page, or as the value of a key field of its class (for
example a CAS number or an HGNC identifier). These are views of one resource. A value is
resolved to each resource that it is a view of, with the field of the view. Numbers, long
texts and the values of other fields are kept as values.

A key field is found in the data: its literal values are short (at most 64 characters),
each resource has one value, and each value belongs to one resource (both for at least 98%
of the statements). Links between records (owl:sameAs, skos:exactMatch) are kept apart, as
views of kind "link", for a more lenient comparison.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from rdflib import Literal, URIRef

from rdfsolve.schema_models.enrichment import LABEL_PREDICATES, SYNONYM_PREDICATES

XSD = "http://www.w3.org/2001/XMLSchema#"
NAME_FIELDS = (
    *LABEL_PREDICATES,
    "http://schema.org/name",
    "https://schema.org/name",
    "http://xmlns.com/foaf/0.1/name",
    "http://purl.org/dc/terms/alternative",
    *SYNONYM_PREDICATES,
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
LINK_FIELDS = (
    "http://www.w3.org/2002/07/owl#sameAs",
    "http://www.w3.org/2004/02/skos/core#exactMatch",
)
FIELD_KINDS = {
    **dict.fromkeys(NAME_FIELDS, "name"),
    **dict.fromkeys(IDENTIFIER_FIELDS, "identifier"),
    **dict.fromkeys(PAGE_FIELDS, "page"),
    **dict.fromkeys(LINK_FIELDS, "link"),
}
# The preferred field when one value shows a resource through several fields.
FIELD_ORDER = {
    "self": 0,
    **{f: i + 1 for i, f in enumerate([*NAME_FIELDS, *IDENTIFIER_FIELDS, *PAGE_FIELDS])},
}
NUMBERS = {XSD + t for t in ("integer", "decimal", "double", "float", "long", "int", "short")}
Key = tuple[str, str, str, str]
Select = Callable[[str], list[dict[str, Any]]]


@dataclass(frozen=True)
class View:
    """A value shows a resource through a field (a property IRI, or "self")."""

    entity: str
    field: str

    @property
    def kind(self) -> str:
        """Give the kind of the field: self, name, identifier, page, link or key."""
        return "self" if self.field == "self" else FIELD_KINDS.get(self.field, "key")


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


def _batches(keys: Sequence[Key], size: int) -> Iterator[str]:
    """Give the keys as VALUES lists of at most `size` keys."""
    for start in range(0, len(keys), size):
        yield " ".join(n3 for key in keys[start : start + size] for n3 in _n3(key))


def is_key_field(field: str, select: Select, cache: dict[str, bool] | None = None) -> bool:
    """Tell whether the literal values of a field identify its resources (see the module)."""
    if cache is not None and field in cache:
        return cache[field]
    rows = select(
        "SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?s) AS ?subjects) (COUNT(DISTINCT ?o) AS ?objects) "
        f"(MAX(STRLEN(STR(?o))) AS ?longest) WHERE {{ ?s {URIRef(field).n3()} ?o FILTER(isLiteral(?o)) }}"
    )
    counts = {k: int(float(v["value"])) for k, v in (rows[0] if rows else {}).items()}
    total = counts.get("n", 0)
    found = bool(
        total
        and counts.get("subjects", 0) >= 0.98 * total
        and counts.get("objects", 0) >= 0.98 * total
        and counts.get("longest", 0) <= 64
    )
    if cache is not None:
        cache[field] = found
    return found


def resolve(
    terms: Iterable[dict[str, str]],
    select: Select,
    *,
    batch: int = 100,
    keys: bool = True,
    links: bool = True,
    key_cache: dict[str, bool] | None = None,
) -> dict[Key, frozenset[View]]:
    """Give the views of each value; select runs a SPARQL SELECT and gives its bindings.

    An IRI is a view of itself, and of each resource that has it as an identifier or page.
    A literal is a view of each resource that has it as a name, an identifier or the value
    of a key field (keys=True). With links=True, the resources linked to these resources
    by owl:sameAs or skos:exactMatch are added as views of kind "link".
    """
    wanted = sorted({term_key(t) for t in terms if resolvable(t)})
    iris = [k for k in wanted if k[0] == "uri"]
    literals = [k for k in wanted if k[0] == "literal"]
    views: dict[Key, set[View]] = {k: {View(k[1], "self")} for k in iris}

    def add(values: Sequence[Key], fields: Iterable[str]) -> None:
        """Add the resources that have the values as values of the fields."""
        predicates = " ".join(URIRef(f).n3() for f in fields)
        if not predicates:
            return
        for values_list in _batches(values, batch):
            query = (
                f"SELECT ?v ?r ?p WHERE {{ VALUES ?v {{ {values_list} }} VALUES ?p {{ {predicates} }} "
                "?r ?p ?v . FILTER(isIRI(?r)) }"
            )
            for row in select(query):
                views.setdefault(term_key(row["v"]), set()).add(
                    View(row["r"]["value"], row["p"]["value"])
                )

    add(iris, [*IDENTIFIER_FIELDS, *PAGE_FIELDS])
    candidates: set[str] = set()
    for values_list in _batches(literals, batch):
        query = f"SELECT DISTINCT ?p WHERE {{ VALUES ?v {{ {values_list} }} ?r ?p ?v . FILTER(isIRI(?r)) }}"
        candidates.update(row["p"]["value"] for row in select(query))
    fields = [f for f in sorted(candidates) if FIELD_KINDS.get(f) in {"name", "identifier"}]
    if keys:
        fields += [
            f
            for f in sorted(candidates)
            if f not in FIELD_KINDS and is_key_field(f, select, key_cache)
        ]
    add(literals, fields)
    if links:
        entities = sorted({v.entity for found in views.values() for v in found})
        linked: dict[str, set[View]] = {}
        predicates = " ".join(URIRef(f).n3() for f in LINK_FIELDS)
        for values_list in _batches([("uri", e, "", "") for e in entities], batch):
            query = (
                f"SELECT ?e ?n ?l WHERE {{ VALUES ?e {{ {values_list} }} VALUES ?l {{ {predicates} }} "
                "{ ?e ?l ?n } UNION { ?n ?l ?e } FILTER(isIRI(?n)) }"
            )
            for row in select(query):
                linked.setdefault(row["e"]["value"], set()).add(
                    View(row["n"]["value"], row["l"]["value"])
                )
        for found in views.values():
            found.update(*(linked.get(v.entity, set()) for v in list(found)))
    return {key: frozenset(found) for key, found in views.items()}
