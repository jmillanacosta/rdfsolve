"""Identifier signatures of mined schemas, and the links between datasets that they imply.

A schema example value is typed by Bioregistry when it is an IRI in a registered URI format,
or a literal CURIE of a registered prefix with a valid local identifier. A class is typed by
its example subjects in the same way. Two datasets are linked by a join when values of one
carry the identifier type of subjects of the other, and by a shared reference when values of
both carry the same identifier type. A link is a candidate: its evidence is the schema
examples, and sampled lookups in the target decide whether it holds.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from rdfsolve.schema_models.core import MinedSchema

if TYPE_CHECKING:
    from rdfsolve.client.api import Client

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
# Bioregistry prefixes of vocabularies: their terms describe data, they do not identify entities.
VOCABULARIES = frozenset(
    {
        "rdf",
        "rdfs",
        "owl",
        "xsd",
        "sd",
        "skos",
        "dcterms",
        "dc",
        "foaf",
        "void",
        "sh",
        "prov",
        "schema",
        "dcat",
        "idot",
        "shex",
        "vann",
        "bibo",
        "oa",
        "biolink",
    }
)


@dataclass
class Signatures:
    """Identifier types of one schema: of property values, and of class subjects."""

    values: dict[str, set[tuple[str, str]]] = field(default_factory=dict)
    subjects: dict[str, set[str]] = field(default_factory=dict)


@dataclass(frozen=True)
class Link:
    """A candidate link from a (class, property) of one dataset to another dataset."""

    kind: Literal["join", "shared"]
    source: str
    source_class: str
    property: str
    identifier_type: str
    target: str
    target_class: str | None = None  # the class whose subjects the values name (joins)
    target_property: str | None = None  # the property with the same identifier type (shared)


def identifier_type(value: str, ignore: Iterable[str] = VOCABULARIES) -> str | None:
    """Return the Bioregistry prefix of an identifier IRI or CURIE, or None."""
    import bioregistry

    if value.startswith(("http://", "https://")):
        prefix, _ = bioregistry.parse_iri(value) or (None, None)
    elif ":" in value and not any(c.isspace() for c in value):
        prefix, local = bioregistry.parse_curie(value) or (None, None)
        if prefix and not bioregistry.is_valid_identifier(prefix, local):
            prefix = None
    else:
        prefix = None
    prefix = bioregistry.normalize_prefix(prefix) if prefix else None
    return None if prefix is None or prefix in set(ignore) else prefix


def signatures(schema: MinedSchema, ignore: Iterable[str] = VOCABULARIES) -> Signatures:
    """Type the example values and subjects of a schema; rdf:type values are classes."""
    ignored = frozenset(ignore)
    values: dict[str, set[tuple[str, str]]] = defaultdict(set)
    subjects: dict[str, set[str]] = defaultdict(set)
    for example in schema.enrichment.examples if schema.enrichment else []:
        if example.property_uri == RDF_TYPE:
            continue
        if example.subject.kind == "uri" and (
            kind := identifier_type(example.subject.value, ignored)
        ):
            subjects[kind].add(example.subject_class)
        if kind := identifier_type(example.value.value, ignored):
            values[kind].add((example.subject_class, example.property_uri))
    return Signatures(dict(values), dict(subjects))


def infer_links(
    schemas: Mapping[str, MinedSchema], ignore: Iterable[str] = VOCABULARIES
) -> list[Link]:
    """Return the candidate joins and shared references between the datasets."""
    found = {name: signatures(schema, ignore) for name, schema in schemas.items()}
    links: set[Link] = set()
    for source, mine in found.items():
        for kind, pairs in mine.values.items():
            for target, theirs in found.items():
                if target == source:
                    continue
                for cls, prop in pairs:
                    for target_class in theirs.subjects.get(kind, ()):
                        links.add(
                            Link("join", source, cls, prop, kind, target, target_class=target_class)
                        )
                    if target > source and not theirs.subjects.get(kind):
                        for target_class, target_property in theirs.values.get(kind, ()):
                            links.add(
                                Link(
                                    "shared",
                                    source,
                                    cls,
                                    prop,
                                    kind,
                                    target,
                                    target_class,
                                    target_property,
                                )
                            )
    return sorted(
        links,
        key=lambda link: (
            link.kind,
            link.source,
            link.target,
            link.identifier_type,
            link.source_class,
            link.property,
        ),
    )


@dataclass
class LinkEvidence:
    """Sampled values of a link's source property, and how many the target has.

    target_forms counts how the target writes the identifiers that it has ({id} marks the
    local identifier): this is the rewrite that applies the link.
    """

    link: Link
    sampled: int
    found: int
    target_forms: dict[str, int]
    examples: list[tuple[str, str]]

    @property
    def share(self) -> float | None:
        """Return the share of sampled values that the target has."""
        return self.found / self.sampled if self.sampled else None


def _local(value: str) -> tuple[str, str] | None:
    """Return the Bioregistry prefix and the local identifier of an IRI or CURIE."""
    import bioregistry

    parsed = bioregistry.parse_iri(value) if value.startswith(("http://", "https://")) else None
    parsed = parsed or bioregistry.parse_curie(value)
    if not parsed or not parsed[0]:
        return None
    return bioregistry.normalize_prefix(parsed[0]) or parsed[0], parsed[1]


def verify(link: Link, source: Client, target: Client, *, sample: int = 50) -> LinkEvidence:
    """Look up up to *sample* values of the link's source property in the target.

    A join looks for the identifiers as subjects of the target class; a shared reference
    looks for them as values of the target property. Every spelling of an identifier is tried.
    """
    from rdflib import URIRef

    from rdfsolve.client.identify import spellings

    def _iri(value: str) -> str:
        return URIRef(value).n3()

    body = source._scope(f"?s a {_iri(link.source_class)} ; {_iri(link.property)} ?v .")
    rows = source._select(f"SELECT DISTINCT ?v WHERE {{ {body} }} LIMIT {sample}")
    keys: dict[str, str] = {}
    for row in rows:
        parsed = _local(row["v"]["value"])
        if parsed and parsed[0] == link.identifier_type:
            keys[f"{parsed[0]}:{parsed[1]}"] = row["v"]["value"]
    if link.kind == "join":
        pattern = f"?t a {_iri(link.target_class or '')} ."
        terms = {key: [t for t in spellings(key) if isinstance(t, URIRef)] for key in keys}
    else:
        pattern = f"?x {_iri(link.target_property or '')} ?t ."
        terms = {key: spellings(key) for key in keys}
    found: dict[str, str] = {}
    pairs = [(key, term) for key, forms in terms.items() for term in forms]
    for start in range(0, len(pairs), 200):
        values = " ".join(f'("{key}" {term.n3()})' for key, term in pairs[start : start + 200])
        scoped = target._scope(f"VALUES (?key ?t) {{ {values} }} {pattern}")
        for row in target._select(f"SELECT DISTINCT ?key ?t WHERE {{ {scoped} }}"):
            found.setdefault(row["key"]["value"], row["t"]["value"])
    forms: dict[str, int] = defaultdict(int)
    for key, term in found.items():
        local = key.split(":", 1)[1]
        forms[term.replace(local, "{id}") if term.endswith(local) else "literal"] += 1
    return LinkEvidence(
        link,
        len(keys),
        len(found),
        dict(forms),
        sorted((keys[key], term) for key, term in found.items()),
    )
