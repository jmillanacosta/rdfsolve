"""The parts of each class by how its members are identified, counted in the data.

The members of one class can be named in many ways: IRIs of several registered identifier
namespaces, IRIs of a namespace whose local part is not a valid identifier there, IRIs of other
namespaces, or blank nodes. Each way is a part of the class (ClassPart), counted over every
member. A part is published as a SHACL node shape without a target, so that a node belongs to
the part when it conforms to the shape (SHACL 3.5); the node shape of the class lists its parts
with sh:or, so that a member named in no counted way is reported. In VoID, a part is a subset
of the class partition with its number of entities, conforming to the part's shape.
"""

from __future__ import annotations

from hashlib import md5
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from rdflib import Graph, URIRef

    from rdfsolve.schema_models.shacl_model import ShaclNodeShape

__all__ = [
    "ClassPart",
    "ClassParts",
    "namespaces_pattern",
    "part_label",
    "part_shape_iri",
    "part_shape_iri_of_key",
]

PartKind = Literal[
    "identifier",
    "invalid_identifier",
    "namespace",
    "host",
    "relative_iri",
    "invalid_scheme",
    "blank_node",
]
_SPECIAL = set("\\.?*+(){}|[]^$")


class ClassPart(BaseModel):
    """One way in which members of a class are identified, and how many members are so.

    *kind*: identifier (a valid identifier of a registered *prefix*), invalid_identifier (an
    IRI in a registered namespace of *prefix* whose local part is not valid there), namespace
    (an IRI of none of the registered namespaces; *namespaces* holds its start: the scheme,
    host and first path segment), host (IRIs of one host whose namespaces in the class are too
    many to list, rdfsolve.mining.class_parts.NAMESPACES_PER_HOST; *namespaces* holds the
    scheme and host), relative_iri (an IRI without a scheme, which RDF does not
    allow), invalid_scheme (an IRI whose text before its first colon is no valid scheme, such
    as a blank-node label written as an IRI; *namespaces* holds that text and the colon) or
    blank_node. *namespaces* are the IRI starts the members
    are written with; *pattern* the prefix's pattern of local identifiers, where known.
    """

    kind: PartKind
    prefix: str | None = None
    namespaces: list[str] = Field(default_factory=list)
    pattern: str | None = None
    peels: list[str] = Field(
        default_factory=list,
        description="Prefixes a local identifier may be written with (any case), as read",
    )
    entities: int

    def key(self) -> str:
        """Return what distinguishes the part within its class: its kind and its prefix, or its
        namespace for IRIs of no registered namespace (part_key in rdfsolve.mining.class_parts).
        """
        if self.kind in ("namespace", "host", "invalid_scheme"):
            return f"{self.kind}|{self.namespaces[0] if self.namespaces else ''}"
        return f"{self.kind}|{self.prefix or ''}"


class ClassParts(BaseModel):
    """The parts of each class (*by_class*: class IRI to its parts, the largest first).

    *complete* is False when the parts were read from examples, not counted over every member.
    """

    by_class: dict[str, list[ClassPart]] = Field(default_factory=dict)
    complete: bool = True


def part_label(class_label: str, part: ClassPart) -> str:
    """Return the label of a part of a class, e.g. "DataNode identified in uniprot"."""
    if part.kind == "identifier":
        return f"{class_label} identified in {part.prefix}"
    if part.kind == "invalid_identifier":
        return (
            f"{class_label} with an IRI of {part.prefix} that is no valid {part.prefix} identifier"
        )
    if part.kind == "blank_node":
        return f"{class_label} as blank nodes"
    if part.kind == "relative_iri":
        return f"{class_label} with IRIs without a scheme"
    if part.kind == "invalid_scheme":
        scheme = part.namespaces[0] if part.namespaces else ""
        return f"{class_label} with IRIs whose scheme is not valid ({scheme})"
    return f"{class_label} with IRIs in {part.namespaces[0] if part.namespaces else 'no namespace'}"


def part_shape_iri(class_shape: str, part: ClassPart) -> str:
    """Return the IRI of the node shape of a part: the class shape's IRI and a hash of the part's
    key, so that it stays the same across releases while the part persists.
    """
    return part_shape_iri_of_key(class_shape, part.key())


def part_shape_iri_of_key(class_shape: str, key: str) -> str:
    """Return the IRI of the node shape of the part with this key (ClassPart.key)."""
    return f"{class_shape}-part-{md5(key.encode(), usedforsecurity=False).hexdigest()[:8]}"


def _escape(text: str) -> str:
    """Return *text* as a regular expression that matches it literally."""
    return "".join("\\" + c if c in _SPECIAL else c for c in text)


def _any_case(text: str) -> str:
    """Return the regular expression of *text* in any case."""
    return "".join(
        f"[{c.lower()}{c.upper()}]" if c.lower() != c.upper() else _escape(c) for c in text
    )


def _exclusions(parts: list[ClassPart]) -> dict[int, list[ClassPart]]:
    """Return, for each part (by position), the parts whose IRIs also match its namespaces, so
    that it excludes them: a longer namespace that starts with one of its own, or the same
    namespace when it is a part of no registered namespace and the other a valid identifier.

    The namespaces are sorted once and each part finds those that start with its own by
    bisection, so classes of many parts are handled without comparing every pair.
    """
    from bisect import bisect_left

    named = sorted(
        (namespace, index)
        for index, part in enumerate(parts)
        if part.kind not in ("blank_node", "relative_iri")
        for namespace in part.namespaces
    )
    keys = [namespace for namespace, _ in named]
    found: dict[int, list[ClassPart]] = {}
    for index, outer in enumerate(parts):
        if outer.kind in ("blank_node", "relative_iri"):
            continue
        inside: dict[int, None] = {}
        for start in outer.namespaces:
            at = bisect_left(keys, start)
            while at < len(keys) and keys[at].startswith(start):
                other = named[at][1]
                inner = parts[other]
                if other != index and (
                    keys[at] != start or (outer.kind == "namespace" and inner.kind == "identifier")
                ):
                    inside[other] = None
                at += 1
        found[index] = [parts[other] for other in inside]
    return found


def namespaces_pattern(namespaces: list[str]) -> str:
    """Return the regular expression of IRIs that start with one of *namespaces*."""
    return "^(" + "|".join(_escape(n) for n in sorted(set(namespaces))) + ")"


def part_shapes(class_shape: str, class_label: str, parts: list[ClassPart]) -> list[ShaclNodeShape]:
    """Return the node shapes of the parts of one class (without targets).

    An identifier part matches its namespaces followed by a valid local identifier; an invalid
    identifier part matches its namespaces and not the identifier part of the same prefix.
    """
    from rdfsolve.schema_models.shacl_model import ShaclNodeShape

    shapes: list[ShaclNodeShape] = []
    valid = {p.prefix: part_shape_iri(class_shape, p) for p in parts if p.kind == "identifier"}
    excluded = _exclusions(parts)
    for index, part in enumerate(parts):
        uri = part_shape_iri(class_shape, part)
        shape = ShaclNodeShape(
            uri=uri,
            name=part_label(class_label, part),
            description=f"{part.entities} members counted in the data",
        )
        if part.kind == "blank_node":
            shape.node_kind = "BlankNode"
        elif part.kind == "relative_iri":
            # A parser resolves an IRI without a scheme against its base IRI, so the IRI a
            # validator reads is not the one written: the part is any IRI.
            shape.description = (
                f"{part.entities} members counted in the data, written without a scheme in the "
                "source; read as IRIs resolved against the base of the reader"
            )
        elif part.kind == "identifier" and part.pattern:
            # XPath regular expressions (sh:pattern) have no non-capturing groups.
            local = part.pattern.removeprefix("^").removesuffix("$").replace("(?:", "(")
            peels = "|".join(_any_case(p) for p in sorted(set(part.peels)))
            shape.pattern = (
                namespaces_pattern(part.namespaces)
                + (f"({peels})?" if peels else "")
                + "("
                + local
                + ")$"
            )
        else:
            shape.pattern = namespaces_pattern(part.namespaces)
        if part.kind == "invalid_identifier" and part.prefix in valid:
            shape.not_shapes = [valid[part.prefix]]
        shape.not_shapes += [
            part_shape_iri(class_shape, other) for other in excluded.get(index, [])
        ]
        if part.kind != "blank_node":
            shape.node_kind = "IRI"
        shapes.append(shape)
    return shapes


def add_void_parts(
    graph: Graph, class_partition: URIRef, class_shape: str, parts: list[ClassPart]
) -> None:
    """Add the parts of a class to its VoID class partition: one subset each, with its entities,
    its URI space (one namespace) or URI pattern, and the part's shape (dcterms:conformsTo).
    """
    from rdflib import Literal, URIRef
    from rdflib.namespace import DCTERMS, RDF, XSD, Namespace

    void = Namespace("http://rdfs.org/ns/void#")
    for part in parts:
        shape = part_shape_iri(class_shape, part)
        node = URIRef(str(class_partition) + "-part-" + shape.rsplit("-part-", 1)[1])
        graph.add((class_partition, void.subset, node))
        graph.add((node, RDF.type, void.Dataset))
        graph.add((node, void.entities, Literal(part.entities, datatype=XSD.integer)))
        graph.add((node, DCTERMS.conformsTo, URIRef(shape)))
        if part.kind == "relative_iri":
            graph.add((node, void.uriRegexPattern, Literal("^[^:]*$")))
        elif part.kind != "blank_node" and len(part.namespaces) == 1:
            graph.add((node, void.uriSpace, Literal(part.namespaces[0])))
        elif part.kind != "blank_node":
            graph.add((node, void.uriRegexPattern, Literal(namespaces_pattern(part.namespaces))))
