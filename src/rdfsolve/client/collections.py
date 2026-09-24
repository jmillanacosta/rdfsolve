"""Explicit ordered RDF values."""

from __future__ import annotations

from hashlib import sha256
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field, field_validator
from rdflib import RDF, BNode, Graph, Literal, URIRef
from rdflib.term import Identifier

from rdfsolve.schema_models.enrichment import RdfTerm

Member = TypeVar("Member")


class RDFList(BaseModel, Generic[Member]):
    """An RDF collection whose members retain order and duplicates."""

    items: list[Member]
    identifier: str = Field(default_factory=lambda: str(BNode()), repr=False)

    @field_validator("items", mode="before")
    @classmethod
    def read_members(cls, values: Any) -> Any:
        """Accept RDF terms, model records and plain literal values."""
        if not isinstance(values, (list, tuple)):
            raise ValueError("RDFList items must be a list or tuple")
        return [
            RdfTerm.from_rdf(value) if isinstance(value, (Literal, URIRef, BNode)) else value
            for value in values
        ]


def member_patterns(extra: dict[str, Any]) -> list[dict[str, Any]]:
    """Return RDF kinds and datatypes permitted for collection members."""
    patterns = []
    for profile in extra.get("rdf_collections", []):
        kinds = set(profile["member_kinds"])
        if "IRI" in kinds or profile["member_types"]:
            patterns.append({"object_class": "Resource"})
        if "BlankNode" in kinds or profile["member_types"]:
            patterns.append({"object_class": "BlankNode"})
        if "Literal" in kinds:
            patterns.extend(
                {"object_class": "Literal", "datatype": datatype}
                for datatype in profile["member_datatypes"] or [None]
            )
    return patterns


def write_collection(
    value: RDFList[Any],
    extra: dict[str, Any],
    graph: Graph,
    seen: set[int],
    field: str,
) -> Identifier:
    """Write one collection with stable cells and its nested records."""
    from rdfsolve.client.authoring import _check_term, coerce_value
    from rdfsolve.client.hydration import class_iri
    from rdfsolve.client.model_rdf import _add_record, _new_term

    profiles = extra.get("rdf_collections", [])
    if not profiles:
        raise ValueError(f"{field}: no RDF collection definition")
    kinds = {kind for profile in profiles for kind in profile["member_kinds"]}
    classes = {cls for profile in profiles for cls in profile["member_types"]}
    datatypes = {dt for profile in profiles for dt in profile["member_datatypes"]}
    nodes = []
    for index, member in enumerate(value.items):
        location = f"{field}[{index}]"
        member = coerce_value(member, {"rdf_patterns": member_patterns(extra)}, location)
        if isinstance(member, (Literal, URIRef, BNode)):
            member = RdfTerm.from_rdf(member)
        if isinstance(member, RdfTerm):
            term = member
            _check_term(term, [], location)
            node = term.to_rdf()
        elif isinstance(member, BaseModel):
            if classes and class_iri(member) not in classes:
                raise ValueError(
                    f"{location}: got class {class_iri(member)!r}; expected {sorted(classes)}"
                )
            node = _new_term(member, {}, "")
            term = RdfTerm.from_rdf(node)
            _add_record(member, graph, seen)
        else:
            raise ValueError(f"{location}: got {member!r}; use RDF terms or typed records")
        kind = {"uri": "IRI", "bnode": "BlankNode", "literal": "Literal"}[term.kind]
        if kinds and kind not in kinds and not (classes and kind in {"IRI", "BlankNode"}):
            raise ValueError(
                f"{location}: got {kind} {term.value!r}; expected {sorted(kinds)}. "
                "Use URIRef for an IRI, BNode for a blank node, or a typed record."
            )
        datatype = (
            str(RDF.langString)
            if term.language
            else term.datatype or "http://www.w3.org/2001/XMLSchema#string"
        )
        if kind == "Literal" and datatypes and datatype not in datatypes:
            raise ValueError(
                f"{location}: got {term.value!r} with datatype {datatype}; "
                f"expected {sorted(datatypes)}"
            )
        nodes.append(node)
    if not nodes:
        return RDF.nil
    cells = [
        BNode("l" + sha256(f"{value.identifier}:{i}".encode()).hexdigest())
        for i in range(len(nodes))
    ]
    for i, node in enumerate(nodes):
        graph.add((cells[i], RDF.first, node))
        graph.add((cells[i], RDF.rest, cells[i + 1] if i + 1 < len(cells) else RDF.nil))
    return cells[0]
