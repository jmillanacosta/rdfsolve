"""Read SHACL paths with cycle and RDF list checks, and SPARQL property path text."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Literal

from rdflib import RDF, SH, BNode, Graph, URIRef
from rdflib.term import Node

from rdfsolve.schema_models.paths import PropertyPath


def read_path(graph: Graph, node: Node, active: frozenset[Node] = frozenset()) -> PropertyPath:
    """Read the SHACL Core path operators. Reject cycles and ambiguous forms."""
    if isinstance(node, URIRef):
        return PropertyPath(operator="predicate", iri=str(node))
    if not isinstance(node, BNode) or node in active:
        raise ValueError("A SHACL path must be an IRI or an acyclic blank-node expression")
    active = active | {node}
    operators = {
        SH.alternativePath: "alternative",
        SH.inversePath: "inverse",
        SH.zeroOrMorePath: "zero_or_more",
        SH.oneOrMorePath: "one_or_more",
        SH.zeroOrOnePath: "zero_or_one",
    }
    forms = [
        (predicate, value) for predicate in operators for value in graph.objects(node, predicate)
    ]
    sequence = (node, RDF.first, None) in graph
    if len(forms) + int(sequence) != 1:
        raise ValueError("A SHACL path must have exactly one path operator")
    if sequence or (forms and forms[0][0] == SH.alternativePath):
        head = node if sequence else forms[0][1]
        members = []
        seen = set()
        while head != RDF.nil:
            if head in seen:
                raise ValueError("Cyclic SHACL path list")
            seen.add(head)
            first = list(graph.objects(head, RDF.first))
            rest = list(graph.objects(head, RDF.rest))
            if len(first) != 1 or len(rest) != 1:
                raise ValueError("Malformed SHACL path list")
            members.append(read_path(graph, first[0], active))
            head = rest[0]
        return PropertyPath(operator="sequence" if sequence else "alternative", items=members)
    predicate, value = forms[0]
    return PropertyPath.model_validate(
        {"operator": operators[predicate], "items": [read_path(graph, value, active)]}
    )


_PATH_TOKEN = re.compile(
    r"<[^<>\"{}|^`\\\s]*>|[A-Za-z][\w.-]*:[\w.-]*|:[\w.-]*|a(?![\w:])|[\^/|*+?()]"
)
_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_REPEAT: dict[str, Literal["zero_or_more", "one_or_more", "zero_or_one"]] = {
    "*": "zero_or_more",
    "+": "one_or_more",
    "?": "zero_or_one",
}


class _SparqlPathReader:
    """Read the property path syntax of SPARQL 1.1 (without negated property sets)."""

    def __init__(self, text: str, prefixes: Mapping[str, str]) -> None:
        """Split the text into parts. Text that is not part of a path is refused."""
        self.prefixes = prefixes
        self.parts: list[tuple[int, str]] = []
        position = 0
        for match in _PATH_TOKEN.finditer(text):
            if text[position : match.start()].strip():
                raise ValueError(f"Unexpected text at position {position}: {text[position:]!r}")
            self.parts.append((match.start(), match.group()))
            position = match.end()
        if text[position:].strip():
            raise ValueError(f"Unexpected text at position {position}: {text[position:]!r}")
        self.index = 0

    def peek(self) -> str | None:
        """Return the next part without reading it."""
        return self.parts[self.index][1] if self.index < len(self.parts) else None

    def take(self) -> str:
        """Read the next part."""
        part = self.peek()
        if part is None:
            raise ValueError("Expected a path element at the end of the path")
        self.index += 1
        return part

    def path(self) -> PropertyPath:
        """Read alternatives of sequences."""
        options = [self.sequence()]
        while self.peek() == "|":
            self.take()
            options.append(self.sequence())
        return (
            options[0] if len(options) == 1 else PropertyPath(operator="alternative", items=options)
        )

    def sequence(self) -> PropertyPath:
        """Read steps joined by ``/``."""
        steps = [self.step()]
        while self.peek() == "/":
            self.take()
            steps.append(self.step())
        return steps[0] if len(steps) == 1 else PropertyPath(operator="sequence", items=steps)

    def step(self) -> PropertyPath:
        """Read an element with a repetition. After ``^`` the whole is in inverse."""
        inverse = self.peek() == "^"
        if inverse:
            self.take()
        element = self.primary()
        repeat = _REPEAT.get(self.peek() or "")
        if repeat:
            self.take()
            element = PropertyPath(operator=repeat, items=[element])
        return PropertyPath(operator="inverse", items=[element]) if inverse else element

    def primary(self) -> PropertyPath:
        """Read a predicate, ``a``, or a group in brackets."""
        position = self.parts[self.index][0] if self.index < len(self.parts) else None
        part = self.take()
        if part == "(":
            inner = self.path()
            if self.peek() != ")":
                raise ValueError(f"Expected ')' to close the group at position {position}")
            self.take()
            return inner
        if part == "a":
            return PropertyPath(operator="predicate", iri=_RDF_TYPE)
        if part.startswith("<"):
            return PropertyPath(operator="predicate", iri=part[1:-1])
        prefix, colon, local = part.partition(":")
        if not colon:
            raise ValueError(f"Expected a path element at position {position}, not {part!r}")
        if prefix not in self.prefixes:
            raise ValueError(f"Unknown prefix: {prefix}")
        return PropertyPath(operator="predicate", iri=self.prefixes[prefix] + local)


def read_sparql_path(text: str, prefixes: Mapping[str, str] | None = None) -> PropertyPath:
    """Read a SPARQL 1.1 property path, for example ``^(schema:author/rdf:rest*/rdf:first)``.

    Prefixed names use *prefixes*; ``a`` is rdf:type. Negated property sets have no SHACL form
    and are not read.
    """
    reader = _SparqlPathReader(text, prefixes or {})
    path = reader.path()
    if reader.peek() is not None:
        raise ValueError(f"Expected the end of the path, not {reader.peek()!r}")
    return path
