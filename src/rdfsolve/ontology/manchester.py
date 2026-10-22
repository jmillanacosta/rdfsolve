"""Write OWL class expressions given as RDF (blank nodes) in Manchester syntax.

Follows the mapping of class expressions to RDF of OWL 2 (OWL 2 Mapping to RDF Graphs,
section 3.2.4, tables 13-15) and the keywords of the OWL 2 Manchester syntax (W3C Note): ``and``,
``or``, ``not``, ``{a, b}``, ``some``, ``only``, ``value``, ``Self``, ``min``, ``max``,
``exactly``, ``inverse``, and datatype restrictions ``xsd:integer[>= 1]``. rdflib's
infixowl writes a different syntax (``THAT``) and does not read qualified cardinalities.

Terms are given as in SPARQL TSV: ``<iri>``, ``_:label``, or a literal; *outgoing* gives the
(predicate IRI, object term) pairs of each blank node of the expression.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence

__all__ = ["EXPRESSION_PREDICATES", "render"]

OWL = "http://www.w3.org/2002/07/owl#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XSD = "http://www.w3.org/2001/XMLSchema#"
_CARDINALITY = {
    "cardinality": "exactly",
    "minCardinality": "min",
    "maxCardinality": "max",
    "qualifiedCardinality": "exactly",
    "minQualifiedCardinality": "min",
    "maxQualifiedCardinality": "max",
}
_FACETS = {
    "minInclusive": ">=",
    "maxInclusive": "<=",
    "minExclusive": ">",
    "maxExclusive": "<",
    "length": "length",
    "minLength": "minLength",
    "maxLength": "maxLength",
    "pattern": "pattern",
    "langRange": "langRange",
}
# The predicates that build class expressions, data ranges and property expressions.
EXPRESSION_PREDICATES = frozenset(
    [RDF + "first", RDF + "rest", RDF + "type"]
    + [
        OWL + name
        for name in (
            "onProperty",
            "someValuesFrom",
            "allValuesFrom",
            "hasValue",
            "hasSelf",
            "onClass",
            "onDataRange",
            "intersectionOf",
            "unionOf",
            "complementOf",
            "oneOf",
            "inverseOf",
            "onDatatype",
            "withRestrictions",
            "datatypeComplementOf",
            *_CARDINALITY,
        )
    ]
    + [XSD + facet for facet in _FACETS]
)


class UnsupportedExpressionError(ValueError):
    """The blank node is not a class expression that OWL 2 maps to RDF."""


def _number(term: str) -> str:
    match = re.match(r'^"?(\d+)', term)
    if not match:
        raise UnsupportedExpressionError(f"not a cardinality: {term}")
    return match[1]


def render(
    term: str,
    outgoing: Mapping[str, Sequence[tuple[str, str]]],
    name: Callable[[str], str],
) -> str:
    """Write the class expression *term* in Manchester syntax; raise UnsupportedExpressionError.

    *name* writes an IRI (a CURIE, or ``<iri>`` for a canonical form).
    """

    def one(node: str, predicate: str) -> str | None:
        """Return the single *predicate* object of *node*; raise when there are several."""
        values = [o for p, o in outgoing.get(node, ()) if p == predicate]
        if len(values) > 1:
            raise UnsupportedExpressionError(f"{node} has several {predicate}")
        return values[0] if values else None

    def items(node: str | None, seen: frozenset[str] = frozenset()) -> list[str]:
        """Return the members of the RDF list headed by *node*."""
        found: list[str] = []
        while node and node != f"<{RDF}nil>":
            if node in seen or not node.startswith("_:"):
                raise UnsupportedExpressionError(f"malformed list at {node}")
            seen = seen | {node}
            first = one(node, RDF + "first")
            if first is None:
                raise UnsupportedExpressionError(f"list cell without rdf:first: {node}")
            found.append(first)
            node = one(node, RDF + "rest")
        return found

    def value(node: str) -> str:
        """Write an individual or literal *node*."""
        if node.startswith("<"):
            return name(node[1:-1])
        if node.startswith("_:"):
            raise UnsupportedExpressionError(f"blank node individual: {node}")
        typed = re.match(r'^(".*")\^\^<([^>]*)>$', node, re.DOTALL)
        if typed:
            return f"{typed[1]}^^{name(typed[2])}"
        return node if node.startswith('"') else f'"{node}"'

    def prop(node: str) -> str:
        """Write a property or inverse property expression *node*."""
        if node.startswith("<"):
            return name(node[1:-1])
        inverse = one(node, OWL + "inverseOf")
        if inverse and inverse.startswith("<"):
            return f"inverse {name(inverse[1:-1])}"
        raise UnsupportedExpressionError(f"property expression: {node}")

    def wrap(text: str) -> str:
        """Parenthesise a compound expression: one with a space outside brackets and quotes
        (double quotes of literals, single quotes of labels).
        """
        depth, quoted, previous = 0, "", ""
        for char in text:
            if char in "\"'" and previous != "\\" and quoted in ("", char):
                quoted = "" if quoted else char
            elif not quoted and char in "([{":
                depth += 1
            elif not quoted and char in ")]}":
                depth -= 1
            elif char == " " and depth == 0 and not quoted:
                return f"({text})"
            previous = char
        return text

    def expression(node: str, depth: int = 0) -> str:
        """Write the class expression *node*, at nesting *depth*."""
        if depth > 50:
            raise UnsupportedExpressionError("expression nested too deep")
        if node.startswith("<"):
            return name(node[1:-1])
        if not node.startswith("_:"):
            raise UnsupportedExpressionError(f"not a class expression: {node}")

        def get(predicate: str) -> str | None:
            """Return the single OWL *predicate* object of *node*, if any."""
            return one(node, OWL + predicate)

        for key, word in (("intersectionOf", " and "), ("unionOf", " or ")):
            members = get(key)
            if members is not None:
                return word.join(wrap(expression(m, depth + 1)) for m in items(members))
        if (target := get("complementOf")) is not None:
            return f"not {wrap(expression(target, depth + 1))}"
        if (target := get("datatypeComplementOf")) is not None:
            return f"not {wrap(expression(target, depth + 1))}"
        if (members := get("oneOf")) is not None:
            return "{" + ", ".join(value(m) for m in items(members)) + "}"
        if (datatype := get("onDatatype")) is not None:
            facets = []
            for facet_node in items(get("withRestrictions")):
                pairs = list(outgoing.get(facet_node, ()))
                if len(pairs) != 1 or not pairs[0][0].startswith(XSD):
                    raise UnsupportedExpressionError(f"facet: {facet_node}")
                facet = _FACETS.get(pairs[0][0][len(XSD) :])
                if facet is None:
                    raise UnsupportedExpressionError(f"facet: {pairs[0][0]}")
                facets.append(f"{facet} {value(pairs[0][1])}")
            return f"{expression(datatype, depth + 1)}[{', '.join(facets)}]"
        on = get("onProperty")
        if on is None:
            raise UnsupportedExpressionError(f"not a class expression: {node}")
        p = prop(on)
        if (target := get("someValuesFrom")) is not None:
            return f"{p} some {wrap(expression(target, depth + 1))}"
        if (target := get("allValuesFrom")) is not None:
            return f"{p} only {wrap(expression(target, depth + 1))}"
        if (target := get("hasValue")) is not None:
            return f"{p} value {value(target)}"
        if get("hasSelf") is not None:
            return f"{p} Self"
        for key, word in _CARDINALITY.items():
            if (n := get(key)) is not None:
                filler = get("onClass") or get("onDataRange")
                tail = f" {wrap(expression(filler, depth + 1))}" if filler else ""
                return f"{p} {word} {_number(n)}{tail}"
        raise UnsupportedExpressionError(f"restriction without a filler: {node}")

    return expression(term)
