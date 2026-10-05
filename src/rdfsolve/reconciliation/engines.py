"""Run a rule on several engines and accept its result only when they agree.

A rule is portable when every engine gives it the same result. An engine is any SPARQL helper:
Oxigraph or RDFLib on local data (``LocalGraphHelper``), or an endpoint such as a QLever index
(``SparqlHelper``). A CONSTRUCT result is compared as a set of triples and a SELECT result as a
multiset of rows; literals are compared in RDF 1.1 terms ("x" is "x"^^xsd:string), and blank
nodes by their position only.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from rdflib import XSD, BNode, Graph, Literal
from rdflib.term import Node

from rdfsolve.reconciliation.rules import Rule
from rdfsolve.sparql_helper import SparqlHelper

__all__ = ["RuleDisagreementError", "agreed", "engine"]

Engine = Callable[[str, str], Any]  # (query, form) -> Graph or SPARQL JSON rows


class RuleDisagreementError(ValueError):
    """Engines gave a rule different results; *only* holds the rows or triples that differ."""

    def __init__(self, rule: Rule, only: dict[str, list[Any]]) -> None:
        """Keep the differences of each engine."""
        super().__init__(f"{rule.iri} gives different results: {only}")
        self.only = only


def _term(node: Node) -> str:
    if isinstance(node, BNode):
        return "_:"
    if isinstance(node, Literal) and node.datatype is None and not node.language:
        node = Literal(str(node), datatype=XSD.string)
    return node.n3()


def _binding(value: dict[str, str]) -> str:
    if value["type"] == "uri":
        return f"<{value['value']}>"
    if value["type"] == "bnode":
        return "_:"
    return _term(
        Literal(value["value"], lang=value.get("xml:lang"), datatype=value.get("datatype"))
    )


def engine(helper: SparqlHelper) -> Engine:
    """Return an engine that answers rules with *helper*: a Graph, or the SPARQL JSON rows."""

    def answer(query: str, form: str) -> Any:
        """Run *query* and return its raw result."""
        if form == "construct":
            return helper.construct_graph(query)
        return helper.select(query, purpose="rule")["results"]["bindings"]

    return answer


def _counted(result: Any, form: str) -> Counter[Any]:
    """Return a result as comparable items: triples, or rows of variable and term."""
    if form == "construct":
        return Counter(tuple(_term(t) for t in triple) for triple in result)
    return Counter(tuple(sorted((k, _binding(v)) for k, v in row.items())) for row in result)


def _readable(items: Counter[Any], form: str) -> list[Any]:
    """Return items as triples written in N-Triples terms, or rows as dictionaries."""
    if form == "construct":
        return sorted(" ".join(triple) for triple in items.elements())
    return [dict(row) for row in sorted(items.elements())]


def agreed(rule: Rule, records: Sequence[str], engines: Mapping[str, Engine]) -> Any:
    """Return the result of *rule* on *records* when every engine gives it; else raise.

    The result is the first engine's Graph for a conversion, and its rows (variable to term)
    for a check.
    """
    query, form = rule.bound(records), rule.form
    raw = {name: run(query, form) for name, run in engines.items()}
    answers = {name: _counted(result, form) for name, result in raw.items()}
    first, reference = next(iter(answers.items()))
    only: dict[str, list[Any]] = {}
    for name, found in answers.items():
        if found != reference:
            only[name] = _readable(found - reference, form)
            only.setdefault(first, []).extend(_readable(reference - found, form))
    if only:
        raise RuleDisagreementError(rule, only)
    return raw[first] if form == "construct" else _readable(reference, form)
