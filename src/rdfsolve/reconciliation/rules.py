"""Rules: conversions and checks written in plain SPARQL 1.1.

A rule is a CONSTRUCT (a conversion) or a SELECT (a check, one row for each finding) with its
prefixes and explicit graphs, so that any engine runs it. The records it applies to are a
declared parameter, a variable of the query; the rule never binds it. Applying the rule binds
the parameter (``bound``), and the executed query is kept with the application, not in the rule.

A rule is kept as a SHACL SPARQL executable (``sh:SPARQLConstructExecutable`` or
``sh:SPARQLSelectExecutable``) with its parameter (``sh:parameter``), as in SIB's collection of
queries, and is saved as a nanopublication.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rdflib import RDF, RDFS, Graph, Literal, Namespace, URIRef
from rdflib.plugins.sparql import prepareQuery

if TYPE_CHECKING:
    from nanopub.nanopub import Nanopub

__all__ = ["Rule"]

SH = Namespace("http://www.w3.org/ns/shacl#")
_FORMS = {"ConstructQuery": "construct", "SelectQuery": "select"}
_WHERE = re.compile(r"\bWHERE\s*\{", re.IGNORECASE)


@dataclass(frozen=True)
class Rule:
    """A conversion or check over the records bound to *parameter*."""

    iri: str
    query: str
    parameter: str = "record"
    comment: str = ""

    def __post_init__(self) -> None:
        """Refuse a query that is not portable SPARQL over its parameter."""
        try:
            form = prepareQuery(self.query).algebra.name
        except Exception as error:  # the parser raises its own error types
            raise ValueError(f"{self.iri} is not SPARQL 1.1: {error}") from error
        if form not in _FORMS:
            raise ValueError(f"{self.iri}: a rule is a CONSTRUCT or SELECT query")
        variable = rf"[?$]{re.escape(self.parameter)}\b"
        if not re.search(variable, self.query):
            raise ValueError(f"{self.iri} does not use its parameter ?{self.parameter}")
        if re.search(rf"VALUES\s*\(?[^{{]*{variable}", self.query, re.IGNORECASE):
            raise ValueError(
                f"{self.iri} binds its parameter with VALUES; it is bound when applied"
            )

    @property
    def form(self) -> str:
        """Return ``construct`` for a conversion and ``select`` for a check."""
        return _FORMS[prepareQuery(self.query).algebra.name]

    def bound(self, records: Sequence[str]) -> str:
        """Return the query applied to *records* (IRIs): the parameter is bound first in WHERE."""
        values = f" VALUES ?{self.parameter} {{ {' '.join(f'<{r}>' for r in records)} }}"
        where = _WHERE.search(self.query)
        at = where.end() if where else self.query.index("{") + 1
        return self.query[:at] + values + self.query[at:]

    def to_graph(self) -> Graph:
        """Return the rule as a SHACL SPARQL executable with its parameter."""
        graph = Graph()
        rule, parameter = URIRef(self.iri), URIRef(f"{self.iri}#{self.parameter}")
        kind = "SPARQLConstructExecutable" if self.form == "construct" else "SPARQLSelectExecutable"
        graph.add((rule, RDF.type, SH[kind]))
        graph.add((rule, SH[self.form], Literal(self.query)))
        graph.add((rule, SH.parameter, parameter))
        graph.add((parameter, SH.path, parameter))
        graph.add((parameter, SH.name, Literal(self.parameter)))
        if self.comment:
            graph.add((rule, RDFS.comment, Literal(self.comment)))
        return graph

    @classmethod
    def from_graph(cls, graph: Graph, iri: str) -> Rule:
        """Read the rule *iri* from *graph*."""
        rule = URIRef(iri)
        query = graph.value(rule, SH.construct) or graph.value(rule, SH.select)
        if query is None:
            raise ValueError(f"No SPARQL executable {iri} in the graph")
        parameter = graph.value(graph.value(rule, SH.parameter), SH.name)
        comment = graph.value(rule, RDFS.comment)
        return cls(iri, str(query), str(parameter), str(comment or ""))

    def nanopublication(self, *, attributed_to: str, created: str) -> Nanopub:
        """Return the rule as an unsigned nanopublication."""
        from rdfsolve.reconciliation.nanopubs import nanopublication

        kind = "SPARQLConstructExecutable" if self.form == "construct" else "SPARQLSelectExecutable"
        return nanopublication(
            self.to_graph(), kinds=[SH[kind]], attributed_to=attributed_to, created=created
        )
