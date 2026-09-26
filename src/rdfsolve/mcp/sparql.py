"""Complete, check and explain SPARQL text from a language model."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from pyparsing import ParseBaseException
from rdflib import RDF, URIRef, Variable
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import CompValue

from rdfsolve.client.query_fragments import PROTECTED, QuerySyntaxError, walk

if TYPE_CHECKING:
    from rdflib.plugins.sparql.sparql import Query

_CURIE = re.compile(r"(?<![\w.:?$-])([A-Za-z][\w.-]*)?:(?=[\w-])")
Triple = tuple[Any, Any, Any]


def _masked(text: str) -> str:
    """Replace strings, IRIs and comments with spaces of the same length."""
    return PROTECTED.sub(lambda match: " " * len(match.group()), text)


def add_prefixes(text: str, prefixes: dict[str, str]) -> tuple[str, list[str]]:
    """Declare the known prefixes that a query uses but does not declare."""
    masked = _masked(text)
    declared = set(re.findall(r"(?i)\bPREFIX\s+([A-Za-z][\w.-]*)?\s*:", masked))
    used = dict.fromkeys(match.group(1) or "" for match in _CURIE.finditer(masked))
    added = [prefix for prefix in used if prefix not in declared and prefix in prefixes]
    header = "".join(f"PREFIX {prefix}: <{prefixes[prefix]}>\n" for prefix in added)
    return header + text, added


def parse(text: str) -> Query:
    """Parse one SELECT query, or raise an error that tells how to correct it."""
    try:
        query = prepareQuery(text)
    except ParseBaseException as exc:
        raise QuerySyntaxError(text, exc, {}, phase="check") from exc
    except Exception as exc:
        # RDFLib raises a plain Exception for an undeclared prefix.
        if str(exc).startswith("Unknown namespace prefix"):
            raise ValueError(
                f"{exc}. Declare it with PREFIX, or write the full IRI in <>."
            ) from exc
        raise
    algebra = query.algebra
    if algebra.name != "SelectQuery":
        raise ValueError("Use a SELECT query. For a yes or no question, select a count.")
    if algebra.datasetClause:
        raise ValueError("Leave out FROM and FROM NAMED. The tools set the graphs of the source.")
    if any(isinstance(n, CompValue) and n.name == "ServiceGraphPattern" for n in walk(algebra)):
        raise ValueError("Leave out SERVICE. Only this source can be queried.")
    return query


def has_limit(query: Query) -> bool:
    """Tell whether the query has its own LIMIT."""
    node = query.algebra.p
    return node.name == "Slice" and node.length is not None


def terms(query: Query) -> tuple[set[str], set[str]]:
    """Return the classes (objects of rdf:type) and the properties that a query names."""
    classes: set[str] = set()
    properties: set[str] = set()
    for node in walk(query.algebra):
        # EXISTS keeps a TriplesBlock of the parse tree, with lists of three terms.
        if isinstance(node, CompValue) and node.name in {"BGP", "TriplesBlock"}:
            for triple in node.triples:
                _, predicate, obj = triple[:3]
                if predicate == RDF.type:
                    if isinstance(obj, URIRef):
                        classes.add(str(obj))
                else:
                    properties.update(str(n) for n in walk(predicate) if isinstance(n, URIRef))
    return classes, properties


def required_triples(node: Any) -> list[Triple]:
    """List the triple patterns that each result row must match."""
    if not isinstance(node, CompValue):
        return []
    if node.name == "BGP":
        return list(node.triples)
    if node.name in {"LeftJoin", "Minus"}:
        return required_triples(node.p1)
    if node.name == "Join":
        return required_triples(node.p1) + required_triples(node.p2)
    if node.name == "Union" or "p" not in node:
        return []
    return required_triples(node.p)


def _n3(term: Any) -> str:
    """Write one term of a triple pattern in SPARQL form."""
    return f"?{term}" if isinstance(term, Variable) else term.n3()


def diagnose(
    query: Query,
    select: Callable[[str], list[dict[str, Any]]],
    show: Callable[[str], str],
    limit: int = 12,
) -> list[str]:
    """Find the first triple patterns that match no data, alone or joined.

    select runs a query and gives its rows; show shortens IRIs for the notes.
    """
    triples = required_triples(query.algebra)[:limit]
    texts = [" ".join(_n3(term) for term in triple) + " ." for triple in triples]
    if not texts:
        return []
    alone = [t for t in texts if not select(f"SELECT * WHERE {{ {t} }} LIMIT 1")]
    if alone:
        return [f"No data matches {show(t)}" for t in alone[:4]]
    for k in range(2, len(texts) + 1):
        if not select(f"SELECT * WHERE {{ {' '.join(texts[:k])} }} LIMIT 1"):
            last = texts[k - 1]
            for other in texts[: k - 1]:
                if not select(f"SELECT * WHERE {{ {other} {last} }} LIMIT 1"):
                    return [
                        (
                            f"Each triple pattern matches data, but {show(other)} and "
                            f"{show(last)} match no data together. Check the direction of the "
                            "properties and the classes of the shared variables."
                        )
                    ]
            patterns = " ".join(show(t) for t in texts[:k])
            return [
                f"Each triple pattern matches data, but these match no data together: {patterns}"
            ]
    return [
        (
            "The triple patterns match data together. A FILTER, VALUES, MINUS or sub-query "
            "removes all rows."
        )
    ]


def with_graphs(text: str, graphs: Sequence[str]) -> str:
    """Add FROM clauses for the graphs of the source before the WHERE clause."""
    if not graphs:
        return text
    masked = _masked(text)
    start = masked.find("{")
    where = re.search(r"\bWHERE\s*$", masked[:start], re.IGNORECASE)
    position = where.start() if where else start
    return text[:position] + "".join(f"FROM <{g}> " for g in graphs) + text[position:]
