"""Write terms that are not RDF IRIs into SPARQL queries.

Lenient parsers and engines keep a term such as ``<http://example.org/a b>`` from data whose IRIs
contain spaces or other characters that RDF IRIs exclude, but no SPARQL parser reads it back
between angle brackets. ``IRI("...")`` builds the same term from a string, so such a term in a
query is rewritten:

- in a group, it is replaced by a variable bound at the start of the group with
  ``BIND(IRI("...") AS ?var)``;
- in a ``VALUES`` block of one variable (``VALUES ?v`` or ``VALUES (?v)``), it is moved to a
  ``UNION`` branch that binds it;
- in the projection, it is replaced by ``IRI("...")``.

A query without such a term is returned unchanged. A term in a dataset clause or in a
``VALUES`` block of several variables is left as it is; the engine then refuses the query.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from rdfsolve.schema_models.paths import _INVALID_IRI_CHAR

__all__ = ["writable_query"]

_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*:")
_VALUES = re.compile(r"VALUES\s*(\?\w+|\(\s*\?\w+\s*\))\s*$", re.IGNORECASE)
_ANY_VALUES = re.compile(r"VALUES\s*(\?\w+|\([^)]*\))\s*$", re.IGNORECASE)
_EMPTY_ROW = re.compile(r"\(\s*\)")
_SELECT = re.compile(r"\s*SELECT\b", re.IGNORECASE)


@dataclass(frozen=True)
class _Term:
    start: int
    end: int
    iri: str
    brace: int | None  # the open brace of the innermost group, or None at the top level
    in_parentheses: bool


def _scan(query: str) -> tuple[list[_Term], dict[int, int]]:
    """Return the terms that are not RDF IRIs and the closing brace of each open brace."""
    terms: list[_Term] = []
    closes: dict[int, int] = {}
    braces: list[int] = []
    parentheses: list[int] = [0]
    i, n = 0, len(query)
    while i < n:
        c = query[i]
        if c in "\"'":
            quote = query[i : i + 3] if query[i : i + 3] in ('"""', "'''") else c
            i += len(quote)
            while i < n and not query.startswith(quote, i):
                i += 2 if query[i] == "\\" else 1
            i += len(quote)
            continue
        if c == "#":
            end = query.find("\n", i)
            i = n if end < 0 else end
            continue
        if c == "<" and _SCHEME.match(query, i + 1):
            end = query.find(">", i + 1)
            body = query[i + 1 : end] if end > 0 else ""
            if body and not set(body) & set('<"{}\n\r'):
                if _INVALID_IRI_CHAR.search(body):
                    terms.append(
                        _Term(i, end + 1, body, braces[-1] if braces else None, parentheses[-1] > 0)
                    )
                i = end + 1
                continue
        if c == "{":
            braces.append(i)
            parentheses.append(0)
        elif c == "}" and braces:
            closes[braces.pop()] = i
            parentheses.pop()
        elif c == "(":
            parentheses[-1] += 1
        elif c == ")":
            parentheses[-1] -= 1
        i += 1
    return terms, closes


def _string(iri: str) -> str:
    return '"' + iri.replace("\\", "\\\\") + '"'


def writable_query(query: str) -> str:
    """Return *query* with every term that is not an RDF IRI written with ``IRI("...")``."""
    terms, closes = _scan(query)
    if not terms:
        return query
    names: dict[str, str] = {}
    edits: list[tuple[int, int, str]] = []
    bound: dict[int, list[str]] = {}
    blocks: dict[int, tuple[int, str, list[_Term]]] = {}
    for term in terms:
        brace = term.brace
        if brace is None:
            if term.in_parentheses:
                edits.append((term.start, term.end, f"IRI({_string(term.iri)})"))
            continue
        values = _VALUES.search(query, 0, brace)
        if values:
            blocks.setdefault(brace, (values.start(), values.group(1), []))[2].append(term)
            continue
        if _ANY_VALUES.search(query, 0, brace):
            continue  # a VALUES block of several variables
        if term.in_parentheses and _SELECT.match(query, brace + 1):
            edits.append((term.start, term.end, f"IRI({_string(term.iri)})"))
            continue
        name = names.setdefault(term.iri, f"?_iri{len(names)}")
        edits.append((term.start, term.end, name))
        if name not in bound.setdefault(brace, []):
            bound[brace].append(name)
            edits.append((brace + 1, brace + 1, f" BIND(IRI({_string(term.iri)}) AS {name})"))
    for brace, (start, variable, inside) in blocks.items():
        close = closes[brace]
        kept = query[brace + 1 : close]
        for term in sorted(inside, key=lambda t: -t.start):
            kept = kept[: term.start - brace - 1] + kept[term.end - brace - 1 :]
        kept = _EMPTY_ROW.sub("", kept)  # the rows of the terms in VALUES (?var) { (...) }
        name = variable.strip("() ")
        branches = [f"{{ VALUES {variable} {{{kept}}} }}"] + [
            f"{{ BIND(IRI({_string(term.iri)}) AS {name}) }}"
            for term in {t.iri: t for t in inside}.values()
        ]
        edits.append((start, close + 1, "{ " + " UNION ".join(branches) + " }"))
    for start, end, text in sorted(edits, key=lambda e: (e[0], e[1]), reverse=True):
        query = query[:start] + text + query[end:]
    return query
