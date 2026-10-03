"""Match observed edges against typed schema profiles."""

from __future__ import annotations

from collections import defaultdict

from rdflib import URIRef

from rdfsolve.mining.query_builders import _context_pattern
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS


def _is(variable: str, iris: set[str]) -> str:
    """Compare a variable with one IRI or a list of IRIs."""
    terms = sorted(URIRef(iri).n3() for iri in iris)
    return f"{variable} = {terms[0]}" if len(terms) == 1 else f"{variable} IN ({', '.join(terms)})"


def typed_match(
    keys: list[tuple[str, str, str, str | None]],
    type_graphs: list[str] | None,
    context_graphs: list[str] | None,
    predicate: str | None = None,
    restriction: str = "",
) -> str:
    """Return an existence test for the union of observed typed profiles.

    With *predicate*, the test is for edges of that one property: its group reads only that
    property. QLever evaluates the group of EXISTS on its own before the join, so a group that
    reads ?s ?p ?o reads the whole graph for each property (Bgee RO_0002162: 217 s, and 20 s
    with the constant property; the same counts). *restriction* (for example FILTER(?o IN
    ...)) follows the edge, so that the group reads only the edges of one census batch.
    """
    keys = [key for key in keys if predicate is None or key[1] == predicate]
    if not keys:
        return "false"
    typed: dict[tuple[str, str], set[str]] = defaultdict(set)
    literal: dict[tuple[str, str], set[str | None]] = defaultdict(set)
    untyped: dict[str, dict[str, set[str]]] = {
        "Resource": defaultdict(set),
        "BlankNode": defaultdict(set),
    }
    for subject, prop, obj, datatype in keys:
        if obj == "Literal":
            literal[prop, subject].add(datatype)
        elif obj in _SENTINEL_OBJECTS:
            untyped[obj][prop].add(subject)
        else:
            typed[prop, subject].add(obj)
    objects = list(dict.fromkeys((type_graphs or []) + (context_graphs or [])))
    subject_type = _context_pattern("?s a ?_subjectType .", objects).replace(
        "?_contextGraph", "?_subjectTypeGraph"
    )
    object_type = _context_pattern("?o a ?_objectType .", objects).replace(
        "?_contextGraph", "?_objectTypeGraph"
    )
    any_type = _context_pattern("?o a ?_anyObjectType .", objects).replace(
        "?_contextGraph", "?_objectAnyGraph"
    )
    if predicate:
        edge = f"?s {URIRef(predicate).n3()} ?o . {restriction}"
    else:
        edge = f"?s ?p ?o . {restriction}"

    def on(prop: str, test: str) -> str:
        """Add the comparison of the property when the test is for every property."""
        return test if predicate else f"{_is('?p', {prop})} && {test}"

    groups = []
    if typed:
        pairs = " ||\n  ".join(
            f"({on(prop, _is('?_subjectType', {subject}))} && {_is('?_objectType', targets)})"
            for (prop, subject), targets in sorted(typed.items())
        )
        groups.append(f"EXISTS {{ {edge} {subject_type} {object_type}\nFILTER(\n  {pairs}\n) }}")
    tests: list[str] = []
    for (prop, subject), datatypes in sorted(literal.items(), key=str):
        kind = "isLiteral(?o)"
        if None not in datatypes:
            kind += f" && {_is('DATATYPE(?o)', {dt for dt in datatypes if dt})}"
        tests.append(f"({on(prop, _is('?_subjectType', {subject}))} && {kind})")
    for prop, subjects in sorted(untyped["Resource"].items()):
        tests.append(
            f"({on(prop, _is('?_subjectType', subjects))} && isIRI(?o) && "
            f"!EXISTS {{ {edge} {any_type} }})"
        )
    for prop, subjects in sorted(untyped["BlankNode"].items()):
        tests.append(f"({on(prop, _is('?_subjectType', subjects))} && isBlank(?o))")
    if tests:
        joined = " ||\n  ".join(tests)
        groups.append(f"EXISTS {{ {edge} {subject_type}\nFILTER(\n  {joined}\n) }}")
    # The types are compared with IRIs, grouped by property and subject class; no VALUES list
    # of profiles is joined with them. QLever joins such a list with every type triple of the
    # graph before the edge (Bgee RO_0002206: 455.7 GB for every batch; HGNC
    # has-approved-symbol: over 6.5 GB) and evaluates an EXISTS group with a large VALUES
    # wrongly (768 objects: every edge untyped). The tests already require a subject type, so
    # no IF(EXISTS ...) wraps them (Virtuoso rejects that form, error SQ156). A typed object is
    # tested in one EXISTS; literals, untyped IRIs and blank nodes in the other. The inner
    # EXISTS of the untyped case repeats the edge and its restriction, because QLever evaluates
    # the group on its own and ?o a ?_anyObjectType alone reads every type triple. Virtuoso
    # gives wrong counts for an OPTIONAL inside the EXISTS (1 of 2 prov:used edges), and RDFLib
    # evaluates a UNION inside an EXISTS as false.
    return f"({' || '.join(groups)})"


def uncovered_filter(
    keys: list[tuple[str, str, str, str | None]],
    type_graphs: list[str] | None,
    context_graphs: list[str] | None,
) -> str:
    """Select edges absent from the observed typed profiles."""
    return f"FILTER(!{typed_match(keys, type_graphs, context_graphs)})"
