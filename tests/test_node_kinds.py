"""Virtuoso evaluates isIRI() as true for its blank nodes (nodeID://...) outside a FILTER, in a BIND
or a projection, and as false in a FILTER; isBlank() is true in both (AOP-Wiki: the 50 subjects
of sh:prefix). A node kind is read with isBlank first, so that a recount that filters on the
kind finds the nodes that the discovery reported."""

import re

from rdfsolve.evidence.observed import build_node_kind_query
from rdfsolve.mining.structural_strategy import _discovery_query


def kind(query: str, variable: str) -> str:
    """Return the expression that a query binds to *variable*."""
    return re.search(rf"BIND\((.*?) AS \?{variable}\)", query)[1]


def test_node_kinds_are_read_with_isblank_first():
    discovery = _discovery_query(None, [], "")
    observed = build_node_kind_query(["urn:A"], None)
    for expression in (kind(discovery, "sk"), kind(discovery, "ok"), kind(observed, "kind")):
        assert expression.startswith("IF(isBlank("), expression
        assert "isIRI" not in expression, "Virtuoso: isIRI is true for blank nodes in a BIND"
