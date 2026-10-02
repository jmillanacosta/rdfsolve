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


def test_discovery_of_one_property_groups_only_its_own_nodes():
    """The property sets are grouped for the subjects and objects of the property only, with
    the same answer as grouping every node (UberGraph: 47 properties refused at 10 min each)."""
    import json

    from rdflib import Graph

    data = Graph().parse(
        format="turtle",
        data="""<urn:a> <urn:p> <urn:b> ; <urn:q> "x" . <urn:b> <urn:r> <urn:c> .
        <urn:c> <urn:q> "y" . _:n <urn:p> "z" ; <urn:r> <urn:a> .""",
    )
    residual = "VALUES ?p { <urn:p> }"
    whole = _discovery_query(None, [], residual)
    own = _discovery_query(None, [], residual, predicate="urn:p")
    assert "?s <urn:p> ?_po" in own and "?_ps <urn:p> ?o" in own

    def rows(query):
        """Return the answer rows, with the property sets in order (GROUP_CONCAT has none)."""
        found = json.loads(data.query(query).serialize(format="json"))["results"]["bindings"]
        for row in found:
            for key in ("ss", "os"):
                if key in row:
                    row[key]["value"] = " ".join(sorted(row[key]["value"].split()))
        return sorted(json.dumps(r, sort_keys=True) for r in found)

    assert rows(own) == rows(whole) and len(rows(own)) == 2
