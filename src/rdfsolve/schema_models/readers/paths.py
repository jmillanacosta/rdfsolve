"""Read SHACL paths with cycle and RDF list checks."""

from __future__ import annotations

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
