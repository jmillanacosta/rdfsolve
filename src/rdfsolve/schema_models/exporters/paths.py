"""Render structured paths as SHACL RDF or SPARQL path syntax."""

from __future__ import annotations

from rdflib import SH, BNode, Graph, URIRef
from rdflib.collection import Collection
from rdflib.term import Node

from rdfsolve.schema_models.paths import PropertyPath


def path_to_rdf(path: PropertyPath, graph: Graph) -> Node:
    """Write a valid SHACL path; do not create an IRI for a compound path."""
    if path.operator == "predicate":
        return URIRef(path.iri or "")
    node = BNode()
    items = [path_to_rdf(item, graph) for item in path.items]
    if path.operator in ("sequence", "alternative"):
        head = node if path.operator == "sequence" else BNode()
        Collection(graph, head, items)
        if path.operator == "alternative":
            graph.add((node, SH.alternativePath, head))
    else:
        predicate = {
            "inverse": SH.inversePath,
            "zero_or_more": SH.zeroOrMorePath,
            "one_or_more": SH.oneOrMorePath,
            "zero_or_one": SH.zeroOrOnePath,
        }[path.operator]
        graph.add((node, predicate, items[0]))
    return node


def path_to_sparql(path: PropertyPath) -> str:
    """Render syntax only. The caller must bound subjects, results, and query time."""
    if path.operator == "predicate":
        return f"<{path.iri}>"
    parts = [path_to_sparql(item) for item in path.items]
    if path.operator in ("sequence", "alternative"):
        return "(" + ("/" if path.operator == "sequence" else "|").join(parts) + ")"
    if path.operator == "inverse":
        return f"^({parts[0]})"
    suffix = {"zero_or_more": "*", "one_or_more": "+", "zero_or_one": "?"}[path.operator]
    return f"({parts[0]}){suffix}"
