"""The hierarchy of ontology terms: named parents and ancestors, from where they are written.

An ontology loaded as an RDF graph (NamedOntologyIndex), a SPARQL endpoint that holds the
ontology (fetch_superclasses), or tab-separated files of (child, parent) pairs for terms whose
ontology the data do not hold (read_hierarchy). Only named OWL/RDFS relations are read here;
this is not OWL entailment.
"""

from __future__ import annotations

import gzip
from collections import defaultdict, deque
from collections.abc import Iterable
from pathlib import Path
from typing import Literal

from rdflib import OWL, RDF, RDFS, Graph, URIRef

from rdfsolve.mining.query_builders import _graph_scope
from rdfsolve.ontology.vocabulary import NOT_DATA_TYPES, SUBCLASS_OF
from rdfsolve.sparql_helper import SparqlHelper

RDFS_SUBCLASS_OF = SUBCLASS_OF


def _named_edges(graph: Graph, predicate: URIRef) -> dict[str, set[str]]:
    edges: dict[str, set[str]] = defaultdict(set)
    for child, parent in graph.subject_objects(predicate):
        if isinstance(child, URIRef) and isinstance(parent, URIRef):
            edges[str(child)].add(str(parent))
    return edges


def _symmetric_edges(graph: Graph, predicate: URIRef) -> dict[str, set[str]]:
    edges: dict[str, set[str]] = defaultdict(set)
    for left, right in graph.subject_objects(predicate):
        if isinstance(left, URIRef) and isinstance(right, URIRef):
            edges[str(left)].add(str(right))
            edges[str(right)].add(str(left))
    return edges


def _closure(start: str, edges: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    queue = deque([start])
    while queue:
        current = queue.popleft()
        for nxt in edges.get(current, ()):
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
    return seen


class NamedOntologyIndex:
    """Named-term semantic index for cheap, deterministic release analyses."""

    def __init__(self, graph: Graph):
        """Index the named class and property hierarchies of an ontology graph."""
        self.graph = graph
        self.class_parents = _named_edges(graph, RDFS.subClassOf)
        self.property_parents = _named_edges(graph, RDFS.subPropertyOf)
        self.class_equivalents = _symmetric_edges(graph, OWL.equivalentClass)
        self.property_equivalents = _symmetric_edges(graph, OWL.equivalentProperty)
        self.classes = (
            self._signature((OWL.Class, RDFS.Class))
            | set(self.class_parents)
            | {term for values in self.class_parents.values() for term in values}
        )
        self.properties = (
            self._signature(
                (RDF.Property, OWL.ObjectProperty, OWL.DatatypeProperty, OWL.AnnotationProperty)
            )
            | set(self.property_parents)
            | {term for values in self.property_parents.values() for term in values}
        )

    def _signature(self, kinds: tuple[URIRef, ...]) -> set[str]:
        return {
            str(subject)
            for kind in kinds
            for subject in self.graph.subjects(RDF.type, kind)
            if isinstance(subject, URIRef)
        }

    def equivalents(self, term: str, *, kind: Literal["class", "property"]) -> set[str]:
        """Return the terms equivalent to a term, transitively."""
        return _closure(
            term, self.class_equivalents if kind == "class" else self.property_equivalents
        )

    def ancestors(self, term: str, *, kind: Literal["class", "property"]) -> set[str]:
        """Return the named ancestors of a term, transitively."""
        return _closure(term, self.class_parents if kind == "class" else self.property_parents)


def fetch_superclasses(
    helper: SparqlHelper,
    terms: Iterable[str],
    *,
    batch_size: int = 500,
    purpose: str = "ontology-terms/superclasses",
    graph_uris: list[str] | None = None,
) -> dict[str, set[str]]:
    """Return named ``rdfs:subClassOf`` parents for *terms* and all their ancestors.

    Read the RDF merge of the selected graphs, or the endpoint default dataset.
    """
    dataset, _, _ = _graph_scope(graph_uris)
    parents: dict[str, set[str]] = {}
    frontier = sorted(set(terms))
    while frontier:
        for start in range(0, len(frontier), batch_size):
            batch = frontier[start : start + batch_size]
            values = " ".join(f"<{iri}>" for iri in batch)
            query = f"""\
SELECT ?c ?parent
{dataset}
WHERE {{
  VALUES ?c {{ {values} }}
  ?c <{RDFS_SUBCLASS_OF}> ?parent .
  FILTER(isIRI(?parent) && ?parent != ?c)
}}"""
            result = helper.select(query, purpose=purpose)
            for iri in batch:
                parents.setdefault(iri, set())
            for row in result.get("results", {}).get("bindings", []):
                child = row.get("c", {}).get("value")
                parent = row.get("parent", {}).get("value")
                if child and parent and parent not in NOT_DATA_TYPES:
                    parents.setdefault(child, set()).add(parent)
        frontier = sorted({p for ps in parents.values() for p in ps} - parents.keys())
    return parents


def read_hierarchy(paths: Iterable[str | Path]) -> dict[str, set[str]]:
    """Read (child, parent) IRI pairs from tab-separated files, plain or gzip-compressed.

    Lines that start with # are comments. The files give parents to terms whose ontology is not
    in the data: PubChem types records with NCIt and PR terms, but the index holds no hierarchy
    for them. A term is not its own parent.
    """
    parents: dict[str, set[str]] = defaultdict(set)
    for path in paths:
        opener = gzip.open if str(path).endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as lines:
            for line in lines:
                if not line.strip() or line.startswith("#"):
                    continue
                child, parent = line.rstrip("\n").split("\t")[:2]
                if child != parent:
                    parents[child].add(parent)
    return dict(parents)


__all__ = ["NamedOntologyIndex", "fetch_superclasses", "read_hierarchy"]
