"""Policies: which source decides each namespace, under which assumptions.

A policy lists one authority for each namespace (a Bioregistry prefix) and states, for it:

- the match levels accepted for mappings to the namespace (``skos:exactMatch`` by default);
- whether the authority's cross-references (``oboInOwl:hasDbXref``) are taken as exact matches,
  an assumption of the policy, never a fact;
- whether the authority's links to the namespace are unique (one for each record), which a
  negative statement requires;
- the entries the authority prefers (for example its reviewed entries), as a shape they meet.

It is written as SHACL with published terms only, one ``sh:NodeShape`` for each namespace:
``dcterms:subject`` is the namespace (its Bioregistry IRI) and ``dcterms:source`` the authority.
The match levels constrain the SSSOM mappings to the namespace in their OWL form (a SPARQL
target, SHACL-AF), the uniqueness is a qualified count of the link, and the preferred entries
are the shape the entry conforms to (``dcterms:conformsTo``). The shapes check mapping claims
and records with any SHACL engine; the decision of mapping claims reads the same policy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rdflib import DCTERMS, OWL, RDF, SKOS, BNode, Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.term import Node

from rdfsolve.config import mint

if TYPE_CHECKING:
    from nanopub.nanopub import Nanopub

__all__ = ["Authority", "Policy"]

SH = Namespace("http://www.w3.org/ns/shacl#")
XREF = "http://www.geneontology.org/formats/oboInOwl#hasDbXref"
REGISTRY = "https://bioregistry.io/registry/"


@dataclass(frozen=True)
class Authority:
    """The source that decides *namespace*, and the assumptions taken for it."""

    namespace: str
    source: str
    exact: bool = False
    unique: bool = False
    link: str = XREF
    preferred: tuple[tuple[str, Node], ...] = ()


def _forms(namespace: str) -> list[str]:
    """Return the starts of the namespace's identifiers: its IRI forms and its CURIE prefix."""
    import bioregistry

    resource = bioregistry.get_resource(namespace)
    if resource is None:
        raise ValueError(f"{namespace} is not a Bioregistry prefix")
    iris = {f[: f.index("$1")] for f in resource.get_uri_formats() if f.endswith("$1")}
    return sorted(iris | {f"{resource.get_preferred_prefix() or namespace}:"})


@dataclass(frozen=True)
class Policy:
    """The authorities of a reconciliation and the match levels it accepts."""

    iri: str
    authorities: tuple[Authority, ...]
    match_levels: tuple[str, ...] = (str(SKOS.exactMatch),)

    @property
    def order(self) -> list[str]:
        """Return the deciding sources, in the order of the policy."""
        return list(dict.fromkeys(a.source for a in self.authorities))

    @property
    def exact(self) -> list[str]:
        """Return the namespaces whose cross-references are taken as exact matches."""
        return [a.namespace for a in self.authorities if a.exact]

    @property
    def unique(self) -> list[str]:
        """Return the namespaces whose links are declared unique."""
        return [a.namespace for a in self.authorities if a.unique]

    def to_graph(self) -> Graph:
        """Return the policy as SHACL shapes, one for each namespace."""
        graph = Graph()
        policy = URIRef(self.iri)
        for rank, authority in enumerate(self.authorities):
            shape = URIRef(f"{self.iri}#{authority.namespace}")
            forms = _forms(authority.namespace)
            graph.add((policy, DCTERMS.hasPart, shape))
            graph.add((shape, RDF.type, SH.NodeShape))
            graph.add((shape, SH.order, Literal(rank)))
            graph.add((shape, DCTERMS.subject, URIRef(REGISTRY + authority.namespace)))
            graph.add((shape, DCTERMS.source, URIRef(mint("dataset", authority.source))))
            starts = " || ".join(f'STRSTARTS(STR(?t), "{f}")' for f in forms)
            target = BNode()
            graph.add((shape, SH.target, target))
            graph.add((target, RDF.type, SH.SPARQLTarget))
            graph.add(
                (
                    target,
                    SH.select,
                    Literal(
                        f"SELECT ?this WHERE {{ ?this <{OWL.annotatedTarget}> ?t FILTER({starts}) }}"
                    ),
                )
            )
            levels = [*self.match_levels, *([XREF] if authority.exact else [])]
            match = BNode()
            graph.add((shape, SH.property, match))
            graph.add((match, SH.path, OWL.annotatedProperty))
            graph.add(
                (match, SH["in"], Collection(graph, BNode(), [URIRef(m) for m in levels]).uri)
            )
            if authority.unique:
                pattern = "^(" + "|".join(re.escape(f) for f in forms) + ")"
                links, values = BNode(), BNode()
                graph.add((shape, SH.targetSubjectsOf, URIRef(authority.link)))
                graph.add((shape, SH.property, links))
                graph.add((links, SH.path, URIRef(authority.link)))
                graph.add((links, SH.qualifiedValueShape, values))
                graph.add((values, SH.pattern, Literal(pattern)))
                graph.add((links, SH.qualifiedMaxCount, Literal(1)))
            if authority.preferred:
                entry = BNode()
                graph.add((shape, DCTERMS.conformsTo, entry))
                graph.add((entry, RDF.type, SH.NodeShape))
                for path, value in authority.preferred:
                    test = BNode()
                    graph.add((entry, SH.property, test))
                    graph.add((test, SH.path, URIRef(path)))
                    graph.add((test, SH.hasValue, value))
        return graph

    @classmethod
    def from_graph(cls, graph: Graph, iri: str) -> Policy:
        """Read the policy *iri* from *graph*."""

        def value(subject: Node, predicate: URIRef) -> Node:
            """Return the one value of *predicate*, which the policy writes."""
            found = graph.value(subject, predicate)
            if found is None:
                raise ValueError(f"{iri}: {subject} has no {predicate}")
            return found

        authorities, levels = [], set()
        shapes = graph.objects(URIRef(iri), DCTERMS.hasPart)
        for shape in sorted(shapes, key=lambda s: int(str(value(s, SH.order)))):
            accepted = [
                str(m)
                for match in graph.objects(shape, SH.property)
                if graph.value(match, SH.path) == OWL.annotatedProperty
                for m in Collection(graph, value(match, SH["in"]))
            ]
            levels |= set(accepted) - {XREF}
            link = graph.value(shape, SH.targetSubjectsOf)
            entry = graph.value(shape, DCTERMS.conformsTo)
            tests = graph.objects(entry, SH.property) if entry else ()
            authorities.append(
                Authority(
                    str(value(shape, DCTERMS.subject)).removeprefix(REGISTRY),
                    str(value(shape, DCTERMS.source)).rsplit("/", 1)[1],
                    exact=XREF in accepted,
                    unique=link is not None,
                    link=str(link or XREF),
                    preferred=tuple(
                        sorted((str(value(t, SH.path)), value(t, SH.hasValue)) for t in tests)
                    ),
                )
            )
        return cls(iri, tuple(authorities), tuple(sorted(levels)) or (str(SKOS.exactMatch),))

    def nanopublication(self, *, attributed_to: str, created: str) -> Nanopub:
        """Return the policy as an unsigned nanopublication."""
        from rdfsolve.reconciliation.nanopubs import nanopublication

        return nanopublication(
            self.to_graph(), kinds=[SH.NodeShape], attributed_to=attributed_to, created=created
        )
