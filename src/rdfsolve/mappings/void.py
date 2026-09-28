"""Verified links as VoID linksets.

A void:Linkset states that triples of the subjects' dataset, with the link predicate, name
resources of the objects' dataset. That holds for a verified link only where the source writes
the target's own terms: a found pair whose source value equals the target term. A link whose
identifiers must be rewritten (the source writes identifiers.org IRIs, the target purl IRIs) is
not a linkset; it stays in the SSSOM mapping set with its target forms. When every value was
read, void:distinctObjects gives the number of target resources linked directly.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import XSD

from rdfsolve.config import mint
from rdfsolve.mappings.signatures import LinkEvidence

__all__ = ["links_to_void"]

VOID = Namespace("http://rdfs.org/ns/void#")


def links_to_void(links: Iterable[LinkEvidence], *, min_share: float = 0.5) -> Graph:
    """Return the linksets of the direct verified links with a share of at least *min_share*."""
    graph = Graph()
    graph.bind("void", VOID)
    for evidence in links:
        if evidence.share is None or evidence.share < min_share:
            continue
        direct = sum(1 for value, term in evidence.examples if value == term)
        if not direct:
            continue
        link = evidence.link
        key = "|".join(
            (
                link.kind,
                link.source_class,
                link.property,
                link.target_class or "",
                link.target_property or "",
            )
        )
        node = URIRef(
            mint("linkset", link.source, link.target, hashlib.sha256(key.encode()).hexdigest()[:16])
        )
        graph.add((node, RDF.type, VOID.Linkset))
        graph.add((node, VOID.subjectsTarget, URIRef(mint("dataset", link.source))))
        graph.add((node, VOID.objectsTarget, URIRef(mint("dataset", link.target))))
        graph.add((node, VOID.linkPredicate, URIRef(link.property)))
        if evidence.complete:
            graph.add((node, VOID.distinctObjects, Literal(direct, datatype=XSD.integer)))
    return graph
