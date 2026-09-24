"""Read RDF lists within each selected graph of a local snapshot."""

from __future__ import annotations

from typing import Literal as Kind

from rdflib import RDF, XSD, BNode, Dataset, Graph, Literal, URIRef
from rdflib.term import Node

from rdfsolve.schema_models.collections import CollectionProfile


def _members(graph: Graph, head: Node) -> list[Node]:
    members: list[Node] = []
    seen: set[Node] = set()
    cell = head
    while cell != RDF.nil:
        if cell in seen or not isinstance(cell, (URIRef, BNode)):
            raise ValueError("Cyclic or non-resource list cell")
        seen.add(cell)
        first, rest = list(graph.objects(cell, RDF.first)), list(graph.objects(cell, RDF.rest))
        if len(first) != 1 or len(rest) != 1:
            raise ValueError("Each list cell needs one rdf:first and one rdf:rest")
        members.append(first[0])
        cell = rest[0]
    return members


def discover_collections(
    source: Graph,
    *,
    graph_uris: list[str] | None = None,
) -> list[CollectionProfile]:
    """Describe typed owners' RDF lists without combining named graphs.

    None or an empty scope selects the default graph. Malformed lists are
    counted separately; observed lengths do not impose cardinality constraints.
    """
    if (
        graph_uris
        and not isinstance(source, Dataset)
        and (len(graph_uris) != 1 or str(source.identifier) != graph_uris[0])
    ):
        raise ValueError("Supply a Dataset containing the selected named graphs")
    graphs = (
        [(iri, source.graph(URIRef(iri))) for iri in graph_uris]
        if isinstance(source, Dataset) and graph_uris
        else [
            (
                graph_uris[0] if graph_uris else None,
                source.default_graph if isinstance(source, Dataset) else source,
            )
        ]
    )
    result: list[CollectionProfile] = []
    for graph_uri, graph in graphs:
        profiles: dict[tuple[str, str], CollectionProfile] = {}
        heads = set(graph.subjects(RDF.first)) | set(graph.subjects(RDF.rest)) | {RDF.nil}
        for head in sorted(heads, key=str):
            owners = [
                (s, p)
                for s, p in graph.subject_predicates(head)
                if p not in {RDF.first, RDF.rest, RDF.type}
            ]
            if not owners:
                continue
            try:
                members = _members(graph, head)
            except ValueError:
                members = None
            for owner, predicate in owners:
                for cls in graph.objects(owner, RDF.type):
                    if not isinstance(cls, URIRef):
                        continue
                    key = str(cls), str(predicate)
                    profile = profiles.setdefault(
                        key,
                        CollectionProfile(
                            subject_class=key[0], property_uri=key[1], graph_uri=graph_uri
                        ),
                    )
                    if members is None:
                        profile.invalid_count += 1
                        continue
                    profile.list_count += 1
                    length = len(members)
                    profile.min_length = (
                        min(profile.min_length, length)
                        if profile.min_length is not None
                        else length
                    )
                    profile.max_length = (
                        max(profile.max_length, length)
                        if profile.max_length is not None
                        else length
                    )
                    for member in members:
                        kind: Kind["IRI", "BlankNode", "Literal"] = (
                            "Literal"
                            if isinstance(member, Literal)
                            else "BlankNode"
                            if isinstance(member, BNode)
                            else "IRI"
                        )
                        profile.member_kinds = sorted(set(profile.member_kinds) | {kind})
                        if isinstance(member, Literal):
                            datatype = (
                                str(RDF.langString)
                                if member.language
                                else str(member.datatype or XSD.string)
                            )
                            profile.member_datatypes = sorted(
                                set(profile.member_datatypes) | {datatype}
                            )
                            if member.language:
                                profile.member_languages = sorted(
                                    set(profile.member_languages) | {member.language}
                                )
                        else:
                            profile.member_types = sorted(
                                set(profile.member_types)
                                | {
                                    str(t)
                                    for t in graph.objects(member, RDF.type)
                                    if isinstance(t, URIRef)
                                }
                            )
        result.extend(profiles[key] for key in sorted(profiles))
    return result
