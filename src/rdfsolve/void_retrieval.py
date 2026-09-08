"""Retrieve published VoID without paging RDF blank-node identifiers."""

from __future__ import annotations

import logging

from rdflib import Graph, URIRef

from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper

logger = logging.getLogger(__name__)
PREFIXES = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
"""


def graph_scope(body: str, graphs: list[str] | None) -> str:
    """Use None for the default graph, not every named graph."""
    if graphs is None:
        return body
    values = " ".join(URIRef(uri).n3() for uri in graphs)
    return "VALUES ?g { " + values + " } GRAPH ?g { " + body + " }"


def discover_graph_names(
    helper: SparqlHelper, *, batch_size: int, max_pages: int
) -> list[str]:
    """List graph names without counting their triples."""
    query = "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } } ORDER BY ?g"
    return [
        row["g"]["value"]
        for page in helper.select_chunked(
            helper.prepare_paginated_query(query),
            chunk_size=batch_size,
            max_pages=max_pages,
            purpose="void/graph-discovery",
        )
        for row in page
    ]


def retrieve_description(helper: SparqlHelper, graphs: list[str] | None) -> Graph:
    """Fetch each connected description in one response.

    Split graph batches on request limits. A single oversized graph raises.
    Never page triples: blank-node names do not persist between responses.
    """
    body = """
    {
      ?s ?vp ?value .
      FILTER(STRSTARTS(STR(?vp), STR(void:)) ||
             STRSTARTS(STR(?vp), STR(void-ext:)))
      ?s ?p ?o .
    } UNION {
      ?s a ?kind ; ?p ?o .
      VALUES ?kind { void:Dataset void:Linkset }
    } UNION {
      ?partition ?termPredicate ?s .
      VALUES ?termPredicate { void:class void:property void-ext:datatype }
      ?s ?p ?o .
      VALUES ?p {
        <http://www.w3.org/2000/01/rdf-schema#label>
        <http://www.w3.org/2000/01/rdf-schema#comment>
        <http://www.w3.org/2004/02/skos/core#prefLabel>
        <http://www.w3.org/2004/02/skos/core#definition>
        <http://purl.obolibrary.org/obo/IAO_0000115>
      }
    } UNION {
      ?s <http://xmlns.com/foaf/0.1/primaryTopic> ?dataset ; ?p ?o .
      ?dataset a void:Dataset .
    }
    """
    query = PREFIXES + "CONSTRUCT { ?s ?p ?o } WHERE { " + graph_scope(body, graphs) + " }"
    try:
        text = helper.construct(query)
        return Graph().parse(data=text, format="turtle", publicID=helper.endpoint_url)
    except EndpointTimeoutError:
        if graphs is None or len(graphs) < 2:
            raise
        middle = len(graphs) // 2
        logger.warning("Split VoID retrieval batch of %d graphs", len(graphs))
        return retrieve_description(helper, graphs[:middle]) + retrieve_description(
            helper, graphs[middle:]
        )


def discover_description(
    helper: SparqlHelper,
    graphs: list[str] | None,
    *,
    batch_size: int,
    graph_batch_size: int,
    max_pages: int,
    excluded_prefixes: tuple[str, ...] = (),
) -> tuple[Graph, list[str], bool]:
    """Try named VoID candidates, other named graphs, then the default graph."""
    if min(batch_size, graph_batch_size, max_pages) < 1:
        raise ValueError("Batch sizes and max_pages must be positive")
    if graphs == []:
        raise ValueError("Use graph_uris=None for all named graphs, or select at least one graph")
    names = graphs if graphs is not None else discover_graph_names(
        helper, batch_size=batch_size, max_pages=max_pages
    )
    names = sorted({name for name in names if not name.startswith(excluded_prefixes)})
    candidates = [name for name in names if "void" in name.lower()]
    scopes = [names] if graphs is not None else [
        candidates, [name for name in names if name not in candidates]
    ]
    result = Graph()
    found: list[str] = []
    for scope in scopes:
        for start in range(0, len(scope), graph_batch_size):
            batch = scope[start : start + graph_batch_size]
            # Select graph identities separately. Empty candidate graphs must
            # not suppress the fallback or appear in the coverage report.
            body = """?s ?p ?o .
                FILTER(STRSTARTS(STR(?p), STR(void:)) ||
                       STRSTARTS(STR(?p), STR(void-ext:)) ||
                       (?p = <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> &&
                        ?o IN (void:Dataset, void:Linkset)))"""
            query = PREFIXES + "SELECT DISTINCT ?g WHERE { " + graph_scope(body, batch) + " } ORDER BY ?g"
            matched: list[str] = [
                row["g"]["value"]
                for page in helper.select_chunked(
                    helper.prepare_paginated_query(query),
                    chunk_size=batch_size,
                    max_pages=max_pages,
                    purpose="void/catalog-discovery",
                )
                for row in page
            ]
            if matched:
                result += retrieve_description(helper, matched)
                found.extend(matched)
        if len(result):
            return result, found, False
    if graphs is None:
        result = retrieve_description(helper, None)
    return result, found, bool(len(result))
