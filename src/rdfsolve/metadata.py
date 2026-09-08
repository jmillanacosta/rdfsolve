"""Retrieve RDF metadata, then project one identified dataset."""

from __future__ import annotations

from typing import Any

from rdflib import Dataset, Graph, URIRef

from rdfsolve.schema_models.metadata import MetadataDocument
from rdfsolve.sparql_helper import SparqlHelper

DEFAULT_TYPES = (
    "http://rdfs.org/ns/void#Dataset",
    "http://www.w3.org/ns/dcat#Dataset",
    "http://www.w3.org/ns/sparql-service-description#Service",
)


def build_metadata_query(
    *,
    subject_iris: list[str] | None = None,
    resource_types: tuple[str, ...] = DEFAULT_TYPES,
) -> str:
    """Read all predicates on selected roots and two blank-node levels.

    Blank-node expansion excludes VoID partition links. Metadata retrieval
    is not partition discovery and never asserts that partitions are absent.
    """
    if subject_iris is not None:
        if not subject_iris:
            raise ValueError("subject_iris must not be empty")
        selector = "VALUES ?root { " + " ".join(URIRef(i).n3() for i in subject_iris) + " }"
    else:
        if not resource_types:
            raise ValueError("resource_types must not be empty")
        selector = (
            "VALUES ?kind { "
            + " ".join(URIRef(i).n3() for i in resource_types)
            + " } ?root a ?kind ."
        )
    return (
        """
    CONSTRUCT { ?root ?p ?o . ?o ?bp ?bo . ?bo ?cp ?co }
    WHERE {
    """
        + selector
        + """
      ?root ?p ?o .
      OPTIONAL {
        FILTER(isBlank(?o))
        FILTER(?p NOT IN (
          <http://rdfs.org/ns/void#classPartition>,
          <http://rdfs.org/ns/void#propertyPartition>,
          <http://ldf.fi/void-ext#datatypePartition>
        ))
        ?o ?bp ?bo .
        OPTIONAL { FILTER(isBlank(?bo)) ?bo ?cp ?co }
      }
    }
    """
    )


def query_metadata_document(
    helper: SparqlHelper,
    *,
    graph_uris: list[str] | None = None,
    subject_iris: list[str] | None = None,
    resource_types: tuple[str, ...] = DEFAULT_TYPES,
) -> MetadataDocument:
    """Retrieve scoped RDF sequentially. Request limits and failures propagate.

    None selects the default graph; an explicit list selects only those
    named graphs. No endpoint-wide fallback scan is performed.
    """
    if graph_uris == []:
        raise ValueError("graph_uris must be None or a nonempty list")
    query = build_metadata_query(subject_iris=subject_iris, resource_types=resource_types)
    graph = Graph()
    dataset = Dataset()
    for uri in graph_uris if graph_uris is not None else [None]:
        scoped = query
        if uri is not None:
            scoped = query.replace("WHERE {", "WHERE { GRAPH " + URIRef(uri).n3() + " {", 1)
            scoped += " }"
        text = helper.construct(scoped)
        retrieved = Graph().parse(data=text, format="turtle", publicID=helper.endpoint_url)
        target = dataset.default_context if uri is None else dataset.graph(uri)
        target += retrieved
        graph += retrieved
    return MetadataDocument(
        graph=graph, rdf_dataset=dataset, endpoint=helper.endpoint_url, graph_uris=graph_uris
    )


def query_endpoint_metadata(
    sparql_helper: SparqlHelper,
    *,
    graph_uris: list[str] | None = None,
    subject_iri: str | None = None,
) -> dict[str, Any]:
    """Return the supported projection; use query_metadata_document for RDF."""
    document = query_metadata_document(
        sparql_helper,
        graph_uris=graph_uris,
        subject_iris=[subject_iri] if subject_iri is not None else None,
    )
    return document.project(subject_iri)
