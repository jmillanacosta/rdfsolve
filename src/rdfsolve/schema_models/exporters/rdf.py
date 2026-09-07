"""Attach source annotations and provenance to RDF exports."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rdflib import Graph

    from rdfsolve.schema_models.core import MinedSchema


def annotate_rdf(schema: MinedSchema, graph: Graph, *, include_examples: bool = True) -> None:
    """Add source text, example triples, and document provenance to RDF."""
    from rdflib import Literal as RdfLiteral
    from rdflib import URIRef
    from rdflib.namespace import DCTERMS, OWL

    if include_examples:
        graph += schema.enrichment.to_rdf_graph()
    else:
        terms = {str(term) for triple in graph for term in triple}
        for definition in schema.enrichment.definitions + schema.enrichment.labels:
            if definition.term_iri in terms:
                graph.add(
                    (
                        URIRef(definition.term_iri),
                        URIRef(definition.predicate),
                        definition.text.to_rdf(),
                    )
                )
    document = URIRef("")
    if schema.about.schema_version:
        graph.add((document, OWL.versionInfo, RdfLiteral(schema.about.schema_version)))
    if schema.about.source_version_iri:
        graph.add((document, DCTERMS.source, URIRef(schema.about.source_version_iri)))
    if schema.about.generated_at:
        graph.add((document, DCTERMS.created, RdfLiteral(schema.about.generated_at)))
