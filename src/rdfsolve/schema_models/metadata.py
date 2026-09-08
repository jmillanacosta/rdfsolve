"""RDF metadata evidence and explicit projections into schema fields."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import DC, DCTERMS, FOAF, OWL, RDF
from rdflib.term import Node

logger = logging.getLogger(__name__)
VOID = Namespace("http://rdfs.org/ns/void#")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
PAV = Namespace("http://purl.org/pav/")

# Aliases select candidate values; they do not equate the source predicates.
FIELDS = {
    "title": (DCTERMS.title, DC.title),
    "description": (DCTERMS.description, DC.description),
    "source_license": (DCTERMS.license,),
    "source_publisher": (DCTERMS.publisher, DC.publisher),
    "source_creator": (DCTERMS.creator, DC.creator, PAV.authoredBy),
    "source_version": (OWL.versionInfo, PAV.version, DCAT.version),
    "source_version_iri": (OWL.versionIRI,),
    "source_issued": (DCTERMS.issued,),
    "source_modified": (DCTERMS.modified, PAV.lastUpdateOn),
    "homepage": (FOAF.homepage,),
}


class MetadataDocument(BaseModel):
    """Keep retrieved RDF without requiring one metadata vocabulary."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    graph: Graph = Field(repr=False, exclude=True)
    endpoint: str | None = None
    graph_uris: list[str] | None = None
    scope: str = "root descriptions and two blank-node levels"

    def to_rdf_graph(self) -> Graph:
        """Return a copy of the evidence, not reconstructed metadata."""
        return self.graph + Graph()

    def to_turtle(self) -> str:
        """Serialize the retrieved RDF."""
        return self.graph.serialize(format="turtle")

    def project(self, subject_iri: str | None = None) -> dict[str, Any]:
        """Project one dataset. Leave absent or ambiguous scalar fields unset.

        An explicit subject may use any vocabulary. Automatic selection only
        considers declared datasets, not services or imported ontologies.
        """
        subject: Node
        if subject_iri is not None:
            subject = URIRef(subject_iri)
            if not any(self.graph.triples((subject, None, None))):
                raise ValueError(f"Metadata subject was not retrieved: {subject_iri}")
            basis = "explicit_subject"
        else:
            candidates = set(self.graph.subjects(RDF.type, VOID.Dataset)) | set(
                self.graph.subjects(RDF.type, DCAT.Dataset)
            )
            linked = {
                node for node in candidates
                if self.endpoint and any(
                    str(endpoint).rstrip("/") == self.endpoint.rstrip("/")
                    for endpoint in self.graph.objects(node, VOID.sparqlEndpoint)
                )
            }
            candidates = linked or candidates
            if len(candidates) != 1:
                logger.info("Metadata projection has %d dataset candidates; select a subject",
                            len(candidates))
                return {}
            subject = next(iter(candidates))
            basis = "endpoint_link" if linked else "single_dataset"
        result: dict[str, Any] = {}
        for name, predicates in FIELDS.items():
            values = {value for predicate in predicates
                      for value in self.graph.objects(subject, predicate)}
            if name == "source_creator" and values:
                result[name] = sorted({str(value) for value in values})
            elif len(values) == 1:
                result[name] = str(next(iter(values)))
            elif len(values) > 1:
                logger.warning("Leave conflicting metadata field %s unset for %s", name, subject)
        result["metadata_subject_iri"] = str(subject)
        result["metadata_identity_basis"] = basis
        return result
