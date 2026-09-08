"""RDF metadata evidence and explicit projections into schema fields."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from rdflib import Dataset, Graph, Namespace, URIRef
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
    rdf_dataset: Dataset | None = Field(default=None, repr=False, exclude=True)
    endpoint: str | None = None
    graph_uris: list[str] | None = None
    scope: str = "root descriptions and two blank-node levels"

    def __repr__(self) -> str:
        return (f"MetadataDocument(triples={len(self.graph)}, "
                f"resources={len(set(self.graph.subjects()))}; print to view)")

    def __str__(self) -> str:
        return self.to_markdown()

    def _repr_markdown_(self) -> str:
        return self.to_markdown()

    def to_markdown(self, *, max_resources: int = 5, max_values: int = 30) -> str:
        """Render a bounded view. Keep all values in the underlying RDF."""
        from html import escape

        from rdflib import Literal

        if max_resources < 1 or max_values < 1:
            raise ValueError("Display limits must be positive")

        def cell(text: str) -> str:
            # Escape source text before displaying it as Markdown.
            for token in ("\\", "`", "*", "_", "[", "]", "|", "#"):
                text = text.replace(token, "\\" + token)
            return escape(text).replace("\r", "").replace("\n", "<br>")

        subjects = set(self.graph.subjects())
        roots = set(self.graph.subjects(RDF.type, VOID.Dataset)) | set(
            self.graph.subjects(RDF.type, DCAT.Dataset)
        )
        partitions = set(self.graph.objects(None, VOID.classPartition)) | set(
            self.graph.objects(None, VOID.propertyPartition)
        )
        ordered = sorted(subjects, key=lambda node: (node not in roots or node in partitions, str(node)))
        lines = ["## Metadata view", ""]
        if self.scope.startswith("rdfsolve export"):
            lines.append("Generated from stored schema fields; not original source metadata.")
        else:
            lines.append("Retrieved information; not a complete description of the endpoint.")
        lines += ["", f"{len(self.graph)} RDF statements across {len(subjects)} resources."]
        if not subjects:
            lines += ["", "No information was retrieved in this scope."]
        for subject in ordered[:max_resources]:
            lines += ["", "### " + cell(str(subject)), "", "| Property | Value |", "| --- | --- |"]
            values = sorted(self.graph.predicate_objects(subject), key=lambda pair: (str(pair[0]), pair[1].n3()))
            for predicate, value in values[:max_values]:
                label = self.graph.namespace_manager.normalizeUri(str(predicate))
                if isinstance(value, Literal):
                    text = str(value)
                    if value.language:
                        text += f" ({value.language})"
                    elif value.datatype:
                        text += " [" + self.graph.namespace_manager.normalizeUri(value.datatype) + "]"
                else:
                    text = str(value)
                if len(text) > 240:
                    text = text[:240] + "… [shortened]"
                lines.append(f"| {cell(label)} | {cell(text)} |")
            if len(values) > max_values:
                lines += ["", f"{len(values) - max_values} more values are not shown."]
        if len(subjects) > max_resources:
            lines += ["", f"{len(subjects) - max_resources} more resources are not shown."]
        lines += ["", "Use to_turtle() for all retained RDF."]
        return "\n".join(lines)

    def for_graph(self, graph_uri: str | None) -> MetadataDocument:
        """Select one retained context; None selects the default graph."""
        if self.rdf_dataset is None:
            raise ValueError("No dataset contexts were retained")
        graph = (self.rdf_dataset.default_context if graph_uri is None
                 else self.rdf_dataset.graph(graph_uri))
        return MetadataDocument(graph=graph + Graph(), endpoint=self.endpoint,
                                graph_uris=[graph_uri] if graph_uri else None,
                                scope=self.scope)

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
            partitions = set(self.graph.objects(None, VOID.classPartition)) | set(
                self.graph.objects(None, VOID.propertyPartition)
            ) | set(self.graph.objects(None, URIRef("http://ldf.fi/void-ext#datatypePartition")))
            candidates -= partitions
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
        identity_key = "metadata_subject_iri" if isinstance(subject, URIRef) else "metadata_subject_blank_node"
        result[identity_key] = str(subject)
        result["metadata_identity_basis"] = basis
        return result
