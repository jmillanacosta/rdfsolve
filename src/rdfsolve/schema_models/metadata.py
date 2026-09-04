"""Infrastructure metadata patterns (DCAT/VoID-ext)."""

from __future__ import annotations

from pydantic import BaseModel
from rdflib import RDF, Graph, Namespace, URIRef


class DatasetDescription(BaseModel):
    """void:Dataset or dcat:Dataset description."""

    uri: str
    title: str | None = None
    description: str | None = None
    homepage: str | None = None
    sparql_endpoint: str | None = None
    license: str | None = None


class ServiceDescription(BaseModel):
    """SPARQL service description."""

    endpoint: str
    supported_language: list[str] = []
    result_format: list[str] = []
    feature: list[str] = []


class MetadataPatterns(BaseModel):
    """Infrastructure metadata patterns."""

    datasets: list[DatasetDescription] = []
    services: list[ServiceDescription] = []

    def to_rdf_graph(self, base_uri: str = "http://example.org/metadata/") -> Graph:
        """Export as RDFLib Graph."""
        g = Graph()
        VOID = Namespace("http://rdfs.org/ns/void#")
        DCAT = Namespace("http://www.w3.org/ns/dcat#")
        DCTERMS = Namespace("http://purl.org/dc/terms/")
        FOAF = Namespace("http://xmlns.com/foaf/0.1/")

        g.bind("void", VOID)
        g.bind("dcat", DCAT)
        g.bind("dcterms", DCTERMS)
        g.bind("foaf", FOAF)

        for ds in self.datasets:
            ds_uri = URIRef(ds.uri)
            g.add((ds_uri, RDF.type, VOID.Dataset))
            if ds.title:
                g.add((ds_uri, DCTERMS.title, RDF.Literal(ds.title)))
            if ds.description:
                g.add((ds_uri, DCTERMS.description, RDF.Literal(ds.description)))
            if ds.homepage:
                g.add((ds_uri, FOAF.homepage, URIRef(ds.homepage)))
            if ds.sparql_endpoint:
                g.add((ds_uri, VOID.sparqlEndpoint, URIRef(ds.sparql_endpoint)))
            if ds.license:
                g.add((ds_uri, DCTERMS.license, URIRef(ds.license)))

        return g

    def to_turtle(self, base_uri: str = "http://example.org/metadata/") -> str:
        """Export as DCAT/VoID Turtle."""
        g = self.to_rdf_graph(base_uri)
        return g.serialize(format="turtle")


__all__ = ["DatasetDescription", "MetadataPatterns", "ServiceDescription"]
