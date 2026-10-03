from __future__ import annotations

from unittest.mock import Mock

from rdflib import RDF, Dataset, Namespace, URIRef
from rdfsolve.void_retrieval import retrieve_description

VOID = Namespace("http://rdfs.org/ns/void#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")


def test_retrieve_description_keeps_sd_name_and_graph_description():
    data = Dataset()
    meta = data.graph(URIRef("urn:g:meta"))
    dataset = URIRef("urn:dataset")
    named = URIRef("urn:named-description")
    graph_desc = URIRef("urn:graph-description")
    data_graph = URIRef("urn:g:data")
    meta.add((dataset, RDF.type, VOID.Dataset))
    meta.add((dataset, SD.namedGraph, named))
    meta.add((named, RDF.type, SD.NamedGraph))
    meta.add((named, SD.name, data_graph))
    meta.add((named, SD.graph, graph_desc))
    meta.add((graph_desc, RDF.type, SD.Graph))
    meta.add((graph_desc, VOID.triples, URIRef("urn:count-placeholder")))
    helper = Mock(endpoint_url="https://example.org/sparql")
    helper.construct.side_effect = lambda query: (
        lambda value: value.decode() if isinstance(value, bytes) else str(value)
    )(data.query(query).serialize(format="turtle"))
    graph = retrieve_description(helper, ["urn:g:meta"])
    assert (named, SD.name, data_graph) in graph
    assert (named, SD.graph, graph_desc) in graph
    assert (graph_desc, RDF.type, SD.Graph) in graph
