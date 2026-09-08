"""Run metadata queries on saved AOPWiki RDF, without endpoint requests."""

from pathlib import Path
from unittest.mock import Mock

from rdflib import Dataset, Graph
from rdflib.compare import isomorphic

from rdfsolve.metadata import query_metadata_document
from rdfsolve.schema_models.exporters.text import trim_descriptions

DATA = Path(__file__).parent / "test_data" / "aopwikirdf_generated_void.ttl"


def test_explicit_subject_keeps_unprojected_predicates_and_graph_scope():
    dataset = Dataset()
    source = dataset.graph("urn:metadata")
    source.parse(DATA, format="turtle")
    subject = str(next(source.subjects()))
    helper = Mock(endpoint_url="https://example.org/sparql")
    helper.construct.side_effect = lambda query: dataset.query(query).serialize(format="turtle").decode()
    document = query_metadata_document(helper, graph_uris=["urn:metadata"], subject_iris=[subject])
    expected = Graph()
    for triple in source.triples((next(node for node in source.subjects() if str(node) == subject), None, None)):
        expected.add(triple)
    assert isomorphic(document.graph, expected)
    assert document.project(subject)["metadata_subject_iri"] == subject
    assert isomorphic(trim_descriptions(document, 20).graph, expected)
    assert not query_metadata_document(
        helper, graph_uris=["urn:absent"], subject_iris=[subject]
    ).graph
    assert not query_metadata_document(helper, graph_uris=["urn:metadata"]).graph
