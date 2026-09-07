"""Check RDF values at model boundaries."""

import pytest
from rdflib import BNode, Graph, Literal, Namespace, URIRef

from rdfsolve.schema_models._rdf import optional_count
from rdfsolve.schema_models.shacl_model import ShaclPropertyShape
from rdfsolve.schema_models.void_model import VoidDataset


@pytest.mark.parametrize(
    "value,expected", [(None, None), (Literal(0), 0), (Literal(2**60 + 1), 2**60 + 1)]
)
def test_optional_count(value, expected):
    assert optional_count(value) == expected


@pytest.mark.parametrize("value", [URIRef("urn:count:1"), Literal(-1), Literal("1.5")])
def test_invalid_count(value):
    with pytest.raises(ValueError):
        optional_count(value)


def test_void_zero_count():
    graph = Graph()
    dataset = VoidDataset(uri="urn:dataset", triples=0, classes_count=0)
    node = dataset.to_rdf(graph)
    assert VoidDataset.from_rdf(graph, node).triples == 0
    assert VoidDataset.from_rdf(graph, node).classes_count == 0


def test_shacl_blank_shape_and_zero_cardinality():
    graph = Graph()
    shape = ShaclPropertyShape(path="urn:property", min_count=0, max_count=0)
    node = shape.to_rdf(graph)
    assert isinstance(node, BNode)
    restored = ShaclPropertyShape.from_rdf(graph, node)
    assert restored.uri is None
    assert restored.min_count == restored.max_count == 0
    assert (node, Namespace("http://www.w3.org/ns/shacl#").maxCount, Literal(0)) in graph
