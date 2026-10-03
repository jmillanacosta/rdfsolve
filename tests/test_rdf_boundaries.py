from rdflib import BNode, Graph, Literal, Namespace
from rdfsolve.schema_models.shacl_model import ShaclPropertyShape


def test_shacl_blank_shape_and_zero_cardinality():
    graph = Graph()
    shape = ShaclPropertyShape(path="urn:property", min_count=0, max_count=0)
    node = shape.to_rdf(graph)
    assert isinstance(node, BNode)
    restored = ShaclPropertyShape.from_rdf(graph, node)
    assert restored.uri is None
    assert restored.min_count == restored.max_count == 0
    assert (node, Namespace("http://www.w3.org/ns/shacl#").maxCount, Literal(0)) in graph
