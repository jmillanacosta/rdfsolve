from rdflib import Graph
from rdfsolve.schema_models.void_model import (
    VoidClassPartition,
    VoidDataset,
    VoidPropertyPartition,
)


def test_nested_partitions():
    """Test nested class and property partitions."""
    dataset = VoidDataset(
        uri="http://example.org/dataset",
        class_partitions=[
            VoidClassPartition(
                uri="http://example.org/cp/1",
                class_uri="http://example.org/Person",
                triples=100,
                property_partitions=[
                    VoidPropertyPartition(
                        uri="http://example.org/pp/1",
                        property_uri="http://example.org/name",
                        triples=100,
                    )
                ],
            )
        ],
    )
    g = Graph()
    dataset.to_rdf(g)
    from rdflib import URIRef

    parsed = VoidDataset.from_rdf(g, URIRef(dataset.uri))
    assert len(parsed.class_partitions) == 1
    assert parsed.class_partitions[0].class_uri == "http://example.org/Person"
    assert len(parsed.class_partitions[0].property_partitions) == 1
