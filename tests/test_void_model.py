"""Tests for VoID Pydantic models."""

from rdflib import Graph

from rdfsolve.schema_models.void_model import (
    VoidClassPartition,
    VoidDataset,
    VoidDatasetDescription,
    VoidLinkset,
    VoidPropertyPartition,
)


def test_void_dataset_roundtrip():
    """Test VoidDataset serialization and parsing."""
    dataset = VoidDataset(
        uri="http://example.org/dataset",
        title="Test Dataset",
        description="A test dataset",
        sparql_endpoint="http://example.org/sparql",
        classes_count=10,
        properties_count=5,
        triples=1000,
    )

    g = Graph()
    dataset.to_rdf(g)

    # Parse back
    from rdflib import URIRef

    parsed = VoidDataset.from_rdf(g, URIRef(dataset.uri))

    assert parsed.title == dataset.title
    assert parsed.classes_count == dataset.classes_count
    assert parsed.triples == dataset.triples


def test_void_linkset_roundtrip():
    """Test VoidLinkset serialization and parsing."""
    linkset = VoidLinkset(
        uri="http://example.org/linkset/1",
        subjects_target_class="http://example.org/Person",
        link_predicate="http://example.org/knows",
        objects_target_class="http://example.org/Person",
        triples=50,
    )

    g = Graph()
    linkset.to_rdf(g)

    from rdflib import URIRef

    parsed = VoidLinkset.from_rdf(g, URIRef(linkset.uri))

    assert parsed.subjects_target_class == linkset.subjects_target_class
    assert parsed.link_predicate == linkset.link_predicate
    assert parsed.objects_target_class == linkset.objects_target_class


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
