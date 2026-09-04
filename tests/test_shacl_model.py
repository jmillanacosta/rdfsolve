"""Tests for SHACL Pydantic models."""

from rdflib import Graph

from rdfsolve.schema_models.shacl_model import (
    ShaclNodeShape,
    ShaclPropertyShape,
    ShaclShapesGraph,
)


def test_node_shape_roundtrip():
    """Test ShaclNodeShape serialization and parsing."""
    shape = ShaclNodeShape(
        uri="http://example.org/PersonShape",
        target_class="http://example.org/Person",
        closed=True,
        property_shapes=[
            ShaclPropertyShape(
                path="http://example.org/name",
                datatype="http://www.w3.org/2001/XMLSchema#string",
                min_count=1,
                max_count=1,
            ),
            ShaclPropertyShape(
                path="http://example.org/knows",
                class_constraint="http://example.org/Person",
                node_kind="IRI",
            ),
        ],
    )

    g = Graph()
    shape.to_rdf(g)

    from rdflib import URIRef

    parsed = ShaclNodeShape.from_rdf(g, URIRef(shape.uri))

    assert parsed.target_class == shape.target_class
    assert parsed.closed == shape.closed
    assert len(parsed.property_shapes) == 2


def test_property_shape_roundtrip():
    """Test ShaclPropertyShape serialization and parsing."""
    ps = ShaclPropertyShape(
        uri="http://example.org/ps/1",
        path="http://example.org/name",
        datatype="http://www.w3.org/2001/XMLSchema#string",
        min_count=1,
        max_count=1,
    )

    g = Graph()
    ps.to_rdf(g)

    from rdflib import URIRef

    parsed = ShaclPropertyShape.from_rdf(g, URIRef(ps.uri))

    assert parsed.path == ps.path
    assert parsed.datatype == ps.datatype
    assert parsed.min_count == ps.min_count
    assert parsed.max_count == ps.max_count


def test_shapes_graph():
    """Test ShaclShapesGraph."""
    shapes = ShaclShapesGraph(
        node_shapes=[
            ShaclNodeShape(
                uri="http://example.org/PersonShape",
                target_class="http://example.org/Person",
                property_shapes=[
                    ShaclPropertyShape(
                        path="http://example.org/name",
                        datatype="http://www.w3.org/2001/XMLSchema#string",
                    ),
                ],
            ),
        ],
    )

    g = shapes.to_rdf()
    parsed = ShaclShapesGraph.from_rdf(g)

    assert len(parsed.node_shapes) == 1
    assert parsed.node_shapes[0].target_class == "http://example.org/Person"


def test_node_kind_mapping():
    """Test that nodeKind values are mapped correctly."""
    for nk in ["IRI", "Literal", "BlankNode"]:
        ps = ShaclPropertyShape(
            path="http://example.org/prop",
            node_kind=nk,
        )

        g = Graph()
        node = ps.to_rdf(g)
        parsed = ShaclPropertyShape.from_rdf(g, node)

        assert parsed.node_kind == nk
