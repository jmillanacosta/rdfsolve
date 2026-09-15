"""Tests for SHACL Pydantic models."""

from rdflib import Graph

from rdfsolve.schema_models.shacl_model import (
    ShaclNodeShape,
    ShaclPropertyShape,
    ShaclShapesGraph,
)


def test_mined_prefixes_survive_json_shacl_and_void():
    from rdflib import SH, XSD, URIRef

    from rdfsolve.schema_models import MinedSchema
    from rdfsolve.schema_models.exporters.shacl import minedschema_to_shacl

    schema = MinedSchema(about={}, prefixes={"mine": "https://example.org/vocabulary/"}, patterns=[{
        "subject_class": "https://example.org/vocabulary/Item",
        "property_uri": "https://example.org/vocabulary/name",
        "object_class": "Literal", "datatype": str(XSD.string),
    }])
    schema = MinedSchema.from_dict(schema.to_dict())
    shapes = minedschema_to_shacl(schema, base_uri="urn:shapes")
    graph = shapes.to_rdf()
    declarations = {str(graph.value(d, SH.prefix)): graph.value(d, SH.namespace)
                    for d in graph.objects(URIRef("urn:shapes"), SH.declare)}
    assert str(declarations["mine"]) == schema.prefixes["mine"]
    assert declarations["mine"].datatype == XSD.anyURI
    assert "mine:Item" in schema.to_shacl(base_uri="urn:shapes")
    restored = MinedSchema.from_shacl(schema.to_shacl(base_uri="urn:shapes"))
    assert restored.prefixes["mine"] == schema.prefixes["mine"]
    assert restored.shapes.prefix_declarations["urn:shapes"]
    restored = MinedSchema.from_void(schema.to_void_graph().serialize(format="turtle"))
    assert restored.prefixes["mine"] == schema.prefixes["mine"]


def test_declarations_without_queries_supply_prefixes_to_source_shapes():
    from rdfsolve.schema_models import MinedSchema

    schema = MinedSchema.from_shacl('''
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        <urn:shapes> sh:declare [sh:prefix "local"; sh:namespace "urn:local:"^^xsd:anyURI] .
        <urn:shape> sh:targetClass <urn:local:Item> .
    ''')
    assert schema.prefixes["local"] == "urn:local:"
    assert "local:Item" in schema.to_shacl()


def test_derived_prefixes_keep_both_colliding_namespaces():
    from rdfsolve.schema_models import MinedSchema

    schema = MinedSchema(about={}, prefixes={"terms": "https://one.test/terms/"}, patterns=[{
        "subject_class": "https://one.test/terms/Item",
        "property_uri": "https://two.test/terms/link",
        "object_class": "https://two.test/terms/Other",
    }])
    assert schema.get_prefixes() == {
        "terms": "https://one.test/terms/", "terms_2": "https://two.test/terms/"
    }
    graph = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert set(schema.get_prefixes().values()) <= {str(ns) for _, ns in graph.namespaces()}


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
