"""Tests for SHACL conversion functions."""

from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.readers.shacl import shacl_to_minedschema
from rdfsolve.schema_models.exporters.shacl import minedschema_to_shacl


def test_roundtrip():
    """Test MinedSchema -> SHACL -> MinedSchema roundtrip."""
    original = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/name",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#string",
            ),
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/knows",
                object_class="http://example.org/Person",
            ),
        ],
        about=AboutMetadata.build(pattern_count=2, class_count=1),
    )

    # Convert to SHACL
    shapes = minedschema_to_shacl(original)
    g = shapes.to_rdf()
    shacl_ttl = g.serialize(format="turtle")

    # Parse back
    roundtrip = shacl_to_minedschema(shacl_ttl)

    assert len(roundtrip.patterns) == len(original.patterns)

    original_keys = {(p.subject_class, p.property_uri, p.object_class) for p in original.patterns}
    roundtrip_keys = {(p.subject_class, p.property_uri, p.object_class) for p in roundtrip.patterns}
    assert original_keys == roundtrip_keys


def test_roundtrip_with_multiple_classes():
    """Test roundtrip with multiple subject classes."""
    original = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/name",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#string",
            ),
            SchemaPattern(
                subject_class="http://example.org/Organization",
                property_uri="http://example.org/name",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#string",
            ),
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/worksAt",
                object_class="http://example.org/Organization",
            ),
        ],
        about=AboutMetadata.build(pattern_count=3, class_count=2),
    )

    # Convert to SHACL
    shapes = minedschema_to_shacl(original)

    # Should have 2 NodeShapes (one per class)
    assert len(shapes.node_shapes) == 2

    # Convert back
    g = shapes.to_rdf()
    shacl_ttl = g.serialize(format="turtle")
    roundtrip = shacl_to_minedschema(shacl_ttl)

    assert len(roundtrip.patterns) == len(original.patterns)


def test_from_shacl_method():
    """Test MinedSchema.from_shacl() method."""
    from rdfsolve.schema_models.exporters.shacl import minedschema_to_shacl

    original = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/name",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#string",
            ),
        ],
        about=AboutMetadata.build(pattern_count=1),
    )

    # Generate SHACL
    shapes = minedschema_to_shacl(original)
    shacl_ttl = shapes.to_rdf().serialize(format="turtle")

    # Parse using from_shacl() method
    roundtrip = MinedSchema.from_shacl(shacl_ttl)

    assert len(roundtrip.patterns) == 1
    assert roundtrip.patterns[0].subject_class == "http://example.org/Person"
    assert roundtrip.patterns[0].property_uri == "http://example.org/name"
    assert roundtrip.patterns[0].object_class == "Literal"


def test_datatype_preservation():
    """Test that datatypes are preserved in roundtrip."""
    original = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/age",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#integer",
            ),
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/birthdate",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#date",
            ),
        ],
        about=AboutMetadata.build(pattern_count=2),
    )

    # Roundtrip
    shapes = minedschema_to_shacl(original)
    shacl_ttl = shapes.to_rdf().serialize(format="turtle")
    roundtrip = shacl_to_minedschema(shacl_ttl)

    # Verify datatypes
    original_datatypes = {(p.property_uri, p.datatype) for p in original.patterns}
    roundtrip_datatypes = {(p.property_uri, p.datatype) for p in roundtrip.patterns}
    assert original_datatypes == roundtrip_datatypes


def test_mixed_values_use_alternatives_not_conflicting_node_kinds():
    """Regress the mixed literal/resource shape produced by mining."""
    from rdflib import Graph, Namespace
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="mixed"),
        patterns=[
            SchemaPattern(
                subject_class="urn:A", property_uri="urn:p", object_class=kind, datatype=datatype
            )
            for kind, datatype in [
                ("urn:B", None),
                ("Resource", None),
                ("BlankNode", None),
                ("Literal", "http://www.w3.org/2001/XMLSchema#string"),
            ]
        ],
    )
    graph = Graph().parse(data=schema.to_shacl(), format="turtle")
    sh = Namespace("http://www.w3.org/ns/shacl#")
    heads = list(graph.objects(None, sh["or"]))
    assert len(heads) == 1
    assert len(list(graph.items(heads[0]))) == 4
    assert all(
        len(list(graph.objects(subject, sh.nodeKind))) == 1
        for subject in graph.subjects(sh.nodeKind, None)
    )
    restored = MinedSchema.from_shacl(schema.to_shacl())
    assert {p.object_class for p in restored.patterns} == {p.object_class for p in schema.patterns}
