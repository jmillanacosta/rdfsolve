"""Tests for VoID conversion functions."""

from rdfsolve.schema_models.core import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.void_convert import minedschema_to_void, void_to_minedschema


def test_roundtrip_simple():
    """Test simple MinedSchema -> VoID -> MinedSchema roundtrip."""
    original = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/name",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#string",
                count=100,
            ),
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/knows",
                object_class="http://example.org/Person",
                count=50,
            ),
        ],
        about=AboutMetadata.build(
            dataset_name="test",
            class_count=1,
            property_count=2,
            pattern_count=2,
        ),
    )

    # Convert to VoID
    from rdflib import Graph

    void_dataset = minedschema_to_void(original, base_url="http://example.org")
    g = Graph()
    void_dataset.to_rdf(g)
    void_ttl = g.serialize(format="turtle")

    # Parse back
    roundtrip = void_to_minedschema(void_ttl)

    # Verify pattern count
    assert len(roundtrip.patterns) == len(original.patterns)

    # Verify pattern content
    original_keys = {(p.subject_class, p.property_uri, p.object_class) for p in original.patterns}
    roundtrip_keys = {(p.subject_class, p.property_uri, p.object_class) for p in roundtrip.patterns}
    assert original_keys == roundtrip_keys


def test_roundtrip_with_datatypes():
    """Test roundtrip with multiple datatypes."""
    original = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/name",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#string",
                count=100,
            ),
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/age",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#integer",
                count=100,
            ),
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/birthdate",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#date",
                count=95,
            ),
        ],
        about=AboutMetadata.build(
            dataset_name="test",
            class_count=1,
            property_count=3,
            pattern_count=3,
        ),
    )

    # Convert to VoID
    from rdflib import Graph

    void_dataset = minedschema_to_void(original, base_url="http://example.org")
    g = Graph()
    void_dataset.to_rdf(g)
    void_ttl = g.serialize(format="turtle")

    # Parse back
    roundtrip = void_to_minedschema(void_ttl)

    # Verify
    assert len(roundtrip.patterns) == len(original.patterns)

    # Verify datatypes are preserved
    original_datatypes = {(p.subject_class, p.property_uri, p.datatype) for p in original.patterns}
    roundtrip_datatypes = {
        (p.subject_class, p.property_uri, p.datatype) for p in roundtrip.patterns
    }
    assert original_datatypes == roundtrip_datatypes


def test_from_void_method():
    """Test MinedSchema.from_void() method."""
    original = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="http://example.org/Person",
                property_uri="http://example.org/name",
                object_class="Literal",
                datatype="http://www.w3.org/2001/XMLSchema#string",
                count=100,
            ),
        ],
        about=AboutMetadata.build(dataset_name="test", pattern_count=1),
    )

    # Generate VoID
    void_graph = original.to_void_graph()
    void_ttl = void_graph.serialize(format="turtle")

    # Parse using from_void() method
    roundtrip = MinedSchema.from_void(void_ttl)

    assert len(roundtrip.patterns) == 1
    assert roundtrip.patterns[0].subject_class == "http://example.org/Person"
    assert roundtrip.patterns[0].property_uri == "http://example.org/name"
    assert roundtrip.patterns[0].object_class == "Literal"


def test_mixed_class_and_datatype_partitions_keep_their_own_counts():
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="mixed"),
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri="urn:p",
                object_class=kind,
                datatype=datatype,
                count=count,
            )
            for kind, datatype, count in [
                ("urn:B", None, 0),
                ("urn:C", None, None),
                ("Literal", "http://www.w3.org/2001/XMLSchema#string", 2**54 + 1),
                ("Literal", "http://www.w3.org/2001/XMLSchema#integer", 12),
            ]
        ],
    )
    restored = MinedSchema.from_dict(schema.to_jsonld())
    key = lambda p: (p.object_class, p.datatype, p.count)
    assert {key(p) for p in restored.patterns} == {key(p) for p in schema.patterns}
