"""Test model classes."""

from rdfsolve.models import SchemaPattern


def test_schema_pattern():
    pattern = SchemaPattern(
        subject_class="http://ex.org/Person",
        property_uri="http://ex.org/name",
        object_class="Literal",
        datatype="http://www.w3.org/2001/XMLSchema#string",
        count=100,
    )
    assert pattern.count == 100
