from rdfsolve.schema_models.core import SchemaPattern, MinedSchema, AboutMetadata
import json

# Create a simple test schema with the new fields
patterns = [
    SchemaPattern(
        subject_class="http://example.org/Person",
        property_uri="http://example.org/name",
        object_class="Literal",
        datatype="http://www.w3.org/2001/XMLSchema#string",
        count=100,
        subject_label="Person",
        property_label="name",
        object_label="Literal",
        subject_description="A person in the database",
        property_description="Full name of the person",
        min_count=1,
        max_count=1,
        subject_examples=["http://example.org/person/1", "http://example.org/person/2"],
        value_examples=["John Doe", "Jane Smith", "Bob Johnson"],
    ),
    SchemaPattern(
        subject_class="http://example.org/Person",
        property_uri="http://example.org/email",
        object_class="Literal",
        datatype="http://www.w3.org/2001/XMLSchema#string",
        count=150,
        subject_label="Person",
        property_label="email",
        object_label="Literal",
        property_description="Email address",
        min_count=0,
        max_count=3,
        value_examples=["john@example.com", "jane@example.org"],
    ),
]

about = AboutMetadata.build(
    dataset_name="test_dataset",
    pattern_count=2,
    class_count=1,
    property_count=2,
)

schema = MinedSchema(patterns=patterns, about=about)

print("=== Test Schema ===")
print(f"Patterns: {len(schema.patterns)}")
for i, pat in enumerate(schema.patterns):
    print(f"\nPattern {i+1}:")
    print(f"  {pat.subject_label}.{pat.property_label} -> {pat.object_label}")
    print(f"  Description: {pat.property_description}")
    print(f"  Cardinality: [{pat.min_count}..{pat.max_count}]")
    print(f"  Examples: {pat.value_examples[:2] if pat.value_examples else None}")

print("\n\n=== JSON Schema Export ===")
jsonschema = schema.to_jsonschema()
print(json.dumps(jsonschema, indent=2))

print("\n\n=== Pydantic Models ===")
models = schema.to_pydantic_models()
for name, model in models.items():
    print(f"\nClass: {name}")
    print(f"  Doc: {model.__doc__}")
    for field_name, field_info in model.model_fields.items():
        print(f"  Field: {field_name}")
        print(f"    Type: {field_info.annotation}")
        if field_info.description:
            print(f"    Description: {field_info.description}")
        if hasattr(field_info, 'examples') and field_info.examples:
            print(f"    Examples: {field_info.examples}")

print("\n\nSuccess! New features working correctly.")
