from rdfsolve.schema_models.core import MinedSchema
import json

# Load the schema
schema = MinedSchema.from_jsonld("/home/javier.millanacosta/rdfsolve/output-new/aopwikirdf/aopwikirdf_schema.jsonld")

print(f"Loaded {len(schema.patterns)} patterns")

# Check if new fields exist
for i, pat in enumerate(schema.patterns[:5]):
    print(f"\n=== Pattern {i+1} ===")
    print(f"Subject: {pat.subject_label}")
    print(f"Property: {pat.property_label}")
    print(f"Object: {pat.object_label}")
    print(f"Subject desc: {pat.subject_description}")
    print(f"Property desc: {pat.property_description}")
    print(f"Min count: {pat.min_count}")
    print(f"Max count: {pat.max_count}")
    print(f"Subject examples: {pat.subject_examples}")
    print(f"Value examples: {pat.value_examples}")

# Test JSON Schema export
print("\n\n=== Testing JSON Schema Export ===")
try:
    jsonschema = schema.to_jsonschema()
    print(f"Generated JSON Schema with {len(jsonschema.get('$defs', {}))} class definitions")
    
    # Save it
    with open("/home/javier.millanacosta/rdfsolve/output-new/aopwikirdf/aopwikirdf_schema.json", "w") as f:
        json.dump(jsonschema, f, indent=2)
    print("Saved to aopwikirdf_schema.json")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Test Pydantic models
print("\n\n=== Testing Pydantic Models ===")
try:
    models = schema.to_pydantic_models()
    print(f"Generated {len(models)} Pydantic models")
    for name in list(models.keys())[:5]:
        print(f"  - {name}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
