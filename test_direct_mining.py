#!/usr/bin/env python3
"""Test mining with new features and export to JSON Schema."""
from rdfsolve.api import mine_schema_from_local
import json

# Mine the aopwikirdf schema with local QLever
print("Mining aopwikirdf schema...")
schema = mine_schema_from_local(
    dataset_name="aopwikirdf",
    download_ttl=[
        "https://raw.githubusercontent.com/marvinm2/AOPWikiRDF/refs/heads/master/data/AOPWikiRDF-Genes.ttl",
        "https://raw.githubusercontent.com/marvinm2/AOPWikiRDF/refs/heads/master/data/AOPWikiRDF-Void.ttl",
        "https://raw.githubusercontent.com/marvinm2/AOPWikiRDF/refs/heads/master/data/AOPWikiRDF.ttl",
    ],
    qlever_port=7050,
    timeout=120.0,
)

print(f"\nMined {len(schema.patterns)} patterns")

# Check new fields
print("\n=== Sample patterns with new fields ===")
for i, pat in enumerate(schema.patterns[:3]):
    print(f"\nPattern {i+1}:")
    print(f"  Subject: {pat.subject_label}")
    print(f"  Property: {pat.property_label}")
    print(f"  Object: {pat.object_label}")
    print(f"  Subject desc: {pat.subject_description[:50] if pat.subject_description else None}")
    print(f"  Property desc: {pat.property_description[:50] if pat.property_description else None}")
    print(f"  Min count: {pat.min_count}, Max count: {pat.max_count}")
    if pat.subject_examples:
        print(f"  Subject examples: {pat.subject_examples[:2]}")
    if pat.value_examples:
        print(f"  Value examples: {pat.value_examples[:2]}")

# Test JSON Schema export
print("\n\n=== Testing JSON Schema Export ===")
try:
    jsonschema = schema.to_jsonschema()
    print(f"Generated JSON Schema with {len(jsonschema.get('$defs', {}))} class definitions")
    
    # Show a sample class definition
    if jsonschema.get('$defs'):
        first_class = list(jsonschema['$defs'].keys())[0]
        print(f"\nSample class: {first_class}")
        props = jsonschema['$defs'][first_class].get('properties', {})
        print(f"  Properties: {len(props)}")
        if props:
            first_prop = list(props.keys())[0]
            print(f"\n  Sample property '{first_prop}':")
            print(f"    {json.dumps(props[first_prop], indent=6)}")
    
    # Save it
    output_path = "/home/javier.millanacosta/rdfsolve/output-new/aopwikirdf/aopwikirdf_schema.json"
    with open(output_path, "w") as f:
        json.dump(jsonschema, f, indent=2)
    print(f"\nSaved to {output_path}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Test Pydantic models
print("\n\n=== Testing Pydantic Models ===")
try:
    models = schema.to_pydantic_models()
    print(f"Generated {len(models)} Pydantic models:")
    for name in list(models.keys())[:5]:
        model = models[name]
        fields = model.model_fields
        print(f"  - {name}: {len(fields)} fields")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Test Pydantic file generation
print("\n\n=== Testing Pydantic .py File Generation ===")
try:
    output_py = "/home/javier.millanacosta/rdfsolve/output-new/aopwikirdf/aopwikirdf_models.py"
    schema.to_pydantic_file(output_py)
    print(f"Saved Pydantic models to {output_py}")
    # Show first few lines
    with open(output_py) as f:
        lines = f.readlines()
        print(f"\nFirst 30 lines:")
        print("".join(lines[:30]))
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("\n\nDone!")
