#!/usr/bin/env python3
"""Test the raw extraction functionality with sample data."""

import sys

sys.path.append('..')

from src.rdfsolve.void_parser import VoidParser


def test_raw_extraction():
    """Test raw extraction functionality with sample data."""
    
    # Sample raw triples data (what would come from SPARQL)
    raw_triples = [
        {
            'subject': 'http://example.com/person1',
            'property': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
            'object': 'http://xmlns.com/foaf/0.1/Person',
            'subject_type': 'Resource',
            'object_type': 'Resource'
        },
        {
            'subject': 'http://example.com/person1',
            'property': 'http://xmlns.com/foaf/0.1/name',
            'object': 'John Doe',
            'subject_type': 'Resource',
            'object_type': 'Literal'
        },
        {
            'subject': 'http://example.com/person1',
            'property': 'http://xmlns.com/foaf/0.1/knows',
            'object': 'http://example.com/person2',
            'subject_type': 'Resource',
            'object_type': 'Resource'
        },
        {
            'subject': 'http://example.com/person2',
            'property': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
            'object': 'http://xmlns.com/foaf/0.1/Person',
            'subject_type': 'Resource',
            'object_type': 'Resource'
        },
        {
            'subject': 'http://example.com/person2',
            'property': 'http://xmlns.com/foaf/0.1/name',
            'object': 'Jane Smith',
            'subject_type': 'Resource',
            'object_type': 'Literal'
        }
    ]
    
    print("Raw Extraction Test")
    print("=" * 50)
    
    print("\n1. Creating parser from raw triples (preserve_values=True)...")
    parser_preserve = VoidParser.from_raw_triples(
        raw_triples,
        preserve_values=True
    )
    
    schema_preserve = parser_preserve.to_schema()
    print(f"   Schema with preserved values: {len(schema_preserve)} entries")
    cols = list(schema_preserve.columns) if len(schema_preserve) > 0 else 'None'
    print(f"   Schema columns: {cols}")
    if len(schema_preserve) > 0:
        print("   Sample entries:")
        for i, row in schema_preserve.head(3).iterrows():
            s = row['subject_class']
            p = row['property']
            o = row['object_class']
            print(f"     {s} -> {p} -> {o}")
    else:
        print("   No entries found - debugging needed")
    
    print("\n2. Creating parser from raw triples (preserve_values=False)...")
    parser_classify = VoidParser.from_raw_triples(
        raw_triples,
        preserve_values=False
    )
    
    schema_classify = parser_classify.to_schema()
    print(f"   Schema with classified values: {len(schema_classify)} entries")
    print("   Sample entries:")
    for i, row in schema_classify.head(3).iterrows():
        s = row['subject_class']
        p = row['property']
        o = row['object_class']
        print(f"     {s} -> {p} -> {o}")
    
    print("\n3. Comparison:")
    print("   Preserve mode shows actual values:")
    preserve_objects = set(schema_preserve['object_class'].unique())
    print(f"     Objects: {preserve_objects}")
    
    print("   Classify mode shows Resource/Literal:")
    classify_objects = set(schema_classify['object_class'].unique())
    print(f"     Objects: {classify_objects}")
    
    print("\n4. Export to JSON...")
    json_preserve = parser_preserve.to_json()
    json_classify = parser_classify.to_json()
    
    print(f"   Preserved JSON keys: {list(json_preserve.keys())}")
    print(f"   Classified JSON keys: {list(json_classify.keys())}")
    
    print("\n✓ Raw extraction test completed successfully!")


if __name__ == "__main__":
    test_raw_extraction()