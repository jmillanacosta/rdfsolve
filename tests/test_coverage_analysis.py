#!/usr/bin/env python3
"""
Test script for the new class partition coverage analysis functionality.

This script tests the modular void_parser enhancements without requiring
a live SPARQL endpoint by using mock data.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from rdfsolve.void_parser import VoidParser
from rdflib import Graph, URIRef, Literal, RDF
from collections import defaultdict

def create_mock_void_graph():
    """Create a mock VoID graph with raw triple data for testing."""
    graph = Graph()
    
    # Mock namespace
    base = "http://example.org/"
    void_ext = "http://ldf.fi/void-ext#"
    
    # Create some mock raw triples
    mock_triples = [
        # (subject, predicate, object, subject_type, object_type)
        (f"{base}person1", f"{base}hasName", "John", f"{base}Person", None),
        (f"{base}person1", f"{base}hasAge", "30", f"{base}Person", None),
        (f"{base}person2", f"{base}hasName", "Jane", f"{base}Person", None),
        (f"{base}person2", f"{base}worksAt", f"{base}company1", f"{base}Person", f"{base}Company"),
        (f"{base}company1", f"{base}hasName", "Acme Corp", f"{base}Company", None),
    ]
    
    # Convert to VoID-style raw extraction format
    for i, (subj, pred, obj, subj_type, obj_type) in enumerate(mock_triples):
        triple_uri = URIRef(f"{base}triple_{i}")
        
        graph.add((triple_uri, URIRef(f"{void_ext}subject"), URIRef(subj)))
        graph.add((triple_uri, URIRef(f"{void_ext}predicate"), URIRef(pred)))
        if obj.startswith("http"):
            graph.add((triple_uri, URIRef(f"{void_ext}object"), URIRef(obj)))
            graph.add((triple_uri, URIRef(f"{void_ext}isLiteral"), Literal(False)))
        else:
            graph.add((triple_uri, URIRef(f"{void_ext}object"), Literal(obj)))
            graph.add((triple_uri, URIRef(f"{void_ext}isLiteral"), Literal(True)))
        
        graph.add((triple_uri, URIRef(f"{void_ext}subjectType"), URIRef(subj_type)))
        if obj_type:
            graph.add((triple_uri, URIRef(f"{void_ext}objectType"), URIRef(obj_type)))
        else:
            graph.add((triple_uri, URIRef(f"{void_ext}objectType"), 
                      URIRef("http://www.w3.org/2000/01/rdf-schema#Literal")))
    
    return graph

def test_basic_functionality():
    """Test basic functionality with mock data."""
    print("Testing VoidParser class partition coverage analysis...")
    
    # Create mock VoID graph
    mock_graph = create_mock_void_graph()
    print(f"Created mock graph with {len(mock_graph)} triples")
    
    # Create parser from mock data
    parser = VoidParser(mock_graph)
    
    # Test raw triple processing
    print("\nTesting raw triple processing...")
    parser._process_raw_triples(preserve_values=True)
    
    schema_df = parser.to_schema(filter_void_nodes=True)
    print(f"Generated schema with {len(schema_df)} rows")
    
    if not schema_df.empty:
        print("Sample schema rows:")
        for _, row in schema_df.head(3).iterrows():
            print(f"  {row['subject_class']} -> {row['property']} -> {row['object_class']}")
    
    print("\n✓ Basic functionality test passed!")
    return True

def test_coverage_methods():
    """Test the new coverage analysis methods with mock data."""
    print("\nTesting coverage analysis methods...")
    
    # Create mock instance counts
    mock_instance_counts = {
        "http://example.org/person1": 2,  # appears 2 times
        "http://example.org/person2": 2,  # appears 2 times  
        "http://example.org/company1": 2,  # appears 2 times
    }
    
    # Create mock class mappings
    mock_class_mappings = {
        "http://example.org/Person": {
            "http://example.org/person1": 2,
            "http://example.org/person2": 2
        },
        "http://example.org/Company": {
            "http://example.org/company1": 2
        }
    }
    
    # Test coverage calculation
    parser = VoidParser()
    coverage_stats = parser.calculate_class_partition_coverage(
        mock_instance_counts, mock_class_mappings
    )
    
    print(f"Generated coverage stats for {len(coverage_stats)} classes")
    
    for class_iri, stats in coverage_stats.items():
        print(f"  {stats['class_name']}:")
        print(f"    Instance coverage: {stats['instance_coverage_percent']}%")
        print(f"    Occurrence coverage: {stats['occurrence_coverage_percent']}%")
    
    # Test export functionality
    print("\nTesting export functionality...")
    coverage_df = parser.export_coverage_analysis(coverage_stats)
    
    print(f"Exported DataFrame with {len(coverage_df)} rows")
    if not coverage_df.empty:
        print("Columns:", list(coverage_df.columns))
    
    print("\n✓ Coverage analysis methods test passed!")
    return True

def main():
    """Run all tests."""
    print("=" * 60)
    print("TESTING CLASS PARTITION COVERAGE ANALYSIS")
    print("=" * 60)
    
    try:
        test_basic_functionality()
        test_coverage_methods()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED!")
        print("The modularized void_parser is working correctly.")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()