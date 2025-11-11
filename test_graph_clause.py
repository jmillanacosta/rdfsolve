#!/usr/bin/env python3
"""
Test script to demonstrate the new graph clause functionality in VoidParser
"""

from src.rdfsolve.void_parser import VoidParser

def test_single_graph():
    """Test with a single graph URI"""
    print("=== Testing single graph URI ===")
    
    queries = VoidParser.get_void_queries(
        graph_uris="http://example.org/graph1",
        counts=False,  # Use faster discovery queries for testing
        exclude_virtuoso_graphs=False  # Disable filtering for this test
    )
    
    print("Class partitions query:")
    print(queries['class_partitions'])
    print("\n" + "="*50 + "\n")

def test_multiple_graphs():
    """Test with multiple graph URIs"""
    print("=== Testing multiple graph URIs ===")
    
    queries = VoidParser.get_void_queries(
        graph_uris=["http://example.org/graph1", "http://example.org/graph2"],
        counts=False,
        exclude_virtuoso_graphs=False
    )
    
    print("Property partitions query:")
    print(queries['property_partitions'])
    print("\n" + "="*50 + "\n")

def test_all_graphs_with_virtuoso_filtering():
    """Test querying all graphs with Virtuoso filtering enabled"""
    print("=== Testing all graphs with Virtuoso filtering ===")
    
    queries = VoidParser.get_void_queries(
        graph_uris=None,  # Query all graphs
        counts=False,
        exclude_virtuoso_graphs=True  # Enable Virtuoso filtering
    )
    
    print("Datatype partitions query:")
    print(queries['datatype_partitions'])
    print("\n" + "="*50 + "\n")

def test_all_graphs_without_filtering():
    """Test querying all graphs without filtering"""
    print("=== Testing all graphs without filtering ===")
    
    queries = VoidParser.get_void_queries(
        graph_uris=None,
        counts=False,
        exclude_virtuoso_graphs=False
    )
    
    print("Class partitions query:")
    print(queries['class_partitions'])
    print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    test_single_graph()
    test_multiple_graphs()
    test_all_graphs_with_virtuoso_filtering()
    test_all_graphs_without_filtering()
    
    print("All tests completed!")