#!/usr/bin/env python3
"""
Test the debug progress monitoring in VoidParser.
"""

import sys
import time

sys.path.append('..')

from src.rdfsolve.void_parser import VoidParser


def test_progress_monitoring():
    """Test progress monitoring with simulated data."""
    
    print("Testing Progress Monitoring")
    print("=" * 50)
    
    # Create a large dataset to test progress monitoring
    print("\n1. Testing post-processing progress monitoring...")
    
    # Generate sample data that should trigger progress updates
    raw_triples = []
    for i in range(15000):  # Large enough to trigger progress monitoring
        raw_triples.append({
            'subject': f'http://example.com/subject{i % 100}',
            'property': f'http://example.com/property{i % 10}',
            'object': (f'http://example.com/object{i}'
                       if i % 2 == 0 else f'Literal{i}'),
            'subject_type': 'Resource',
            'object_type': 'Resource' if i % 2 == 0 else 'Literal'
        })
    
    print(f"Generated {len(raw_triples):,} test triples")
    
    # Test with preserve_values=True
    print("\nTesting preserve_values=True...")
    start_time = time.monotonic()
    
    parser_preserve = VoidParser.from_raw_triples(
        raw_triples,
        preserve_values=True
    )
    
    process_time = time.monotonic() - start_time
    print(f"Total processing took {process_time:.2f}s")
    
    # Get schema summary
    schema = parser_preserve.to_schema()
    print(f"Result: {len(schema):,} schema entries")
    
    print("\n2. Testing SPARQL progress monitoring (simulated)...")
    print("Note: This would show progress during actual SPARQL queries")
    print("The following emojis will appear during real SPARQL operations:")
    print("🔄 Starting query")
    print("⏳ Query still running... (every minute)")
    print("✅ Finished query")
    print("📊 Parsing results")
    print("⚠️  Empty/No results")
    print("❌ Query failed")
    print("⏰ Query timed out")
    print("⏭️  Skipping optional query")
    print("🚀 Starting VoID extraction")
    print("📡 Endpoint info")
    print("🎯 Graph info")
    print("⚡ Raw extraction mode")
    print("🔧 Traditional mode")
    print("💾 Saved to file")
    print("🎉 Extraction completed")
    print("📊 Final statistics")
    
    print("\n✅ Progress monitoring test completed!")


if __name__ == "__main__":
    test_progress_monitoring()