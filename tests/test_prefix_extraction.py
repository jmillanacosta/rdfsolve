#!/usr/bin/env python3
"""
Quick test for automatic prefix extraction functionality.
"""

import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

def test_prefix_extraction():
    """Test automatic prefix extraction from VoID files."""
    from rdfsolve import RDFSolver
    
    # Test with DrugBank VoID file
    drugbank_void = "notebooks/idsm/drugbank_drugs_void.ttl"
    
    if not Path(drugbank_void).exists():
        print(f"Test file not found: {drugbank_void}")
        return False
    
    try:
        # Create solver and load VoID
        solver = RDFSolver(
            endpoint="https://test.example.com/sparql",
            path="/tmp",
            void_iri="https://test.example.com/void",
            dataset_name="test_dataset"
        )
        
        solver.parse_void(drugbank_void)
        
        # Extract prefixes
        prefixes = solver._extract_prefixes_from_void()
        
        # Verify expected prefixes
        expected = ['rdf', 'rdfs', 'void', 'void-ext']
        missing = [p for p in expected if p not in prefixes]
        
        if missing:
            print(f"Missing expected prefixes: {missing}")
            return False
        
        print("Prefix extraction test passed!")
        print(f"   Found {len(prefixes)} prefixes: {', '.join(sorted(prefixes.keys()))}")
        
        # Test JSON-LD export without context (should use auto prefixes)
        try:
            jsonld = solver.export_schema_jsonld(indent=2)
            if '"@context"' in jsonld:
                print("JSON-LD export with auto-context successful!")
                return True
            else:
                print("JSON-LD missing @context section")
                return False
                
        except Exception as e:
            print(f"JSON-LD export failed: {e}")
            return False
            
    except Exception as e:
        print(f"Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_prefix_extraction()
    sys.exit(0 if success else 1)