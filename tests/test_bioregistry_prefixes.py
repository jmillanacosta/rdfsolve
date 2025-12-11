"""Test bioregistry prefix enrichment in VoID graphs.

This test suite validates that the bioregistry integration correctly
resolves and binds prefixes to VoID graphs.

Note: Tests access _enrich_void_with_bioregistry_prefixes which is a
protected method but we're explicitly testing its functionality.
"""

import pytest
from rdflib import Graph, Namespace, URIRef

from rdfsolve.parser import VoidParser


@pytest.fixture
def sample_void_graph():
    """Create a sample VoID graph with URIs from various namespaces."""
    test_graph = Graph()

    void_ns = Namespace("http://rdfs.org/ns/void#")
    test_graph.bind("void", void_ns)

    # Add test triples with URIs from different namespaces
    # ChEBI (should be recognized by bioregistry)
    test_graph.add(
        (
            URIRef("http://example.org/partition1"),
            void_ns.class_,
            URIRef("http://purl.obolibrary.org/obo/CHEBI_24867"),
        )
    )

    # GO (should be recognized)
    test_graph.add(
        (
            URIRef("http://example.org/partition2"),
            void_ns.class_,
            URIRef("http://purl.obolibrary.org/obo/GO_0008150"),
        )
    )

    # UniProt (should be recognized)
    test_graph.add(
        (
            URIRef("http://example.org/partition3"),
            void_ns.property,
            URIRef("http://purl.uniprot.org/core/reviewed"),
        )
    )

    # WikiPathways (should be recognized)
    test_graph.add(
        (
            URIRef("http://example.org/partition4"),
            void_ns.class_,
            URIRef("http://identifiers.org/wikipathways/WP4263"),
        )
    )

    return test_graph


def test_bioregistry_prefix_enrichment(sample_void_graph):
    """Test that bioregistry enriches VoID graph with proper prefixes."""
    initial_namespaces = {prefix for prefix, _ in sample_void_graph.namespaces()}

    # Enrich with bioregistry prefixes
    # pylint: disable=protected-access
    enriched_graph = VoidParser._enrich_void_with_bioregistry_prefixes(sample_void_graph)

    # Get enriched namespaces
    enriched_namespaces = {prefix for prefix, _ in enriched_graph.namespaces()}

    # Should have more namespaces after enrichment
    assert len(enriched_namespaces) > len(initial_namespaces), (
        "Enrichment should add new namespace bindings"
    )

    # Check for specific expected prefixes
    expected_prefixes = {"chebi", "go", "uniprot", "wikipathways"}
    found_prefixes = enriched_namespaces & expected_prefixes

    assert len(found_prefixes) > 0, (
        f"Should find at least some bioregistry prefixes. Found: {found_prefixes}"
    )

    # Validate that graph content is unchanged
    assert len(enriched_graph) == len(sample_void_graph), (
        "Enrichment should not change triple count"
    )


def test_bioregistry_with_unknown_uris():
    """Test that enrichment handles URIs not in bioregistry gracefully."""
    test_graph = Graph()

    void_ns = Namespace("http://rdfs.org/ns/void#")
    test_graph.bind("void", void_ns)

    # Add URIs that bioregistry might not recognize
    test_graph.add(
        (
            URIRef("http://example.org/partition1"),
            void_ns.class_,
            URIRef("http://some-unknown-namespace.org/SomeClass"),
        )
    )

    test_graph.add(
        (
            URIRef("http://example.org/partition2"),
            void_ns.property,
            URIRef("http://another-unknown.com/prop123"),
        )
    )

    # Should not raise an exception
    # pylint: disable=protected-access
    enriched_graph = VoidParser._enrich_void_with_bioregistry_prefixes(test_graph)

    # Graph should still be valid
    assert len(enriched_graph) == len(test_graph)
    assert len(enriched_graph) > 0


def test_bioregistry_preserves_existing_bindings():
    """Test that enrichment preserves manually-bound prefixes."""
    test_graph = Graph()

    # Add custom bindings
    test_graph.bind("custom", "http://example.org/custom#")
    test_graph.bind("void", "http://rdfs.org/ns/void#")

    void_ns = Namespace("http://rdfs.org/ns/void#")

    # Add some triples
    test_graph.add(
        (
            URIRef("http://example.org/custom#Thing1"),
            void_ns.class_,
            URIRef("http://purl.obolibrary.org/obo/CHEBI_24867"),
        )
    )

    # Enrich
    # pylint: disable=protected-access
    enriched_graph = VoidParser._enrich_void_with_bioregistry_prefixes(test_graph)

    # Check that custom binding is preserved
    namespaces_dict = dict(enriched_graph.namespaces())
    assert "custom" in namespaces_dict, "Custom prefix should be preserved"
    assert "void" in namespaces_dict, "Void prefix should be preserved"


def test_void_generation_includes_void_ext_prefix():
    """Test that void-ext prefix is bound in generated VoID graphs."""
    # This is a unit test that just checks the prefix is there
    # We'll use build_void_graph_from_partitions as it's simpler to test

    partitions = [
        {
            "subjectClass": "http://example.org/ClassA",
            "prop": "http://example.org/property1",
            "objectClass": "http://example.org/ClassB",
        }
    ]

    void_graph = VoidParser().build_void_graph_from_partitions(
        partitions, base_uri="http://example.org/void"
    )

    # Check that void-ext is bound
    namespaces_dict = dict(void_graph.namespaces())
    assert "void-ext" in namespaces_dict, "void-ext prefix should be bound"
    assert str(namespaces_dict["void-ext"]) == "http://ldf.fi/void-ext#", (
        "void-ext should have correct namespace URI"
    )

    # Also check void and rdf are bound
    assert "void" in namespaces_dict, "void prefix should be bound"
    assert "rdf" in namespaces_dict, "rdf prefix should be bound"


@pytest.mark.integration
def test_bioregistry_with_real_endpoint():
    """Test bioregistry enrichment with actual VoID generation.

    This is an integration test that generates a real VoID graph
    and validates bioregistry enrichment works end-to-end.
    """
    from rdfsolve.api import generate_void_from_endpoint

    # Generate VoID from a small endpoint
    void_graph = generate_void_from_endpoint(
        endpoint_url="https://aopwiki.rdf.bigcat-bioinformatics.org/sparql/",
        counts=False,
        offset_limit_steps=300,
    )

    # Should have void-ext namespace URI bound (regardless of prefix name)
    namespaces_dict = dict(void_graph.namespaces())
    namespace_uris = {str(uri) for uri in namespaces_dict.values()}
    assert "http://ldf.fi/void-ext#" in namespace_uris, "void-ext namespace URI should be bound"

    # Should have some bioregistry-resolved prefixes (not just default RDFLib ns*)
    # Check that we have prefixes that are NOT auto-generated ns0, ns1, etc.
    non_default_prefixes = {
        p for p in namespaces_dict.keys() if not (p.startswith("ns") and p[2:].isdigit())
    }
    assert len(non_default_prefixes) > 3, (
        f"Should have meaningful prefixes beyond just void, rdf, rdfs. "
        f"Found: {sorted(non_default_prefixes)}"
    )
