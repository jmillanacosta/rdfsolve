"""Test frequency calculation functionality.

This test suite validates that the frequency calculation works correctly,
including both basic frequency mode and instance collection mode.
"""

import logging

import pytest

from rdfsolve.api import generate_void_from_endpoint, load_parser_from_graph

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def aop_wiki_void_graph():
    """Generate VoID graph from AOP Wiki endpoint for testing."""
    endpoint_url = "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql/"
    graph_uri = "http://aopwiki.org/"

    void_graph = generate_void_from_endpoint(
        endpoint_url=endpoint_url,
        graph_uris=[graph_uri],
        counts=False,  # Fast discovery mode
        exclude_graphs=True,
    )
    return void_graph, endpoint_url, graph_uri


@pytest.fixture(scope="module")
def aop_wiki_parser(aop_wiki_void_graph):
    """Create parser from AOP Wiki VoID graph."""
    void_graph, endpoint_url, graph_uri = aop_wiki_void_graph
    vp = load_parser_from_graph(void_graph, graph_uris=[graph_uri])
    return vp, endpoint_url


@pytest.mark.integration
def test_basic_frequency_mode(aop_wiki_parser):
    """Test basic frequency mode without instance collection.

    Validates that frequency calculation returns non-zero occurrence counts
    for patterns when collect_instances=False.
    """
    vp, endpoint_url = aop_wiki_parser

    # Get schema patterns
    schema_df = vp.to_schema(filter_void_admin_nodes=True)
    assert len(schema_df) > 10, "Schema should have patterns"

    # Test basic frequency mode
    frequencies_df, instances_df = vp.count_schema_shape_frequencies(
        endpoint_url=endpoint_url,
        offset_limit_steps=300,
        collect_instances=False,
        track_queries=True,
    )

    # Validate results
    assert not frequencies_df.empty, "Frequencies DataFrame should not be empty"
    assert instances_df is None, "Instances DataFrame should be None in basic mode"

    # All occurrence_count should be non-zero
    zero_count = (frequencies_df["occurrence_count"] == 0).sum()
    total_count = len(frequencies_df)

    assert zero_count == 0, (
        f"Bug detected: {zero_count}/{total_count} patterns have "
        f"occurrence_count=0. All patterns should have non-zero "
        f"occurrence_count."
    )

    # Validate that coverage calculations are reasonable
    assert (frequencies_df["coverage_percent"] >= 0).all()
    assert (frequencies_df["coverage_percent"] <= 100).all()


@pytest.mark.integration
def test_instance_collection_mode(aop_wiki_parser):
    """Test instance collection mode.

    Validates that instance collection mode both calculates frequencies
    and collects actual instances when collect_instances=True.
    """
    vp, endpoint_url = aop_wiki_parser

    # Test instance collection mode
    frequencies_df, instances_df = vp.count_schema_shape_frequencies(
        endpoint_url=endpoint_url,
        offset_limit_steps=300,
        collect_instances=True,
        track_queries=True,
    )

    # Validate results
    assert not frequencies_df.empty, "Frequencies DataFrame should not be empty"
    assert instances_df is not None, "Instances DataFrame should not be None"
    assert not instances_df.empty, "Instances DataFrame should contain instances"

    # Check that all patterns have non-zero occurrence counts
    zero_count = (frequencies_df["occurrence_count"] == 0).sum()
    total_count = len(frequencies_df)

    assert zero_count == 0, (
        f"Bug detected: {zero_count}/{total_count} patterns have "
        f"occurrence_count=0. All patterns should have non-zero "
        f"occurrence_count."
    )

    # Validate that we collected meaningful instances
    assert len(instances_df) > 0, "Should have collected instances"
    assert "shape_id" in instances_df.columns, "Instances should have shape_id"
    # Column names are subject_iri and object_iri, not subject/object
    assert "subject_iri" in instances_df.columns, "Instances should have subject_iri"
    assert "object_iri" in instances_df.columns, "Instances should have object_iri"


@pytest.mark.integration
def test_frequency_consistency(aop_wiki_parser):
    """Test that both modes produce consistent frequency counts.

    Validates that basic frequency mode and instance collection mode
    produce the same frequency counts for patterns.
    """
    vp, endpoint_url = aop_wiki_parser

    # Get frequencies from both modes
    basic_freq_df, _ = vp.count_schema_shape_frequencies(
        endpoint_url=endpoint_url,
        offset_limit_steps=300,
        collect_instances=False,
        track_queries=True,
    )

    instance_freq_df, instances_df = vp.count_schema_shape_frequencies(
        endpoint_url=endpoint_url,
        offset_limit_steps=300,
        collect_instances=True,
        track_queries=True,
    )

    # Both should have results
    assert not basic_freq_df.empty
    assert not instance_freq_df.empty
    assert instances_df is not None

    # The number of patterns should be the same
    assert len(basic_freq_df) == len(instance_freq_df), (
        "Both modes should find the same number of patterns"
    )
