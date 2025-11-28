"""Tests for VoidParser."""

import pandas as pd
import pytest
from rdflib import Graph, Literal, Namespace

from rdfsolve.parser import VoidParser

EX = Namespace("http://example.org/")
VOID = Namespace("http://rdfs.org/ns/void#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")


@pytest.fixture
def sample_void_graph():
    """Create sample RDF graph with VoID metadata."""
    g = Graph()
    dataset = EX.myDataset
    g.add((dataset, RDF.type, VOID.Dataset))
    class_partition = EX.personPartition
    g.add((dataset, VOID.classPartition, class_partition))
    g.add((class_partition, VOID["class"], EX.Person))
    g.add((class_partition, VOID.entities, Literal(100)))
    return g


@pytest.fixture
def parser_with_graph(sample_void_graph):
    """Create VoidParser with sample graph."""
    return VoidParser(void_source=sample_void_graph)


@pytest.fixture
def empty_parser():
    """Create empty VoidParser."""
    return VoidParser()


class TestInitialization:
    """Test VoidParser initialization."""

    def test_init_no_args(self):
        """Test initialization with no arguments."""
        parser = VoidParser()
        if parser.graph is None:
            pytest.fail("Expected graph initialized")

    def test_init_with_graph(self, sample_void_graph):
        """Test initialization with graph."""
        parser = VoidParser(void_source=sample_void_graph)
        if parser.graph != sample_void_graph:
            pytest.fail("Graph not set correctly")

    def test_init_with_string_uri(self):
        """Test initialization with single URI."""
        parser = VoidParser(graph_uris="http://example.org/graph1")
        expected = ["http://example.org/graph1"]
        if parser.graph_uris != expected:
            pytest.fail(f"Expected {expected}, got {parser.graph_uris}")


class TestSchemaExtraction:
    """Test schema extraction."""

    def test_to_schema(self, parser_with_graph):
        """Test to_schema method."""
        df = parser_with_graph.to_schema()
        if not isinstance(df, pd.DataFrame):
            pytest.fail(f"Expected DataFrame, got {type(df)}")


class TestJSONLD:
    """Test JSON-LD generation."""

    def test_to_jsonld(self, parser_with_graph):
        """Test JSON-LD generation."""
        jsonld = parser_with_graph.to_jsonld()
        if "@context" not in jsonld:
            pytest.fail("Missing '@context'")


class TestLinkML:
    """Test LinkML generation."""

    def test_to_linkml(self, parser_with_graph):
        """Test LinkML generation."""
        schema = parser_with_graph.to_linkml()
        if schema is None:
            pytest.fail("Expected LinkML schema")


class TestSPARQLQueries:
    """Test SPARQL query generation."""

    def test_get_queries(self):
        """Test query generation."""
        queries = VoidParser.get_void_queries()
        if not isinstance(queries, dict):
            pytest.fail(f"Expected dict, got {type(queries)}")
        if "class_partitions" not in queries:
            pytest.fail("Missing 'class_partitions'")
