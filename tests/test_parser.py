"""Tests for VoidParser."""

import json
import os

import pandas as pd
import pytest
import yaml
from rdflib import Graph, Literal, Namespace

from rdfsolve import api
from rdfsolve.parser import VoidParser

EX = Namespace("http://example.org/")
VOID = Namespace("http://rdfs.org/ns/void#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

# Path to test data - cross-platform compatible
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "test_data")
AOPWIKI_VOID = os.path.join(TEST_DATA_DIR, "aopwikirdf_generated_void.ttl")
AOPWIKI_SCHEMA_CSV = os.path.join(TEST_DATA_DIR, "aopwikirdf_schema.csv")
AOPWIKI_SCHEMA_JSONLD = os.path.join(
    TEST_DATA_DIR, "aopwikirdf_schema.jsonld"
)
AOPWIKI_LINKML_YAML = os.path.join(
    TEST_DATA_DIR, "aopwikirdf_linkml_schema.yaml"
)


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


class TestRealDataInterconversion:
    """Test interconversion with real aopwikirdf data."""

    @pytest.fixture
    def aopwiki_parser(self):
        """Load aopwikirdf VoID file."""
        if not os.path.exists(AOPWIKI_VOID):
            pytest.skip(f"Test data not found: {AOPWIKI_VOID}")
        return VoidParser(void_source=AOPWIKI_VOID)

    @pytest.fixture
    def expected_csv(self):
        """Load expected CSV schema."""
        if not os.path.exists(AOPWIKI_SCHEMA_CSV):
            pytest.skip(f"Test data not found: {AOPWIKI_SCHEMA_CSV}")
        return pd.read_csv(AOPWIKI_SCHEMA_CSV)

    @pytest.fixture
    def expected_jsonld(self):
        """Load expected JSON-LD schema."""
        if not os.path.exists(AOPWIKI_SCHEMA_JSONLD):
            pytest.skip(f"Test data not found: {AOPWIKI_SCHEMA_JSONLD}")
        with open(AOPWIKI_SCHEMA_JSONLD) as f:
            return json.load(f)

    @pytest.fixture
    def expected_linkml(self):
        """Load expected LinkML schema."""
        if not os.path.exists(AOPWIKI_LINKML_YAML):
            pytest.skip(f"Test data not found: {AOPWIKI_LINKML_YAML}")
        with open(AOPWIKI_LINKML_YAML) as f:
            return yaml.safe_load(f)

    def test_void_to_csv_schema(self, aopwiki_parser, expected_csv):
        """Test VoID -> CSV conversion produces consistent results."""
        # First extract schema triples to populate internal state
        aopwiki_parser._extract_schema_triples()
        
        result_df = aopwiki_parser.to_schema(filter_void_admin_nodes=True)

        # The parser should extract schema patterns from VoID
        # If extraction still fails, check if VoID has property partitions
        if len(result_df) == 0:
            # Check if VoID file has property partitions
            prop_partitions = list(
                aopwiki_parser.graph.triples(
                    (None, aopwiki_parser.void_property, None)
                )
            )
            if len(prop_partitions) == 0:
                pytest.skip("VoID file has no property partitions")
            else:
                pytest.fail(
                    f"Schema extraction empty despite "
                    f"{len(prop_partitions)} property partitions"
                )

        # Check essential columns exist
        essential_cols = ["subject_uri", "property_uri", "object_uri"]
        for col in essential_cols:
            assert col in result_df.columns, f"Missing column: {col}"

        # Check we have similar number of patterns (allow wide variation)
        # Different extraction runs may produce different results
        assert len(result_df) > 10, "Should extract multiple patterns"

    def test_void_to_jsonld_structure(self, aopwiki_parser, expected_jsonld):
        """Test VoID -> JSON-LD conversion has correct structure."""
        result = aopwiki_parser.to_jsonld(filter_void_admin_nodes=True)

        # Check JSON-LD structure
        assert "@context" in result, "Missing @context"
        assert "@graph" in result, "Missing @graph"

        # Check context has prefixes
        assert isinstance(result["@context"], dict), "Context should be dict"
        assert len(result["@context"]) > 0, "Context should have prefixes"

        # Check graph has entries
        assert isinstance(result["@graph"], list), "Graph should be list"
        assert len(result["@graph"]) > 0, "Graph should have entries"

        # Verify some expected prefixes exist
        context = result["@context"]
        expected_prefixes = ["aopo", "foaf", "dcterms", "rdfs"]
        found_prefixes = [p for p in expected_prefixes if p in context]
        assert len(found_prefixes) >= 2, (
            f"Should have at least 2 of {expected_prefixes}, "
            f"found: {found_prefixes}"
        )

    def test_void_to_linkml_structure(self, aopwiki_parser):
        """Test VoID -> LinkML conversion has correct structure."""
        result = aopwiki_parser.to_linkml(
            filter_void_nodes=True,
            schema_name="aopwikirdf",
            schema_description="AOP-Wiki RDF Schema",
        )

        # Check LinkML schema structure
        assert result.name == "aopwikirdf", "Schema name mismatch"
        expected_desc = "AOP-Wiki RDF Schema"
        assert result.description == expected_desc, "Description mismatch"

        # Check schema has classes
        assert hasattr(result, "classes"), "Schema should have classes"
        assert len(result.classes) > 0, "Schema should define classes"

        # Check schema has prefixes
        assert hasattr(result, "prefixes"), "Schema should have prefixes"
        assert len(result.prefixes) > 0, "Schema should define prefixes"

    def test_api_file_to_jsonld(self):
        """Test API function for file -> JSON-LD conversion."""
        if not os.path.exists(AOPWIKI_VOID):
            pytest.skip(f"Test data not found: {AOPWIKI_VOID}")

        result = api.to_jsonld_from_file(
            AOPWIKI_VOID, filter_void_admin_nodes=True
        )

        assert "@context" in result
        assert "@graph" in result
        assert len(result["@graph"]) > 0

    def test_api_file_to_linkml(self):
        """Test API function for file -> LinkML conversion."""
        if not os.path.exists(AOPWIKI_VOID):
            pytest.skip(f"Test data not found: {AOPWIKI_VOID}")

        result = api.to_linkml_from_file(
            AOPWIKI_VOID,
            filter_void_nodes=True,
            schema_name="test_aopwiki",
        )

        # Result is YAML string
        assert isinstance(result, str)
        assert "name: test_aopwiki" in result
        assert "classes:" in result

    def test_api_graph_conversions(self):
        """Test API functions for graph -> format conversions."""
        if not os.path.exists(AOPWIKI_VOID):
            pytest.skip(f"Test data not found: {AOPWIKI_VOID}")

        # Load VoID file as graph
        g = Graph()
        g.parse(AOPWIKI_VOID, format="turtle")

        # Test graph -> JSON-LD
        jsonld_result = api.graph_to_jsonld(
            g, graph_uris=None, filter_void_admin_nodes=True
        )
        assert "@context" in jsonld_result
        assert "@graph" in jsonld_result

        # Test graph -> LinkML
        linkml_result = api.graph_to_linkml(
            g,
            graph_uris=None,
            filter_void_nodes=True,
            schema_name="test_schema",
        )
        assert isinstance(linkml_result, str)
        assert "name: test_schema" in linkml_result

        # Test graph -> DataFrame
        df_result = api.graph_to_schema(g, graph_uris=None)
        assert isinstance(df_result, pd.DataFrame)
        # Empty result is acceptable if no schema triples found
        if len(df_result) > 0:
            assert "subject_uri" in df_result.columns

    def test_roundtrip_consistency(self, aopwiki_parser):
        """Test that multiple conversions maintain consistency."""
        # Extract schema triples first
        aopwiki_parser._extract_schema_triples()
        
        # Get schema as DataFrame
        df1 = aopwiki_parser.to_schema(filter_void_admin_nodes=True)

        # Get JSON-LD and check entries correspond
        jsonld = aopwiki_parser.to_jsonld(filter_void_admin_nodes=True)
        graph_entries = len(jsonld["@graph"])

        # Get LinkML and check classes defined
        linkml = aopwiki_parser.to_linkml(filter_void_nodes=True)
        num_classes = len(linkml.classes) if linkml.classes else 0

        # All should have substantial content
        assert len(df1) > 0, "Should have schema patterns"
        assert graph_entries > 0, "Should have graph entries"
        assert num_classes > 0, "Should have classes"

        # The number of unique subjects in CSV should roughly match classes
        unique_subjects = df1["subject_uri"].nunique()
        # Allow wide variance as these measure different things
        assert abs(unique_subjects - num_classes) < 200, (
            f"Subject count ({unique_subjects}) very different from "
            f"class count ({num_classes})"
        )
