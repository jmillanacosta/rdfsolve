"""Pytest fixtures for acceptance tests."""

import pytest
from rdflib import Graph, Literal, RDF, RDFS, URIRef

from rdfsolve import MinedSchema, SchemaPattern
from rdfsolve.client_api import Client
from rdfsolve.schema_models.enrichment import RdfTerm, TermAnnotation

from tests.acceptance.fixtures import EX
from tests.acceptance.fixtures import f01_unrestricted
from tests.acceptance.fixtures import f02_ambiguous
from tests.acceptance.fixtures import f03_conjunction


def _build_client(fixture_module):
    """Build a Client from a fixture module."""
    g = fixture_module.create_graph()
    fixture_module.add_labels(g)

    metadata = fixture_module.SCHEMA_METADATA

    # Build patterns from metadata
    patterns = []
    for p in metadata.get("patterns", []):
        patterns.append(
            SchemaPattern(
                subject_class=p["subject"],
                property_uri=p["predicate"],
                object_class=p["object"],
            )
        )

    # Build schema
    schema = MinedSchema(
        about={"dataset_name": "fixture"},
        patterns=patterns,
    )

    # Add class labels to enrichment
    for cls in metadata.get("classes", []):
        schema.enrichment.labels.append(
            TermAnnotation(
                term_iri=cls["iri"],
                predicate=str(RDFS.label),
                text=RdfTerm(kind="literal", value=cls["label"]),
            )
        )
        if cls.get("description"):
            schema.enrichment.definitions.append(
                TermAnnotation(
                    term_iri=cls["iri"],
                    predicate=str(RDFS.comment),
                    text=RdfTerm(kind="literal", value=cls["description"]),
                )
            )

    # Add predicate labels to enrichment
    for pred in metadata.get("predicates", []):
        schema.enrichment.labels.append(
            TermAnnotation(
                term_iri=pred["iri"],
                predicate=str(RDFS.label),
                text=RdfTerm(kind="literal", value=pred["label"]),
            )
        )
        if pred.get("description"):
            schema.enrichment.definitions.append(
                TermAnnotation(
                    term_iri=pred["iri"],
                    predicate=str(RDFS.comment),
                    text=RdfTerm(kind="literal", value=pred["description"]),
                )
            )

    return Client(schema, g, graph_uris=[])


@pytest.fixture
def f01_client():
    """Create a Client with F01 fixture data."""
    return _build_client(f01_unrestricted)


@pytest.fixture
def f02_client():
    """Create a Client with F02 fixture data."""
    return _build_client(f02_ambiguous)


@pytest.fixture
def f03_client():
    """Create a Client with F03 fixture data."""
    return _build_client(f03_conjunction)


@pytest.fixture
def f01_graph():
    """Create the F01 fixture graph with labels."""
    g = f01_unrestricted.create_graph()
    f01_unrestricted.add_labels(g)
    return g


@pytest.fixture
def f02_graph():
    """Create the F02 fixture graph with labels."""
    g = f02_ambiguous.create_graph()
    f02_ambiguous.add_labels(g)
    return g


@pytest.fixture
def f03_graph():
    """Create the F03 fixture graph with labels."""
    g = f03_conjunction.create_graph()
    f03_conjunction.add_labels(g)
    return g


class BackendSpy:
    """Spy to track backend calls."""

    def __init__(self, graph: Graph):
        self.graph = graph
        self.calls: list[str] = []
        self._original_query = graph.query

    def wrap(self) -> None:
        """Install the spy."""
        def spy_query(query_text, *args, **kwargs):
            self.calls.append(str(query_text))
            return self._original_query(query_text, *args, **kwargs)
        self.graph.query = spy_query

    def unwrap(self) -> None:
        """Remove the spy."""
        self.graph.query = self._original_query


@pytest.fixture
def backend_spy(f01_graph):
    """Create a spy for tracking backend calls."""
    spy = BackendSpy(f01_graph)
    spy.wrap()
    yield spy
    spy.unwrap()
