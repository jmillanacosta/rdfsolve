from pathlib import Path
from types import SimpleNamespace

from rdflib import Graph, Namespace, RDF, SH

from rdfsolve.evidence.declared_sources import empirical_graph_scope, harvest_configured_declared_artifacts
from rdfsolve.models.source_model import SparqlExamples


class Helper:
    def __init__(self, graph):
        self.graph = graph
        self.queries = []

    def construct_graph(self, query):
        self.queries.append(query)
        return self.graph


def test_empirical_graph_scope_excludes_explicit_example_graphs():
    source = SimpleNamespace(
        graph_uris=["urn:data", "urn:examples"],
        sparql_examples=SparqlExamples(shacl_graph_in_endpoint=["urn:examples"]),
    )
    assert empirical_graph_scope(source) == ["urn:data"]


def test_remote_harvest_archives_named_graph_without_turning_it_into_observed_data(tmp_path: Path):
    ex = Namespace("urn:ex:")
    graph = Graph()
    graph.add((ex.Query, RDF.type, SH.SPARQLSelectExecutable))
    graph.add((ex.Query, SH.select, __import__('rdflib').Literal("SELECT * WHERE { ?s ?p ?o }")))
    source = SimpleNamespace(
        endpoint="https://example.org/sparql",
        graph_uris=["urn:data", "urn:examples"],
        sparql_examples=SparqlExamples(shacl_graph_in_endpoint=["urn:examples"]),
    )
    bundle = harvest_configured_declared_artifacts(
        source=source,
        dataset_id="demo",
        output_dir=tmp_path,
        access_context="remote_endpoint",
        helper=Helper(graph),
    )
    assert len(bundle.artifacts) == 1
    assert bundle.artifacts[0].source_graph == "urn:examples"
    assert bundle.artifacts[0].kind == "sparql_examples"
    assert bundle.artifacts[0].representation == "constructed_graph"


def test_local_run_does_not_query_remote_endpoint_graph(tmp_path: Path):
    graph = Graph()
    helper = Helper(graph)
    source = SimpleNamespace(
        endpoint="https://example.org/sparql",
        graph_uris=["urn:data", "urn:examples"],
        sparql_examples=SparqlExamples(shacl_graph_in_endpoint=["urn:examples"]),
    )
    bundle = harvest_configured_declared_artifacts(
        source=source,
        dataset_id="demo",
        output_dir=tmp_path,
        access_context="local_distribution",
        helper=helper,
    )
    assert bundle.artifacts == []
    assert helper.queries == []
