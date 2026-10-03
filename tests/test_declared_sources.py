from pathlib import Path
from types import SimpleNamespace

from rdflib import RDF, SH, Graph, Namespace
from rdfsolve.evidence.declared_sources import harvest_configured_declared_artifacts
from rdfsolve.models.source_model import SparqlExamples


class Helper:
    def __init__(self, graph):
        self.graph = graph
        self.queries = []

    def construct_graph(self, query):
        self.queries.append(query)
        return self.graph


def test_remote_harvest_archives_named_graph_without_turning_it_into_observed_data(tmp_path: Path):
    ex = Namespace("urn:ex:")
    graph = Graph()
    graph.add((ex.Query, RDF.type, SH.SPARQLSelectExecutable))
    graph.add((ex.Query, SH.select, __import__("rdflib").Literal("SELECT * WHERE { ?s ?p ?o }")))
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
