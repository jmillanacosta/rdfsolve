from __future__ import annotations

import json

from rdflib import OWL, RDF, Dataset, Literal, URIRef
from rdfsolve.mining.ontology_discovery import discover_remote_ontology_graphs


class DatasetHelper:
    def __init__(self, data: Dataset):
        self.data = data
        self.endpoint_url = "https://example.org/sparql"

    def ask(self, query: str) -> bool:
        return bool(self.data.query(query).askAnswer)

    def select(self, query: str, purpose: str = "") -> dict:
        return json.loads(self.data.query(query).serialize(format="json"))

    def prepare_paginated_query(self, query: str) -> str:
        return query

    def select_chunked(self, query: str, **kwargs):
        result = self.select(query)
        yield result["results"]["bindings"]


def _dataset() -> Dataset:
    data = Dataset()
    ontology = data.graph(URIRef("https://example.org/graph/ontology.owl"))
    ontology.add((URIRef("https://example.org/onto"), RDF.type, OWL.Ontology))
    ontology.add((URIRef("https://example.org/onto"), OWL.versionInfo, Literal("2026.09")))
    ontology.add(
        (URIRef("https://example.org/onto"), OWL.imports, URIRef("https://example.org/base"))
    )
    ontology.add((URIRef("https://example.org/C"), RDF.type, OWL.Class))
    ontology.add((URIRef("https://example.org/p"), RDF.type, OWL.ObjectProperty))
    data_graph = data.graph(URIRef("https://example.org/graph/data"))
    data_graph.add((URIRef("urn:s"), RDF.type, URIRef("https://example.org/C")))
    data_graph.add((URIRef("urn:s"), URIRef("https://example.org/p"), URIRef("urn:o")))
    metadata = data.graph(URIRef("https://example.org/graph/metadata"))
    metadata.add(
        (URIRef("urn:dataset"), URIRef("http://purl.org/pav/version"), Literal("dataset-v1"))
    )
    return data


def test_remote_discovery_keeps_version_and_empirical_overlap():
    helper = DatasetHelper(_dataset())
    result = discover_remote_ontology_graphs(
        helper,
        include_default_graph=False,
        observed_classes=["https://example.org/C"],
        observed_properties=["https://example.org/p"],
    )
    assert result.discovered_named_graphs == 3
    assert result.graph_scan_truncated is False
    assert [item.graph_uri for item in result.candidates] == [
        "https://example.org/graph/ontology.owl"
    ]
    [candidate] = result.candidates
    assert candidate.explicit_ontology_iris == ["https://example.org/onto"]
    assert candidate.imports == ["https://example.org/base"]
    assert candidate.observed_class_overlap == ["https://example.org/C"]
    assert candidate.observed_property_overlap == ["https://example.org/p"]
    assert candidate.used_by_schema is True
    assert [(row.value, row.scope) for row in candidate.version_evidence] == [
        ("2026.09", "ontology")
    ]
    assert candidate.query_ids
