"""rdfsolve.ontology.discovery: ontologies are discovered at an endpoint and their usage analysed."""

from __future__ import annotations

import json

from rdflib import OWL, RDF, Dataset, Literal, URIRef

from rdfsolve.ontology.discovery import discover_ontology_graphs, discover_remote_ontology_graphs


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


def test_usage_requires_overlap_with_observed_classes_or_properties():
    dataset = Dataset()
    graph = dataset.graph(URIRef("urn:ontology-graph"))
    graph.add((URIRef("urn:onto"), RDF.type, OWL.Ontology))
    graph.add((URIRef("https://example.org/C"), RDF.type, OWL.Class))
    graph.add((URIRef("https://example.org/p"), RDF.type, OWL.DatatypeProperty))
    [candidate] = discover_ontology_graphs(
        dataset,
        observed_classes=["https://example.org/C"],
        observed_properties=["https://example.org/p"],
    )
    assert candidate.used_by_schema is True
    assert candidate.observed_class_overlap == ["https://example.org/C"]
    assert candidate.observed_property_overlap == ["https://example.org/p"]


def test_remote_discovery_records_where_the_graph_names_came_from():
    helper = DatasetHelper(_dataset())
    scanned = discover_remote_ontology_graphs(helper, include_default_graph=False)
    given = discover_remote_ontology_graphs(
        helper,
        graph_uris=["https://example.org/graph/ontology.owl"],
        include_default_graph=False,
        graph_names_source="service_description",
        graph_names_evidence="https://example.org/.well-known/void",
    )
    assert scanned.graph_names_source == "endpoint_scan"
    assert scanned.graph_names_evidence is None
    assert given.graph_names_source == "service_description"
    assert given.graph_names_evidence == "https://example.org/.well-known/void"
    assert given.discovered_named_graphs == 1


def test_graph_names_are_listed_unordered_and_paged_only_at_a_cap(monkeypatch):
    """The unordered DISTINCT listing answers first (STRING: 0.25 s against 18 s for the
    ordered form, job 115333); the ordered pages are read only when the answer has exactly a
    common server cap of rows, and then a timeout is not retried with a smaller page."""
    from rdfsolve.sparql_helper import SparqlHelper
    from rdfsolve.void_retrieval import discover_graph_names

    calls: list[dict] = []
    selects: list[str] = []

    class Helper(DatasetHelper):
        def select(self, query: str, **kwargs):
            selects.append(query)
            return super().select(query, **kwargs)

        def select_chunked(self, query: str, **kwargs):
            calls.append(kwargs)
            yield from super().select_chunked(query, **kwargs)

    names = discover_graph_names(Helper(_dataset()), batch_size=100, max_pages=10)
    assert len(names) == 3 and names == sorted(names)
    assert calls == [] and "ORDER BY" not in selects[0]

    monkeypatch.setattr(SparqlHelper, "SUSPECTED_ROW_CAPS", frozenset({3}))
    names = discover_graph_names(Helper(_dataset()), batch_size=100, max_pages=10)
    assert len(names) == 3
    assert calls[0]["max_page_retries"] == 0
