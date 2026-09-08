"""Keep dataset releases separate from the canonical JSON format version."""

from unittest.mock import Mock

import pytest
from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import OWL

from rdfsolve.metadata import build_metadata_query, query_endpoint_metadata
from rdfsolve.schema_models import AboutMetadata, MinedSchema


@pytest.mark.parametrize(
    "fields,expected",
    [
        ({"source_version_iri": "urn:release", "source_version": "1"}, "urn:release"),
        ({"source_version": "2026.09"}, "2026.09"),
        ({"source_modified": "2026-09-01"}, "2026-09-01"),
        ({"source_issued": "2026-08-01"}, "2026-08-01"),
        ({}, "snapshot:2026-09-07T12:00:00+00:00"),
    ],
)
def test_build_uses_source_release_then_dated_snapshot(fields, expected):
    about = AboutMetadata.build(finished_at="2026-09-07T12:00:00+00:00", **fields)
    assert about.schema_version == expected
    assert MinedSchema(patterns=[], about=about).to_dict()["version"] == 1


def test_version_iri_survives_rdf_exports():
    schema = MinedSchema(
        patterns=[],
        about=AboutMetadata.build(
            dataset_name="test",
            endpoint="https://example.org/sparql",
            source_version_iri="urn:release",
        ),
    )
    assert MinedSchema.from_dict(schema.to_jsonld()).about.source_version_iri == "urn:release"
    assert (
        MinedSchema.from_void(
            schema.to_void_graph().serialize(format="turtle")
        ).about.schema_version
        == "urn:release"
    )
    assert schema.to_linkml().version == "urn:release"


def test_dataset_query_does_not_select_imported_ontology():
    graph = Graph()
    graph.add((URIRef("urn:imported"), RDF.type, OWL.Ontology))
    graph.add((URIRef("urn:imported"), OWL.versionIRI, URIRef("urn:wrong-release")))
    assert list(graph.query(build_metadata_query())) == []


def test_metadata_does_not_mix_datasets():
    helper = Mock(endpoint_url="https://example.org/sparql")
    graph = Graph()
    void = Namespace("http://rdfs.org/ns/void#")
    for name in ("a", "b"):
        graph.add((URIRef(f"urn:{name}"), RDF.type, void.Dataset))
        graph.add((URIRef(f"urn:{name}"), OWL.versionIRI, URIRef(f"urn:release-{name}")))
    helper.construct.side_effect = lambda query: graph.query(query).serialize(format="turtle").decode()
    assert query_endpoint_metadata(helper) == {}
    graph.add((URIRef("urn:b"), void.sparqlEndpoint, URIRef(helper.endpoint_url)))
    assert query_endpoint_metadata(helper)["source_version_iri"] == "urn:release-b"


def test_metadata_failure_is_not_empty_metadata():
    helper = Mock()
    helper.construct.side_effect = RuntimeError("query failed")
    with pytest.raises(RuntimeError, match="query failed"):
        query_endpoint_metadata(helper)
