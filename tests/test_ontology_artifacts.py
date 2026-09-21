from pathlib import Path

from rdflib import OWL, RDF, Graph, Literal, URIRef

from rdfsolve.evidence.ontology_artifacts import archive_ontology_bytes


def test_archive_preserves_provider_version_and_hashes_bytes(tmp_path: Path):
    ontology = URIRef("https://example.org/onto")
    version = URIRef("https://example.org/onto/1.2")
    graph = Graph()
    graph.add((ontology, RDF.type, OWL.Ontology))
    graph.add((ontology, OWL.versionIRI, version))
    graph.add((ontology, OWL.versionInfo, Literal("1.2")))
    graph.add((ontology, URIRef("http://purl.org/dc/terms/issued"), Literal("2026-09-18")))
    graph.add((ontology, OWL.imports, URIRef("https://example.org/base")))
    data = graph.serialize(format="turtle", encoding="utf-8")

    artifact, parsed = archive_ontology_bytes(
        data,
        cache_dir=tmp_path,
        source_url="https://example.org/onto.ttl",
        rdf_format="turtle",
    )
    assert artifact.ontology_iris == [str(ontology)]
    assert artifact.version_iris == [str(version)]
    assert artifact.version_values == ["1.2"]
    assert artifact.issued_values == ["2026-09-18"]
    assert artifact.imports == ["https://example.org/base"]
    assert artifact.sha256 and Path(artifact.local_path).read_bytes() == data
    assert len(parsed) == len(graph)


def test_retrieval_time_does_not_become_provider_version(tmp_path: Path):
    graph = Graph()
    ontology = URIRef("https://example.org/unversioned")
    graph.add((ontology, RDF.type, OWL.Ontology))
    data = graph.serialize(format="turtle", encoding="utf-8")
    artifact, _ = archive_ontology_bytes(
        data,
        cache_dir=tmp_path,
        retrieved_at="2026-09-18T10:00:00+00:00",
        rdf_format="turtle",
    )
    assert artifact.version_iris == []
    assert artifact.version_values == []
    assert artifact.retrieved_at == "2026-09-18T10:00:00+00:00"
