"""rdfsolve.ontology.artifacts: ontology artifacts, their registry, configured sources and
references."""

import json
from pathlib import Path

from rdflib import OWL, RDF, Graph, Literal, URIRef

from rdfsolve.ontology.artifacts import (
    LocalOntologyFileCandidate,
    OntologyArtifact,
    OntologyRegistry,
    archive_ontology_bytes,
)
from rdfsolve.ontology.reference import acquire_reference_ontologies
from rdfsolve.ontology.sources import build_ontology_acquisition_plan
from rdfsolve.ontology.usage import ObservedOntologyTerms


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
        data, cache_dir=tmp_path, source_url="https://example.org/onto.ttl", rdf_format="turtle"
    )
    assert artifact.ontology_iris == [str(ontology)]
    assert artifact.version_iris == [str(version)]
    assert artifact.version_values == ["1.2"]
    assert artifact.issued_values == ["2026-09-18"]
    assert artifact.imports == ["https://example.org/base"]
    assert artifact.sha256 and Path(artifact.local_path).read_bytes() == data
    assert len(parsed) == len(graph)


def artifact(id_, sha, version=None):
    return OntologyArtifact(
        artifact_id=id_,
        ontology_iris=["https://example.org/onto"],
        version_values=[version] if version else [],
        source_url="https://example.org/onto.owl",
        sha256=sha,
    )


def test_registry_separates_stable_identity_from_artifact_versions():
    registry = OntologyRegistry()
    registry.register_artifact(
        "onto", artifact("sha256:a", "a", "1"), preferred_iri="https://example.org/onto"
    )
    registry.register_artifact("onto", artifact("sha256:b", "b", "2"))
    assert list(registry.ontologies) == ["onto"]
    assert [item.version_values for item in registry.releases("onto")] == [["1"], ["2"]]


def _provider_ontology() -> bytes:
    g = Graph()
    onto = URIRef("https://provider.example/onto")
    g.add((onto, RDF.type, OWL.Ontology))
    g.add((URIRef("https://provider.example/Class"), RDF.type, OWL.Class))
    return g.serialize(format="xml", encoding="utf-8")


def test_local_owl_file_is_retained_only_when_signature_overlaps(tmp_path: Path):
    d = tmp_path / "demo"
    d.mkdir()
    (d / "demo_local_schema.json").write_text(
        json.dumps(
            {
                "schema": {
                    "patterns": [
                        {
                            "subject_class": "https://provider.example/Class",
                            "property_uri": "https://example.org/p",
                            "object_class": "Literal",
                        }
                    ]
                }
            }
        )
    )
    declared = d / "declared" / "local-distribution"
    declared.mkdir(parents=True)
    artifact_path = declared / "onto.owl"
    artifact_path.write_bytes(_provider_ontology())
    import hashlib

    digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    plan = build_ontology_acquisition_plan(
        "demo",
        ObservedOntologyTerms(
            classes={"https://provider.example/Class"}, properties={"https://example.org/p"}
        ),
        mining_context="local_distribution",
        local_ontology_file_candidates=[
            LocalOntologyFileCandidate(
                source_url="https://provider.example/onto.owl",
                source_dataset_id="demo",
                archived_path="declared/local-distribution/onto.owl",
                sha256=digest,
            )
        ],
    )
    (d / "demo_local_ontology_acquisition.json").write_text(plan.model_dump_json(indent=2))
    result = acquire_reference_ontologies(
        tmp_path,
        fetcher=lambda _: (_ for _ in ()).throw(AssertionError("must not refetch local artifact")),
    )
    assert not result.failures
    usages = json.loads((d / "demo_ontology_usage.json").read_text())["usages"]
    local = [u for u in usages if u["artifact_relation"] == "local_distribution_artifact"]
    assert len(local) == 1
    assert local[0]["resolved_classes"] == ["https://provider.example/Class"]


def _ontology_bytes() -> bytes:
    g = Graph()
    onto = URIRef("http://purl.obolibrary.org/obo/test.owl")
    cls = URIRef("http://purl.obolibrary.org/obo/TEST_1")
    prop = URIRef("http://purl.obolibrary.org/obo/TEST_2")
    g.add((onto, RDF.type, OWL.Ontology))
    g.add((onto, OWL.versionInfo, URIRef("https://example.org/releases/1")))
    g.add((cls, RDF.type, OWL.Class))
    g.add((prop, RDF.type, OWL.ObjectProperty))
    return g.serialize(format="xml", encoding="utf-8")


def _run_fixture(root: Path) -> None:
    d = root / "demo"
    d.mkdir(parents=True)
    (d / "demo_remote_schema.json").write_text(
        json.dumps(
            {
                "schema": {
                    "patterns": [
                        {
                            "subject_class": "http://purl.obolibrary.org/obo/TEST_1",
                            "property_uri": "http://purl.obolibrary.org/obo/TEST_2",
                            "object_class": "Literal",
                        },
                        {
                            "subject_class": "https://unrelated.example/A",
                            "property_uri": "https://unrelated.example/p",
                            "object_class": "Literal",
                        },
                    ]
                }
            }
        )
    )
    (d / "demo_remote_ontology_acquisition.json").write_text(
        json.dumps(
            {
                "dataset_id": "demo",
                "candidates": [
                    {
                        "namespace": "http://purl.obolibrary.org/obo/TEST_",
                        "ontology_id": "test",
                        "observed_classes": ["http://purl.obolibrary.org/obo/TEST_1"],
                        "observed_properties": ["http://purl.obolibrary.org/obo/TEST_2"],
                        "provider_graphs": [],
                        "reference_sources": [
                            {
                                "ontology_id": "test",
                                "namespace": "http://purl.obolibrary.org/obo/TEST_",
                                "source_url": "https://purl.obolibrary.org/obo/test.owl",
                                "resolution_basis": "obo_purl_namespace",
                                "observed_classes": ["http://purl.obolibrary.org/obo/TEST_1"],
                                "observed_properties": ["http://purl.obolibrary.org/obo/TEST_2"],
                            }
                        ],
                        "identity_basis": "reference_source",
                    }
                ],
                "infrastructure_namespaces": [],
            }
        )
    )


def test_reference_acquisition_deduplicates_and_scopes_usage(tmp_path: Path):
    _run_fixture(tmp_path)
    calls = []
    result = acquire_reference_ontologies(
        tmp_path, fetcher=lambda url: calls.append(url) or _ontology_bytes()
    )
    assert calls == ["https://purl.obolibrary.org/obo/test.owl"]
    assert not result.failures
    usage = json.loads((tmp_path / "demo" / "demo_ontology_usage.json").read_text())["usages"][0]
    assert usage["resolved_classes"] == ["http://purl.obolibrary.org/obo/TEST_1"]
    assert usage["unresolved_classes"] == []
    assert usage["resolved_properties"] == ["http://purl.obolibrary.org/obo/TEST_2"]
    assert usage["unresolved_properties"] == []
    registry = OntologyRegistry.load(tmp_path / "ontologies" / "registry.json")
    assert len(registry.releases("test")) == 1
