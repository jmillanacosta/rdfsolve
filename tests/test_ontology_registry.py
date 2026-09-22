from rdfsolve.evidence.ontology import OntologyArtifact
from rdfsolve.evidence.ontology_registry import OntologyRegistry


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
