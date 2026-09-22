import json
from pathlib import Path

from rdflib import Graph, OWL, RDF, URIRef

from rdfsolve.evidence.ontology_reference import acquire_reference_ontologies
from rdfsolve.evidence.ontology_registry import OntologyRegistry


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
    (d / "demo_remote_schema.json").write_text(json.dumps({"schema": {"patterns": [
        {"subject_class": "http://purl.obolibrary.org/obo/TEST_1", "property_uri": "http://purl.obolibrary.org/obo/TEST_2", "object_class": "Literal"},
        {"subject_class": "https://unrelated.example/A", "property_uri": "https://unrelated.example/p", "object_class": "Literal"},
    ]}}))
    (d / "demo_remote_ontology_acquisition.json").write_text(json.dumps({
        "dataset_id": "demo",
        "candidates": [{
            "namespace": "http://purl.obolibrary.org/obo/TEST_",
            "ontology_id": "test",
            "observed_classes": ["http://purl.obolibrary.org/obo/TEST_1"],
            "observed_properties": ["http://purl.obolibrary.org/obo/TEST_2"],
            "provider_graphs": [],
            "reference_sources": [{
                "ontology_id": "test",
                "namespace": "http://purl.obolibrary.org/obo/TEST_",
                "source_url": "https://purl.obolibrary.org/obo/test.owl",
                "resolution_basis": "obo_purl_namespace",
                "observed_classes": ["http://purl.obolibrary.org/obo/TEST_1"],
                "observed_properties": ["http://purl.obolibrary.org/obo/TEST_2"]
            }],
            "identity_basis": "reference_source"
        }],
        "infrastructure_namespaces": []
    }))


def test_reference_acquisition_deduplicates_and_scopes_usage(tmp_path: Path):
    _run_fixture(tmp_path)
    calls=[]
    result=acquire_reference_ontologies(tmp_path, fetcher=lambda url: calls.append(url) or _ontology_bytes())
    assert calls == ["https://purl.obolibrary.org/obo/test.owl"]
    assert not result.failures
    usage=json.loads((tmp_path / "demo" / "demo_ontology_usage.json").read_text())["usages"][0]
    assert usage["resolved_classes"] == ["http://purl.obolibrary.org/obo/TEST_1"]
    assert usage["unresolved_classes"] == []
    assert usage["resolved_properties"] == ["http://purl.obolibrary.org/obo/TEST_2"]
    assert usage["unresolved_properties"] == []
    registry=OntologyRegistry.load(tmp_path / "ontologies" / "registry.json")
    assert len(registry.releases("test")) == 1


def test_reference_acquisition_reuses_archived_source(tmp_path: Path):
    _run_fixture(tmp_path)
    data=_ontology_bytes()
    first=[]
    acquire_reference_ontologies(tmp_path, fetcher=lambda url: first.append(url) or data)
    second=[]
    acquire_reference_ontologies(tmp_path, fetcher=lambda url: second.append(url) or data)
    assert len(first)==1
    assert second == []


def test_reference_acquisition_keeps_output_outside_working_directory(tmp_path, monkeypatch):
    working = tmp_path / "working"
    working.mkdir()
    output = tmp_path / "run"
    _run_fixture(output)
    monkeypatch.chdir(working)
    result = acquire_reference_ontologies(output, fetcher=lambda _: _ontology_bytes())
    assert not result.failures
    assert (output / result.registry_path).is_file()
    assert list(working.iterdir()) == []
