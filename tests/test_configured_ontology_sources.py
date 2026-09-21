import json
from pathlib import Path

import pytest
from rdflib import Graph, OWL, RDF, URIRef

from rdfsolve.evidence.local_ontology_files import archive_local_ontology_files
from rdfsolve.evidence.ontology import ObservedOntologyTerms
from rdfsolve.evidence.ontology_acquisition import (
    LocalOntologyFileCandidate,
    build_ontology_acquisition_plan,
)
from rdfsolve.evidence.ontology_reference import acquire_reference_ontologies


def _provider_ontology() -> bytes:
    g = Graph()
    onto = URIRef("https://provider.example/onto")
    g.add((onto, RDF.type, OWL.Ontology))
    g.add((URIRef("https://provider.example/Class"), RDF.type, OWL.Class))
    return g.serialize(format="xml", encoding="utf-8")


def _local_candidate(url: str, dataset_id: str = "demo") -> LocalOntologyFileCandidate:
    return LocalOntologyFileCandidate(
        source_url=url,
        source_field="download_owl",
        source_dataset_id=dataset_id,
    )


def test_local_download_owl_is_distribution_candidate_not_usage_claim():
    plan = build_ontology_acquisition_plan(
        "demo",
        ObservedOntologyTerms(classes={"https://provider.example/Class"}),
        mining_context="local_distribution",
        local_ontology_file_candidates=[_local_candidate("https://provider.example/onto.owl")],
    )
    source = plan.local_ontology_file_candidates[0]
    assert source.source_url.endswith("onto.owl")
    assert source.discovery_basis == "local_distribution_format"
    assert source.source_dataset_id == "demo"
    assert source.archived_path is None
    assert all(candidate.identity_basis != "local_distribution_format" for candidate in plan.candidates)


def test_local_distribution_candidate_cannot_be_attached_to_remote_endpoint_run():
    with pytest.raises(ValueError, match="Local distribution"):
        build_ontology_acquisition_plan(
            "demo",
            ObservedOntologyTerms(classes={"https://provider.example/Class"}),
            mining_context="remote_endpoint",
            local_ontology_file_candidates=[_local_candidate("https://provider.example/onto.owl")],
        )


def test_local_owl_file_is_archived_from_exact_local_download(tmp_path: Path):
    workdir = tmp_path / "work"
    rdf = workdir / "rdf"
    rdf.mkdir(parents=True)
    (rdf / "onto.owl").write_bytes(_provider_ontology())
    output = tmp_path / "out"
    output.mkdir()
    rows = archive_local_ontology_files(
        source_dataset_id="demo",
        urls=["https://provider.example/onto.owl"],
        source_workdir=workdir,
        dataset_output_dir=output,
    )
    assert len(rows) == 1
    assert rows[0].archived_path == "declared/local-distribution/onto.owl"
    assert rows[0].sha256
    assert (output / rows[0].archived_path).read_bytes() == (rdf / "onto.owl").read_bytes()


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
            classes={"https://provider.example/Class"},
            properties={"https://example.org/p"},
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

    # If acquisition attempted to refetch the URL this test would fail.
    result = acquire_reference_ontologies(
        tmp_path,
        fetcher=lambda _: (_ for _ in ()).throw(AssertionError("must not refetch local artifact")),
    )
    assert not result.failures
    usages = json.loads((d / "demo_ontology_usage.json").read_text())["usages"]
    local = [u for u in usages if u["artifact_relation"] == "local_distribution_artifact"]
    assert len(local) == 1
    assert local[0]["resolved_classes"] == ["https://provider.example/Class"]
