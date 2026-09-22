import json
from pathlib import Path

from rdflib import OWL, RDF, Graph, URIRef
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
