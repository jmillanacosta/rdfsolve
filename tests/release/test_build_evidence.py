"""rdfsolve.release.build evidence: the summary evidence of a release, its declared comparisons and
ontology integration."""

import json
from pathlib import Path
import yaml
from rdfsolve.release import build_release_manifest, summarize_release
from rdflib import RDF, SH, Graph, Namespace
from rdfsolve.analysis.release_declared import (
    build_release_declared_comparison,
    write_release_declared_comparison,
)
from rdfsolve.evidence.declared import DeclaredArtifact, project_declared_evidence
from rdfsolve.evidence.declared_sources import DeclaredArtifactBundle
from rdfsolve.release.build import build_release_manifest, write_release_manifest
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern


def test_release_summary_reads_only_frozen_evidence_artifacts(tmp_path: Path):
    (tmp_path / "sources.yaml").write_text(
        yaml.safe_dump([{"name": "demo", "endpoint": "https://example.org/sparql"}]),
        encoding="utf-8",
    )
    dataset = tmp_path / "demo"
    dataset.mkdir()
    (dataset / "demo_remote_report.json").write_text(
        json.dumps({"completion_state": "complete"}), encoding="utf-8"
    )
    (dataset / "demo_remote_schema.json").write_text(
        json.dumps(
            {
                "schema": {
                    "patterns": [
                        {
                            "subject_class": "urn:A",
                            "property_uri": "urn:p",
                            "object_class": "urn:B",
                            "pattern_type": "object_property",
                            "evidence_source": "mined",
                            "count": 3,
                            "distinct_subjects": 2,
                            "distinct_objects": 2,
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    (dataset / "demo_remote_property_usage.json").write_text(
        json.dumps(
            {
                "dataset_id": "demo",
                "class_populations": [
                    {"class_iri": "urn:A", "subject_count": 2, "count_status": "complete"},
                    {"class_iri": "urn:B", "subject_count": 0, "count_status": "complete"},
                    {"class_iri": "urn:C", "subject_count": 1, "count_status": "partial"},
                    {"class_iri": "urn:D", "subject_count": None, "count_status": "failed"},
                ],
                "records": [
                    {
                        "subject_class": "urn:A",
                        "property_uri": "urn:p",
                        "eligible_subjects": 2,
                        "subjects_with_property": 2,
                        "summary_state": {"status": "complete"},
                        "node_kind_state": {"status": "complete"},
                        "datatype_state": None,
                        "histogram_state": {"status": "not_run"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (dataset / "demo_remote_declared_artifacts.json").write_text(
        json.dumps(
            {
                "dataset_id": "demo",
                "access_context": "remote_endpoint",
                "artifacts": [
                    {"kind": "shacl", "parse_status": "parsed"},
                    {"kind": "void", "parse_status": "parsed"},
                ],
                "evidence": [
                    {"declaration_type": "shacl_class"},
                    {"declaration_type": "rdfs_range"},
                ],
                "errors": [],
            }
        ),
        encoding="utf-8",
    )
    schema_path = dataset / "demo_remote_schema.json"
    raw = json.loads(schema_path.read_text())
    evidence = raw["schema"]
    evidence["raw_patterns"] = evidence["patterns"].copy()
    term = dict(evidence["patterns"][0], object_binding="term")
    evidence["term_patterns"] = [term]
    from rdfsolve.schema_models.structural import StructuralPattern
    shape = StructuralPattern(
        subject_properties=["urn:p", "urn:q"], subject_kind="IRI",
        property_uri="urn:p", object_kind="Literal", graph_uri="urn:g",
        count=1, distinct_subjects=1, distinct_objects=1,
        witness_query="SELECT ?s WHERE { ?s <urn:p> ?o } LIMIT 1",
        recount_query="SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p> ?o }",
    ).model_dump()
    evidence["structural_patterns"] = [shape, dict(shape, property_uri="urn:q")]
    schema_path.write_text(json.dumps(raw))
    (dataset / "demo_local_schema.json").write_text(json.dumps({
        "schema": {"patterns": [], "structural_patterns": [shape,
            dict(shape, shape_semantics="property_profile", subject_properties=[])]}}))
    (dataset / "demo_local_report.json").write_text(
        json.dumps({"completion_state": "partial"}))
    manifest = build_release_manifest(tmp_path, release_id="test")
    summary = summarize_release(manifest, tmp_path)
    assert summary["observed_evidence"] == {
        "datasets": 1,
        "schema_artifacts": 2,
        "patterns": 1,
        "raw_patterns": 1,
        "term_patterns": 1,
        "structural_patterns": 4,
        "structural_subject_shapes": 2,
        "retained_collections": {"raw_patterns": 1, "term_patterns": 1, "structural_patterns": 2},
        "pattern_types": {"object_property": 1},
        "evidence_sources": {"mined": 1},
        "patterns_with_counts": 1,
        "patterns_with_distinct_subjects": 1,
        "patterns_with_distinct_objects": 1,
        "sampled_patterns": 0,
        "patterns_with_lower_bound_counts": 0,
    }
    assert summary["property_usage_evidence"]["datasets"] == 1
    assert summary["property_usage_evidence"]["records"] == 1
    assert summary["property_usage_evidence"]["class_populations"] == 4
    assert summary["property_usage_evidence"]["class_populations_available"] == 2
    assert summary["property_usage_evidence"]["records_with_support_fraction"] == 1
    assert summary["property_usage_evidence"]["summary_state"] == {"complete": 1}
    assert summary["declared_evidence"]["artifacts"] == 2
    assert summary["declared_evidence"]["artifact_kinds"] == {"shacl": 1, "void": 1}
    assert summary["declared_evidence"]["declaration_types"] == {"rdfs_range": 1, "shacl_class": 1}


def _release(tmp_path: Path):
    (tmp_path / "sources.yaml").write_text(
        "sources:\n  demo:\n    endpoint: https://example.org/sparql\n", encoding="utf-8"
    )
    ds_dir = tmp_path / "demo"
    ds_dir.mkdir()
    schema = MinedSchema(
        about=AboutMetadata(dataset_name="demo", endpoint="https://example.org/sparql"),
        patterns=[
            SchemaPattern(
                subject_class="urn:ex:Person", property_uri="urn:ex:p", object_class="urn:ex:Place"
            )
        ],
    )
    for channel in ("local", "remote"):  # two access channels, declared evidence for one
        (ds_dir / f"demo_{channel}_schema.json").write_text(
            __import__("json").dumps(schema.to_dict(), indent=2), encoding="utf-8"
        )
    ex = Namespace("urn:ex:")
    graph = Graph()
    graph.add((ex.Shape, RDF.type, SH.NodeShape))
    graph.add((ex.Shape, SH.targetClass, ex.Person))
    graph.add((ex.Shape, SH.property, ex.PS))
    graph.add((ex.PS, SH.path, ex.p))
    graph.add((ex.PS, SH["class"], ex.Place))
    artifact = DeclaredArtifact(
        artifact_id="declared:fixture",
        dataset_id="demo",
        kind="shacl",
        source_graph="urn:graph:shapes",
        retrieved_at="2026-09-21T00:00:00+00:00",
        sha256="0" * 64,
        local_path="declared/shapes.ttl",
        representation="constructed_graph",
        retrieval_method="fixture",
    )
    bundle = DeclaredArtifactBundle(
        dataset_id="demo",
        access_context="remote_endpoint",
        artifacts=[artifact],
        evidence=project_declared_evidence(graph, artifact),
    )
    (ds_dir / "demo_local_declared_artifacts.json").write_text(
        bundle.model_dump_json(indent=2), encoding="utf-8"
    )
    manifest = build_release_manifest(tmp_path, release_id="release:test")
    write_release_manifest(manifest, tmp_path)
    return manifest


def test_release_declared_comparison_uses_frozen_artifacts_only(tmp_path):
    manifest = _release(tmp_path)
    result = build_release_declared_comparison(manifest, tmp_path)
    assert result.compared_datasets == ["demo"], "Several channels do not exclude a dataset"
    assert result.skipped == {"demo/demo_remote": "mined schema without declared artifacts"}
    assert {row.channel for row in result.comparisons} == {"demo_local"}
    by_dim = {row.dimension: row for row in result.comparisons}
    assert by_dim["property"].relation == "both"
    assert by_dim["class"].relation == "equal"
    json_path, tsv_path = write_release_declared_comparison(result, tmp_path / "analysis")
    assert json_path.exists() and tsv_path.exists()
    assert "urn:ex:Person" in tsv_path.read_text(encoding="utf-8")


def test_release_links_global_ontology_registry_and_dataset_usage(tmp_path: Path):
    (tmp_path / "sources.yaml").write_text(yaml.safe_dump({"sources": [{"name": "demo"}]}))
    d = tmp_path / "demo"
    d.mkdir()
    (d / "demo_remote_report.json").write_text(json.dumps({"completion_state": "complete"}))
    (d / "demo_remote_schema.json").write_text(json.dumps({"schema": {"patterns": []}}))
    (d / "demo_ontology_acquisition.json").write_text(
        json.dumps(
            {
                "dataset_id": "demo",
                "infrastructure_namespaces": [],
                "candidates": [
                    {
                        "namespace": "http://purl.obolibrary.org/obo/TEST_",
                        "ontology_id": "test",
                        "observed_classes": ["http://purl.obolibrary.org/obo/TEST_1"],
                        "observed_properties": [],
                        "provider_graphs": [],
                        "reference_sources": [{"source_url": "https://example.org/test.owl"}],
                        "identity_basis": "reference_source",
                    }
                ],
            }
        )
    )
    (d / "demo_ontology_usage.json").write_text(
        json.dumps(
            {
                "dataset_id": "demo",
                "usages": [
                    {
                        "dataset_id": "demo",
                        "ontology_artifact_id": "sha256:abc",
                        "ontology_id": "test",
                        "namespace": "http://purl.obolibrary.org/obo/TEST_",
                        "artifact_relation": "reference_release",
                        "version_match_status": "unknown",
                        "resolved_classes": ["http://purl.obolibrary.org/obo/TEST_1"],
                        "unresolved_classes": [],
                        "resolved_properties": [],
                        "unresolved_properties": [],
                        "term_usage": [],
                    }
                ],
            }
        )
    )
    o = tmp_path / "ontologies"
    o.mkdir()
    (o / "registry.json").write_text("{}")
    manifest = build_release_manifest(tmp_path, release_id="test")
    assert all((ds.dataset_id != "ontologies" for ds in manifest.datasets))
    assert manifest.ontology_registry_artifact
    usage = manifest.datasets[0].ontology_usages[0]
    assert usage.ontology_artifact_id == "sha256:abc"
    assert usage.resolved_class_count == 1
    summary = summarize_release(manifest)
    assert summary["assessed_ontology_usages"] == 1
    assert summary["ontology_version_match"] == {"unknown": 1}
