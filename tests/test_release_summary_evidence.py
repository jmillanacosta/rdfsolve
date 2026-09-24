import json
from pathlib import Path

import yaml
from rdfsolve.release import build_release_manifest, summarize_release


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
    manifest = build_release_manifest(tmp_path, release_id="test")
    summary = summarize_release(manifest, tmp_path)
    assert summary["observed_evidence"] == {
        "datasets": 1,
        "patterns": 1,
        "structural_patterns": 0,
        "pattern_types": {"object_property": 1},
        "evidence_sources": {"mined": 1},
        "patterns_with_counts": 1,
        "patterns_with_distinct_subjects": 1,
        "patterns_with_distinct_objects": 1,
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
