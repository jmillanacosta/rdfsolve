"""Release denominators retain explicit resource kinds and unknown decisions."""

import json

from rdfsolve.analysis.release import analyze_release
from rdfsolve.release.build import build_release_manifest, write_release_manifest


def test_release_separates_reference_resources_without_dropping_them(tmp_path):
    (tmp_path / "sources.yaml").write_text(
        "- name: observations\n  dataset_kind: instance\n"
        "- name: vocabulary\n  dataset_kind: ontology\n"
        "- name: undecided\n"
        "- name: service\n  source_role: service\n"
    )
    manifest = build_release_manifest(tmp_path)
    write_release_manifest(manifest, tmp_path)
    result = analyze_release(tmp_path)
    assert result["paper_statistics"]["dataset_kinds"] == {
        "instance": 1,
        "ontology": 1,
        "unknown": 1,
    }, "Ontology and unclassified records cannot inflate instance counts"
    assert result["paper_statistics"]["service_records"] == 1
    assert {row["dataset_id"]: row["dataset_kind"] for row in result["dataset_inventory"]} == {
        "observations": "instance",
        "vocabulary": "ontology",
        "undecided": "unknown",
    }
    saved = json.loads((tmp_path / "release.json").read_text())
    assert {row["dataset_kind"] for row in saved["datasets"]} == {
        "instance",
        "ontology",
        "unknown",
    }, "Classification survives release serialization"
