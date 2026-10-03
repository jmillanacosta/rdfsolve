import json

import pytest
import yaml
from rdfsolve.release import build_release_manifest
from rdfsolve.release.scientific_validation import build_scientific_validation_plan
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern


def test_plan_uses_observed_scope_snapshot_and_only_mined_patterns(tmp_path):
    (tmp_path / "sources.yaml").write_text(
        yaml.safe_dump(
            [
                {
                    "name": "demo",
                    "endpoint": "https://example.org/sparql",
                    "graph_uris": ["urn:registry"],
                }
            ]
        )
    )
    folder = tmp_path / "demo"
    folder.mkdir()
    schema = MinedSchema(
        about=AboutMetadata(
            dataset_name="demo", graph_uris=["urn:observed"], snapshot_id="urn:snapshot"
        ),
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri="urn:p",
                object_class="urn:B",
                evidence_source=source,
            )
            for source in ("mined", "inferred")
        ],
    )
    path = folder / "demo_remote_schema.json"
    path.write_text(json.dumps(schema.to_dict()))
    manifest = build_release_manifest(tmp_path)
    plan = build_scientific_validation_plan(manifest, tmp_path, patterns_per_schema=10)
    assert len(plan.pattern_checks) == 1
    check = plan.pattern_checks[0]
    assert check.snapshot_id == "urn:snapshot"
    assert check.graph_scope == ["urn:observed"]
    assert "urn:registry" not in check.query
    path.write_text("{}")
    with pytest.raises(ValueError, match="differs from release"):
        build_scientific_validation_plan(manifest, tmp_path)
