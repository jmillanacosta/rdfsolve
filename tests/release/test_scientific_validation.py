"""rdfsolve.release scientific validation: a validation plan and its execution."""

import json
import pytest
import yaml
from rdfsolve.release import build_release_manifest
from rdfsolve.release.scientific_validation import build_scientific_validation_plan
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern
from unittest.mock import Mock
from rdfsolve.release.scientific_execution import execute_scientific_validation_plan
from rdfsolve.release.scientific_validation import (
    PatternSpotCheckPlan,
    RouteCheckPlan,
    ScientificValidationPlan,
)
from rdfsolve.sparql_helper import EndpointTimeoutError


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


def test_saved_checks_keep_observations_and_failures():
    common = dict(
        dataset_id="demo",
        snapshot_id="snapshot:one",
        extraction_mode="remote",
        schema_artifact_id="sha256:one",
        target_kind="remote_endpoint",
        endpoint="https://example.org/sparql",
        query="SELECT ?s ?o WHERE { ?s ?p ?o } LIMIT 1",
    )
    plan = ScientificValidationPlan(
        release_id="r",
        patterns_per_schema=4,
        routes_per_schema=1,
        pattern_checks=[
            PatternSpotCheckPlan(check_id=name, pattern={}, **common)
            for name in ("witness", "absent", "timeout", "invalid")
        ],
        route_checks=[
            RouteCheckPlan(
                check_id="route",
                expected_instance_support="matched",
                expected_source_count=2,
                expected_matched_sources=1,
                **common,
            )
        ],
    )
    witness = {"s": {"type": "uri", "value": "urn:s"}, "o": {"type": "literal", "value": ""}}
    helper = Mock(endpoint_url=common["endpoint"])
    helper.select.side_effect = [
        {"results": {"bindings": [witness]}},
        {"results": {"bindings": []}},
        EndpointTimeoutError("offline"),
        {"results": {"bindings": [{}]}},
        {"results": {"bindings": [{"sources": {"value": "2"}, "matched": {"value": "1"}}]}},
    ]
    result = execute_scientific_validation_plan(
        plan, helper, dataset_id="demo", extraction_mode="remote"
    )
    assert {row.check_id: (row.state, row.comparison) for row in result.results} == {
        "witness": ("matched", "agrees"),
        "absent": ("no_match", "differs"),
        "timeout": ("timeout", "not_comparable"),
        "invalid": ("error", "not_comparable"),
        "route": ("matched", "agrees"),
    }, "Scientific check outcomes"
    assert result.results[0].rows == [witness], "Saved witness"
    assert (result.results[-1].source_count, result.results[-1].matched_sources) == (2, 1), (
        "Route counts"
    )
    assert result.results[2].error == "offline", "Query failure detail"
    helper.endpoint_url = "https://wrong.example/sparql"
    with pytest.raises(ValueError, match="Endpoint differs"):
        execute_scientific_validation_plan(
            plan, helper, dataset_id="demo", extraction_mode="remote"
        )
