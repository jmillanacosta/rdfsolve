"""Scientific observations must retain failures and source identity."""

from unittest.mock import Mock

import pytest

from rdfsolve.release.scientific_execution import execute_scientific_validation_plan
from rdfsolve.release.scientific_validation import (
    PatternSpotCheckPlan, RouteCheckPlan, ScientificValidationPlan,
)
from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError, PaginationTruncatedError


def plan(route=False, mode="remote"):
    common = dict(check_id="check:one", dataset_id="demo", snapshot_id="snapshot:one",
                  extraction_mode=mode, schema_artifact_id="sha256:one",
                  target_kind="remote_endpoint" if mode == "remote" else "frozen_local_index",
                  endpoint="https://example.org/sparql" if mode == "remote" else None,
                  query="SELECT ?s ?o WHERE { ?s ?p ?o } LIMIT 1")
    if route:
        check = RouteCheckPlan(**common, expected_instance_support="matched",
                               expected_source_count=2, expected_matched_sources=1)
        return ScientificValidationPlan(release_id="r", patterns_per_schema=1, routes_per_schema=1,
                                        route_checks=[check])
    check = PatternSpotCheckPlan(**common, pattern={})
    return ScientificValidationPlan(release_id="r", patterns_per_schema=1, routes_per_schema=1,
                                    pattern_checks=[check])


def execute(p, rows=None, error=None, **kwargs):
    helper = Mock(endpoint_url="https://example.org/sparql")
    helper.select.return_value = {"results": {"bindings": rows}}
    helper.select.side_effect = error
    return execute_scientific_validation_plan(p, helper, dataset_id="demo",
                                             extraction_mode=kwargs.pop("mode", "remote"), **kwargs)


@pytest.mark.parametrize("rows,state,comparison", [
    ([], "no_match", "differs"),
    ([{"s": {"type": "uri", "value": "urn:s"}, "o": {"type": "literal", "value": ""}}], "matched", "agrees"),
    ([{}], "error", "not_comparable"),
    (None, "error", "not_comparable"),
])
def test_pattern_observation(rows, state, comparison):
    result = execute(plan(), rows).results[0]
    assert (result.state, result.comparison) == (state, comparison)
    assert result.observed_at and result.snapshot_id == "snapshot:one"


@pytest.mark.parametrize("error,state", [
    (EndpointTimeoutError("timeout"), "timeout"),
    (EndpointError("blocked"), "error"),
    (PaginationTruncatedError("truncated"), "partial"),
])
def test_failures_are_not_negative_evidence(error, state):
    result = execute(plan(), error=error).results[0]
    assert result.state == state
    assert result.comparison == "not_comparable"
    assert result.error


@pytest.mark.parametrize("sources,matched,state,comparison", [
    (2, 1, "matched", "agrees"), (2, 0, "no_match", "differs"),
    (3, 1, "matched", "differs"), (1, 2, "error", "not_comparable"),
    (-1, 0, "error", "not_comparable"),
])
def test_route_recomputation(sources, matched, state, comparison):
    rows = [{"sources": {"value": str(sources)}, "matched": {"value": str(matched)}}]
    result = execute(plan(route=True), rows).results[0]
    assert (result.state, result.comparison) == (state, comparison)


def test_failed_original_route_is_not_a_negative_baseline():
    p = plan(route=True)
    p.route_checks[0].expected_instance_support = "timeout"
    result = execute(p, [{"sources": {"value": "2"}, "matched": {"value": "0"}}]).results[0]
    assert result.state == "no_match" and result.comparison == "not_comparable"


def test_targets_are_checked_before_queries():
    helper = Mock(endpoint_url="https://wrong.invalid/sparql")
    with pytest.raises(ValueError, match="Endpoint differs"):
        execute_scientific_validation_plan(plan(), helper, dataset_id="demo", extraction_mode="remote")
    with pytest.raises(ValueError, match="index reference"):
        execute_scientific_validation_plan(plan(mode="local"), helper, dataset_id="demo", extraction_mode="local")
    helper.select.assert_not_called()
    result = execute(plan(mode="local"), [], mode="local", index_reference="sha256:index-inputs")
    assert result.index_reference == "sha256:index-inputs" and result.plan_sha256


def test_release_validates_scientific_result_models(tmp_path):
    from rdfsolve.release import build_release_manifest, validate_release

    path = tmp_path / "demo_scientific_check_results.json"
    path.write_text('{"release_id": "r"}')
    manifest = build_release_manifest(tmp_path)
    assert manifest.artifacts[0].role == "scientific_validation_results"
    checked = validate_release(manifest, tmp_path)
    assert not checked.valid
    assert any(issue.kind == "json_model" for issue in checked.issues)
