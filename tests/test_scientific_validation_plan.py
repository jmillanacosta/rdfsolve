import json
from pathlib import Path

import yaml
from rdflib.plugins.sparql.parser import parseQuery

from rdfsolve.release import build_release_manifest
from rdfsolve.release.scientific_validation import build_scientific_validation_plan
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary
from rdfsolve.schema_models.pattern import SchemaPattern


def test_validation_plan_is_deterministic_and_separates_remote_local_targets(tmp_path: Path):
    (tmp_path / "sources.yaml").write_text(
        yaml.safe_dump([
            {"name": "remote", "endpoint": "https://example.org/sparql", "graph_uris": ["urn:g"]},
            {"name": "local", "download_ttl": "https://example.org/local.ttl"},
        ]), encoding="utf-8"
    )
    for name, mode in [("remote", "remote"), ("local", "local")]:
        folder = tmp_path / name
        folder.mkdir()
        pattern = SchemaPattern(
            subject_class="urn:A", property_uri="urn:p", object_class="urn:B"
        )
        route = NavigationPath(
            steps=[pattern, SchemaPattern(subject_class="urn:B", property_uri="urn:q", object_class="urn:C")],
            instance_support="matched",
            source_count=2,
            matched_sources=1,
            query="SELECT ?source WHERE { ?source <urn:p>/<urn:q> ?target }",
        )
        schema = MinedSchema(
            patterns=[pattern],
            about=AboutMetadata(name=name, endpoint="https://example.org/sparql" if name == "remote" else None),
            navigation=NavigationSummary(
                max_hops=2,
                max_paths_per_length=10,
                edge_count=2,
                walk_counts={2: 1},
                paths=[route],
            ),
        )
        (folder / f"{name}_{mode}_schema.json").write_text(
            json.dumps(schema.to_dict(), indent=2), encoding="utf-8"
        )
        (folder / f"{name}_{mode}_report.json").write_text(
            json.dumps({"completion_state": "complete"}), encoding="utf-8"
        )
    manifest = build_release_manifest(tmp_path, release_id="pilot")
    first = build_scientific_validation_plan(manifest, tmp_path, patterns_per_schema=1, routes_per_schema=1)
    second = build_scientific_validation_plan(manifest, tmp_path, patterns_per_schema=1, routes_per_schema=1)
    assert first == second
    assert len(first.pattern_checks) == 2
    by_name = {check.dataset_id: check for check in first.pattern_checks}
    assert by_name["remote"].target_kind == "remote_endpoint"
    assert by_name["remote"].endpoint == "https://example.org/sparql"
    assert by_name["local"].target_kind == "frozen_local_index"
    assert by_name["local"].endpoint is None
    for check in first.pattern_checks:
        parseQuery(check.query)
    assert len(first.route_checks) == 2
