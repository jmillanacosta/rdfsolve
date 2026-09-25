import json
from datetime import datetime, timezone

import pytest
import yaml
from click.testing import CliRunner
from rdflib import RDF, Dataset, Literal, Namespace, URIRef
from rdfsolve.cli import main
from rdfsolve.config import mint
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.release.model import ReleaseManifest
from rdfsolve.release.scientific_execution import execute_scientific_validation_plan
from rdfsolve.release.scientific_validation import (
    build_scientific_validation_plan,
    write_scientific_validation_plan,
)
from rdfsolve.schema_models import MinedSchema
from rdfsolve.sparql_helper import EndpointError
from scripts.pipeline_stages.cli import Pipeline
from scripts.pipeline_stages.config import PipelineConfig
from scripts.pipeline_stages.grouped import GroupedMiningStage
from scripts.pipeline_stages.local import LocalMiningStage
from scripts.pipeline_stages.remote import RemoteMiningStage

E = Namespace("urn:fixture:")


@pytest.mark.parametrize("mode", ["remote", "local", "grouped"])
def test_graph_pipeline_release(tmp_path, monkeypatch, mode):
    state = "partial"
    working = tmp_path / "working"
    working.mkdir()
    monkeypatch.chdir(working)

    def no_http(*args, **kwargs):
        raise AssertionError("Fixture must not make HTTP requests")

    monkeypatch.setattr("requests.sessions.Session.request", no_http)
    data = Dataset(default_union=True)
    left_name, right_name = ("fixture.left", "fixture.right")
    left_graph, right_graph = (mint("graph", left_name), mint("graph", right_name))
    left = data.graph(URIRef(left_graph))
    right = data.graph(URIRef(right_graph))
    left.add((E.a, RDF.type, E.A))
    left.add((E.a, E.link, E.b))
    left.add((E.a, E.text, Literal("x")))
    left.add((E.a, E.text, Literal("y")))
    right.add((E.b, RDF.type, E.B))
    right.add((E.b, E.text, Literal("b")))
    right.add((E.b, E.link, E.c))
    right.add((E.c, RDF.type, E.C))
    context_graph = "urn:fixture:context"
    context = data.graph(URIRef(context_graph))
    context.add((E.b, RDF.type, E.Linked))
    context.add((E.decoy, RDF.type, E.A))
    context.add((E.decoy, E.leak, Literal("context")))
    rows = [
        {
            "name": left_name,
            "endpoint": "https://fixture.invalid/sparql",
            "graph_uris": [left_graph, right_graph],
            "last_checked": datetime.now(timezone.utc).isoformat(),
            "delay": 0,
            "type_context_graph_uris": [context_graph],
        }
    ]
    if mode != "remote":
        rows[0].update(local_provider="fixture", endpoint=None)
    if mode == "grouped":
        rows[0]["graph_uris"] = [left_graph]
        rows.append({"name": right_name, "local_provider": "fixture", "graph_uris": [right_graph]})
    registry = tmp_path / "sources.yaml"
    original = yaml.safe_dump(rows)
    registry.write_text(original)
    config = PipelineConfig(
        base_dir=tmp_path,
        repo_dir=tmp_path,
        sources_file=registry,
        output_dir=tmp_path / "run",
        output_suffix=f"_{mode}",
        output_formats=["void"],
        enrich=False,
        delay=0,
        navigation_hops=2,
        navigation_min_hops=2,
        navigation_probes=1,
        collect_property_usage_evidence=True,
        discover_ontology_graphs=True,
        no_index=True,
        no_download=True,
    )
    config.load_sources()
    config.archive_run_inputs()
    miners = []

    def factory(**kwargs):
        miner = SchemaMiner.from_graph(data, **kwargs)
        miners.append(miner)
        if state == "partial":
            select = miner.helper.select

            def fail_literal_count(query, **options):
                if options.get("purpose") == "counts/literal":
                    raise EndpointError("Fixture count failure")
                return select(query, **options)

            monkeypatch.setattr(miner.helper, "select", fail_literal_count)
        return miner

    if mode == "remote":
        monkeypatch.setattr("rdfsolve.SchemaMiner", factory)
        stage = RemoteMiningStage
    else:

        def local_miner(self, port, graph_uris, report_path, *, type_context_graph_uris=None,
                        resume_checkpoint=None):
            return factory(
                graph_uris=graph_uris or [left_graph, right_graph], report_path=report_path, delay=0,
                type_context_graph_uris=type_context_graph_uris, resume_checkpoint=resume_checkpoint
            )

        monkeypatch.setattr(LocalMiningStage, "_local_miner", local_miner)
        monkeypatch.setattr(LocalMiningStage, "_ensure_qlever_image", lambda self: None)
        monkeypatch.setattr(LocalMiningStage, "_has_qlever_index", lambda *args: True)
        monkeypatch.setattr(LocalMiningStage, "_qlever_start", lambda *args: 1)
        monkeypatch.setattr(LocalMiningStage, "_qlever_stop", lambda *args: None)
        stage = GroupedMiningStage if mode == "grouped" else LocalMiningStage
    try:
        results = Pipeline(config).add_stage(stage).run()
    finally:
        for miner in miners:
            miner.close()
    result = results[stage.name]
    assert result["state"] == state
    assert result["success"] is (state == "complete")
    assert not result["failed"]
    if state == "partial":
        assert result["partial"]
    output = config.output_dir
    schema_paths = [output / row["name"] / f"{row['name']}_{mode}_schema.json" for row in rows]
    schemas = [MinedSchema.from_json(path) for path in schema_paths]
    link = next(
        (
            p
            for p in schemas[0].patterns
            if p.subject_class == str(E.A) and p.property_uri == str(E.link)
        )
    )
    assert {p.object_class for p in schemas[0].patterns if p.property_uri == str(E.link) and p.subject_class == str(E.A)} == {str(E.B), str(E.Linked)}
    assert all(p.property_uri != str(E.leak) for schema in schemas for p in schema.patterns)
    assert link.count == 1 and link.graphs == {left_graph: 1}
    assert all((p.pattern_type != "unknown" for schema in schemas for p in schema.patterns))
    assert schemas[0].about.class_entity_counts[str(E.A)] == 1
    for path in schema_paths:
        report = json.loads(
            path.with_name(path.name.replace("_schema.json", "_report.json")).read_text()
        )
        assert report["completion_state"] == state and report["finished_at"]
    if mode == "grouped":
        group = MinedSchema.from_json(output / "grouped_fixture/fixture_grouped_schema.json")
        for index, graph in enumerate([left_graph, right_graph]):
            expected = {
                (p.subject_class, p.property_uri, p.object_class, p.datatype): p.graphs[graph]
                for p in group.patterns
                if p.graphs and graph in p.graphs
            }
            actual = {
                (p.subject_class, p.property_uri, p.object_class, p.datatype): p.count
                for p in schemas[index].patterns
            }
            assert actual == expected
        assert len({schema.about.snapshot_id for schema in schemas}) == 2
        if state == "complete":
            assert all((p.graphs for p in group.patterns))
    elif state == "complete":
        text = next(
            (
                p
                for p in schemas[0].patterns
                if p.subject_class == str(E.A) and p.property_uri == str(E.text)
            )
        )
        assert text.count == 2
        assert schemas[0].navigation.paths
    runner = CliRunner()
    for command in ["build", "validate"]:
        checked = runner.invoke(main, ["release", command, str(output)])
        assert checked.exit_code == 0, checked.output
    manifest = ReleaseManifest.model_validate_json((output / "release.json").read_text())
    assert len(manifest.datasets) == len(rows)
    assert all((d.completion_state == state for d in manifest.datasets))
    plan = build_scientific_validation_plan(manifest, output, patterns_per_schema=100)
    write_scientific_validation_plan(plan, output / "validation/scientific_checks.json")
    assert plan.pattern_checks
    target = "remote_endpoint" if mode == "remote" else "frozen_local_index"
    assert all((check.target_kind == target for check in plan.pattern_checks + plan.route_checks))
    validator = SchemaMiner.from_graph(data)
    try:
        for row in rows:
            checks = [check for check in plan.pattern_checks if check.dataset_id == row["name"]]
            if mode == "remote":
                validator.helper.endpoint_url = checks[0].endpoint
            observed = execute_scientific_validation_plan(
                plan,
                validator.helper,
                dataset_id=row["name"],
                extraction_mode=mode,
                index_reference="in-memory-fixture" if mode != "remote" else None,
            )
            assert all(
                (
                    result.state == "matched"
                    for result in observed.results
                    if result.check_id in {c.check_id for c in checks}
                )
            )
            assert all((result.comparison == "agrees" for result in observed.results))
            (output / "validation" / f"{row['name']}_scientific_check_results.json").write_text(
                observed.model_dump_json(indent=2)
            )
    finally:
        validator.close()
    for command in ["build", "validate"]:
        checked = runner.invoke(main, ["release", command, str(output)])
        assert checked.exit_code == 0, checked.output
    summary = json.loads((output / "summary.json").read_text())
    assert summary["attempted_completion"] == {state: len(rows)}
    assert summary["observed_evidence"]["patterns"] == sum((len(s.patterns) for s in schemas))
    assert summary["property_usage_evidence"]["class_populations_available"] > 0
    assert "unknown" not in summary["observed_evidence"]["pattern_types"]
    for schema in schemas:
        assert all(
            (
                not any(
                    (
                        str(term).startswith("http://www.w3.org/ns/shacl#")
                        or "sparql-examples" in str(term)
                        for term in (p.subject_class, p.property_uri, p.object_class)
                    )
                )
                for p in schema.patterns
            )
        )
    for path in output.glob("*/*_ontology_acquisition.json"):
        assert not json.loads(path.read_text())["local_ontology_file_candidates"]
    assert registry.read_text() == original == (output / "sources.yaml").read_text()
    assert list(working.iterdir()) == []
