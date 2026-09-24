import json

import pytest
from rdflib import Dataset, URIRef

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.edge_graph_split import split_by_edge_graph
from rdfsolve.release import build_release_manifest
from rdfsolve.release.scientific_validation import build_scientific_validation_plan
from rdfsolve.release.scientific_execution import execute_scientific_validation_plan
from rdfsolve.release.summary import summarize_release
from rdfsolve.schema_models import MinedSchema
from rdfsolve.sparql_helper import EndpointError


def test_untyped_relations_survive_mining_release_and_recount(tmp_path, monkeypatch):
    data = Dataset(default_union=False)
    data.graph(URIRef("urn:data")).parse(data='''
        <urn:disease:1> <urn:association> <urn:a1>, <urn:a2> ; <urn:label> "Disease" .
        <urn:a1> <urn:evidence> <urn:eco:1>; <urn:pathogen> <urn:taxon:1> .
        <urn:a2> <urn:evidence> <urn:eco:1>; <urn:pathogen> <urn:taxon:2> .
        <urn:eco:1> <urn:label> "Evidence"@en .
        <urn:disease:2> <urn:association> [ <urn:evidence> <urn:eco:1>;
            <urn:pathogen> <urn:taxon:2> ] .
    ''', format="turtle")
    data.graph(URIRef("urn:other")).parse(data='''
        <urn:disease:1> <urn:association> <urn:a1> .
    ''', format="turtle")
    data.graph(URIRef("urn:context")).parse(data='''
        <urn:disease:1> a <http://www.w3.org/2002/07/owl#Class>;
            <urn:contextOnly> "excluded" .
    ''', format="turtle")
    folder = tmp_path / "fixture"
    folder.mkdir()
    with SchemaMiner.from_graph(data, graph_uris=["urn:data", "urn:other"],
            type_context_graph_uris=["urn:context"], delay=0,
            report_path=folder / "fixture_local_report.json") as miner:
        schema = miner.mine("fixture")
        assert not schema.patterns and not schema.about.class_entity_counts
        structural = schema.structural_patterns
        assert structural and miner.last_report.completion_state == "complete"
        assert all(p.property_uri != "urn:contextOnly" for p in structural)
        assert sum(p.count for p in structural) == 12, "Count each scoped triple once"
        link = next(p for p in structural if p.graph_uri == "urn:data"
                    and p.property_uri == "urn:association" and p.object_kind == "IRI")
        assert link.subject_properties == ["urn:association", "urn:label"]
        assert link.object_properties == ["urn:evidence", "urn:pathogen"]
        assert (link.count, link.distinct_subjects, link.distinct_objects) == (2, 1, 2)
        assert link.examples[0]["s"]["value"] == "urn:disease:1"
        for pattern in structural:
            rows = miner.helper.select(pattern.recount_query)["results"]["bindings"]
            assert int(rows[0]["n"]["value"]) == pattern.count, pattern
            assert miner.helper.select(pattern.witness_query)["results"]["bindings"], pattern
        split = split_by_edge_graph(schema, "urn:other", "other")
        assert len(split.structural_patterns) == 1
        assert split.structural_patterns[0].count == 1
        path = folder / "fixture_local_schema.json"
        path.write_text(json.dumps(schema.to_dict()))
        assert MinedSchema.from_json(path).structural_patterns == structural
        (tmp_path / "sources.yaml").write_text("- name: fixture\n")
        manifest = build_release_manifest(tmp_path)
        summary = summarize_release(manifest, tmp_path)
        assert summary["observed_evidence"]["structural_patterns"] == len(structural)
        plan = build_scientific_validation_plan(manifest, tmp_path, patterns_per_schema=100)
        assert len(plan.pattern_checks) == len(structural)
        results = execute_scientific_validation_plan(plan, miner.helper, dataset_id="fixture",
            extraction_mode="local", index_reference="fixture")
        assert all(r.comparison == "agrees" for r in results.results)

    with SchemaMiner.from_graph(data, graph_uris=["urn:data"], delay=0) as miner:
        select = miner.helper.select
        def fail_discovery(query, purpose=""):
            if purpose == "two-phase/classes":
                raise EndpointError("unavailable")
            return select(query, purpose)
        monkeypatch.setattr(miner.helper, "select", fail_discovery)
        with pytest.raises(EndpointError, match="unavailable"):
            miner.mine("fixture")
        assert miner.last_report.completion_state == "failed", "Do not hide failed type discovery"

    from click.testing import CliRunner
    from rdfsolve.cli import main
    from scripts.pipeline_stages.cli import Pipeline
    from scripts.pipeline_stages.config import PipelineConfig
    from scripts.pipeline_stages.local import LocalMiningStage

    output = tmp_path / "pipeline"
    registry = tmp_path / "pipeline-sources.yaml"
    registry.write_text("- name: fixture\n  local_provider: fixture\n  graph_uris: [urn:data]\n")
    config = PipelineConfig(base_dir=tmp_path, sources_file=registry,
        output_dir=output, output_suffix="_local", output_formats=["json"],
        enrich=False, navigation_hops=0, no_index=True, no_download=True)
    config.load_sources()
    def local_miner(self, port, graph_uris, report_path, **kwargs):
        return SchemaMiner.from_graph(data, graph_uris=graph_uris,
            report_path=report_path, delay=0)
    monkeypatch.setattr(LocalMiningStage, "_local_miner", local_miner)
    monkeypatch.setattr(LocalMiningStage, "_ensure_qlever_image", lambda self: None)
    monkeypatch.setattr(LocalMiningStage, "_has_qlever_index", lambda *args: True)
    monkeypatch.setattr(LocalMiningStage, "_qlever_start", lambda *args: 1)
    monkeypatch.setattr(LocalMiningStage, "_qlever_stop", lambda *args: None)
    result = Pipeline(config).add_stage(LocalMiningStage).run()
    assert result["local_mining"]["success"], result
    config.archive_run_inputs()
    runner = CliRunner()
    for command in ("build", "validate"):
        checked = runner.invoke(main, ["release", command, str(output)])
        assert checked.exit_code == 0, checked.output
    summary = json.loads((output / "summary.json").read_text())
    assert summary["attempted_completion"] == {"complete": 1}
    assert summary["observed_evidence"]["patterns"] == 0
    assert summary["observed_evidence"]["structural_patterns"] > 0
