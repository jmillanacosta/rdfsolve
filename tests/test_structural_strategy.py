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
        select = miner.helper.select
        mining_queries = []
        def record_query(query, purpose=""):
            mining_queries.append(purpose)
            if purpose == "structural/discovery":
                phase = miner._report.report.phases[-1]
                assert phase.name == "structural-discovery" and phase.finished_at is None, (
                    "Attribute bulk discovery to its own active phase"
                )
            return select(query, purpose)
        monkeypatch.setattr(miner.helper, "select", record_query)
        schema = miner.mine("fixture")
        assert "structural/count" not in mining_queries
        assert "structural/witness" not in mining_queries

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

    mixed = Dataset(default_union=False)
    mixed.graph(URIRef("urn:data")).parse(data='''
        <urn:record> a <urn:Record>; <urn:context> <urn:context1> .
        <urn:context1> <urn:growth> "starvation"; <urn:allele> <urn:gene> .
        <urn:cross> <urn:value> "typed in another data graph"; <urn:link> <urn:gene> .
    ''', format="turtle")
    mixed.graph(URIRef("urn:types")).parse(
        data='<urn:cross> a <urn:Record> .', format="turtle")
    mixed.graph(URIRef("urn:objects")).parse(data="<urn:gene> a <urn:Gene> .", format="turtle")
    for strategy in ["two-phase", "one-shot", "single-pass", "structural"]:
        with SchemaMiner.from_graph(mixed, graph_uris=["urn:data", "urn:types"],
                type_context_graph_uris=["urn:objects"], strategy=strategy, delay=0) as miner:
            select = miner.helper.select
            def numeric_booleans(query, purpose=""):
                response = select(query, purpose)
                if purpose == "structural/coverage":
                    for row in response["results"]["bindings"]:
                        for field in ("typed", "eligible", "covered"):
                            if field in row:
                                row[field]["value"] = "1" if row[field]["value"] == "true" else "0"
                return response
            monkeypatch.setattr(miner.helper, "select", numeric_booleans)
            result = miner.mine("mixed")
            assert result.patterns, "Keep the typed schema"
            assert {p.property_uri for p in result.structural_patterns} == {
                "urn:growth", "urn:allele"}, "Only add uncovered subject records"
            assert sum(p.count for p in result.structural_patterns) == 2
            for p in result.structural_patterns:
                assert miner.helper.select(p.witness_query)["results"]["bindings"]
            assert miner.last_report.config["structural_coverage"][0]["untyped_subject_triples"] == 2

    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
    original = TwoPhaseStrategy.mine
    def omit_value(self, context):
        return [p for p in original(self, context) if p.property_uri != "urn:value"]
    with monkeypatch.context() as patched:
        patched.setattr(TwoPhaseStrategy, "mine", omit_value)
        with SchemaMiner.from_graph(mixed, graph_uris=["urn:data", "urn:types"],
                type_context_graph_uris=["urn:objects"], delay=0) as miner:
            result = miner.mine("missing typed edge")
            assert {p.property_uri for p in result.structural_patterns} == {
                "urn:growth", "urn:allele", "urn:value"}, "Compare actual profiles, not just type presence"
            missing = next(p for p in result.structural_patterns if p.property_uri == "urn:value")
            assert missing.count == 1 and missing.subject_selection == "uncovered"
            assert miner.helper.select(missing.witness_query)["results"]["bindings"]
            coverage = miner.last_report.config["structural_coverage"][0]
            assert coverage["uncovered_triples"] == 3 and coverage["covered_triples"] == 3

    varied = Dataset(default_union=False)
    graph = varied.graph(URIRef("urn:data"))
    graph.parse(data='<urn:typed> a <urn:Record>; <urn:value> "typed" .', format="turtle")
    from rdflib import Literal
    for i in range(105):
        graph.add((URIRef(f"urn:s{i}"), URIRef(f"urn:p{i}"), Literal("uncovered")))
    with SchemaMiner.from_graph(varied, graph_uris=["urn:data"], delay=0) as miner:
        result = miner.mine("no arbitrary cutoff")
        profiles = result.structural_patterns
        assert len(profiles) == 105 and sum(p.count for p in profiles) == 105
        assert all(p.subject_selection == "uncovered" for p in profiles)
        assert miner.last_report.completion_state == "complete"
        assert "structural_pattern_budget" not in miner.last_report.config
        path = tmp_path / "profiles.json"
        path.write_text(json.dumps(result.to_dict()))
        assert MinedSchema.from_json(path).structural_patterns == profiles
    with SchemaMiner.from_graph(mixed, graph_uris=["urn:data"], delay=0) as miner:
        select = miner.helper.select
        def fail_coverage(query, purpose=""):
            if purpose == "structural/coverage":
                raise EndpointError("coverage unavailable")
            return select(query, purpose)
        monkeypatch.setattr(miner.helper, "select", fail_coverage)
        with pytest.raises(EndpointError, match="coverage unavailable"):
            miner.mine("failed coverage")
        assert miner.last_report.completion_state != "complete"

    with SchemaMiner.from_graph(mixed, graph_uris=["urn:data"], delay=0) as miner:
        from rdfsolve.sparql_helper import SparqlHelper
        local = miner.helper
        select = local.select
        miner._helper = SparqlHelper("https://example.org/sparql")
        def zero_coverage(query, purpose=""):
            assert purpose != "structural/discovery", "Contradictory coverage must stop extraction"
            response = select(query, purpose)
            if purpose == "structural/coverage":
                for row in response["results"]["bindings"]:
                    row["covered"]["value"] = "false"
            return response
        monkeypatch.setattr(miner.helper, "select", zero_coverage)
        with pytest.raises(ValueError, match="Typed observations have zero edge coverage"):
            miner.mine("contradictory coverage")
        assert miner.last_report.completion_state == "failed"
        assert all(e["state"] == "failed" for e in miner.last_report.config["structural_coverage"])

    from rdflib import Graph
    lists = Graph().parse(data='''
        @prefix e: <https://example.org/> .
        e:s a e:A, e:B; e:value "typed"; e:items ("member") .
        e:u e:label "untyped" .
    ''', format="turtle")
    with SchemaMiner.from_graph(lists, delay=0) as miner:
        result = miner.mine("lists and multiple types")
        rows = result.structural_patterns
        assert len(result.patterns) == 6
        assert len(rows) == 3 and sum(p.count for p in rows) == 3
        assert len({(p.graph_uri, p.subject_kind, tuple(p.subject_properties)) for p in rows}) == 2
        assert len(result.collections) == 2
        assert miner.last_report.config["structural_coverage"][0]["covered_triples"] == 4
        for p in rows:
            assert int(miner.helper.select(p.recount_query)["results"]["bindings"][0]["n"]["value"]) == p.count
