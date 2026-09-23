import json

from rdflib import Graph

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointError
from scripts.pipeline_stages.base import PartialMiningError
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.local import LocalMiningStage


def test_late_failure_keeps_schema_and_marks_partial(tmp_path, monkeypatch):
    data = Graph().parse(data='<urn:s> a <urn:C>; <urn:p> "value" .', format="turtle")
    config = PipelineConfig(base_dir=tmp_path, output_dir=tmp_path / "output", enrich=False, navigation_hops=0)
    source = Source.from_dict({"name": "fixture", "local_provider": "fixture"})
    stage = LocalMiningStage(config)
    report_path = config.output_dir / "fixture" / "fixture_report.json"
    with SchemaMiner.from_graph(data, report_path=report_path, delay=0) as miner:
        select = miner.helper.select

        def select_with_failure(query, **options):
            if options.get("purpose") == "declared_classes":
                raise EndpointError("fixture declaration timeout")
            return select(query, **options)

        monkeypatch.setattr(miner.helper, "select", select_with_failure)
        schema = miner.mine("fixture")
        assert any(p.property_uri == "urn:p" and p.count == 1 for p in schema.patterns)
        report = json.loads(report_path.read_text())
        assert report["completion_state"] == "partial"
        assert any("declared_classes" in str(f) for f in report["query_failures"])
        output = config.output_dir / "fixture"

        def fail_optional(*args, **kwargs):
            assert json.loads(report_path.read_text())["finished_at"] is None
            raise EndpointError("fixture property usage timeout")

        monkeypatch.setattr(stage, "_save_property_usage_evidence", fail_optional)
        try:
            with stage._output_phase(miner, report_path):
                stage._save_dataset_outputs(source, schema, output, miner.helper, "local_distribution")
        except PartialMiningError:
            pass
        path = output / "fixture_schema.json"
        assert path.is_file(), "Optional evidence failure discarded the mined schema"
        assert type(schema).from_json(path).patterns == schema.patterns
        report = json.loads(report_path.read_text())
        assert report["completion_state"] == "partial" and report["finished_at"]
        assert any(p["error"] == "fixture property usage timeout" for p in report["phases"])

    from rdfsolve.mining import mine_with_ontology
    from scripts.pipeline_stages.remote import RemoteMiningStage

    def unavailable(*args, **kwargs):
        raise EndpointError("fixture ontology timeout")

    monkeypatch.setattr("rdfsolve.mining.OntologyMiner.mine", unavailable)
    with SchemaMiner.from_graph(data, delay=0) as miner:
        result = mine_with_ontology(miner, extract_ontology=True, dataset_name="ontology_failure")
        assert result.data_schema.patterns and result.ontology is None
        assert miner.last_report.completion_state == "partial"
    skipped = Source.from_dict({"name": "unavailable", "endpoint": "https://example.invalid/sparql", "endpoint_down": True, "failure_count": 3})
    outcome = RemoteMiningStage(config)._mine_single_source(skipped)
    assert outcome["status"] == "skipped"
    record = json.loads((config.output_dir / "unavailable/unavailable_report.json").read_text())
    assert record["completion_state"] == "skipped" and record["reason"]

    health = tmp_path / "health.json"
    health.write_text(json.dumps({"endpoints": {"offline": {"status": "down"}}}))
    config.endpoint_status_file = health
    both = Source.from_dict({"name": "offline", "endpoint": "https://example.invalid/sparql", "download_ttl": ["https://example.invalid/data.ttl"]})
    config.sources = config._filter_by_health_checks([both])
    assert config.get_local_sources() == [both], "Endpoint health removed the local access channel"
    assert config.get_remote_sources() == [both], "Skipped remote attempt has no report path"
    assert RemoteMiningStage(config)._mine_single_source(both)["status"] == "skipped"
