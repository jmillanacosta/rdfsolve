import json

import pytest
from rdflib import Dataset, URIRef
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.schema_models import MinedSchema
from scripts.pipeline_stages.base import PartialMiningError
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.grouped import GroupedMiningStage


def test_group_outputs_keep_counts_and_report_failures(tmp_path, monkeypatch):
    data = Dataset(default_union=False)
    sources = []
    for name in ("first", "second"):
        graph = "urn:graph:" + name
        data.graph(URIRef(graph)).parse(
            data=f'<urn:{name}> a <urn:C>; <urn:p> "{name}" .', format="turtle"
        )
        sources.append(Source.from_dict({"name": name, "graph_uris": [graph]}))

    for failure in ("member", "group", None):
        output = tmp_path / (failure or "complete")
        config = PipelineConfig(base_dir=tmp_path, output_dir=output,
                                enrich=False, navigation_hops=0, output_formats=["json-ld"])
        stage = GroupedMiningStage(config)
        group_report = output / "grouped_fixture/fixture_report.json"

        def local_miner(port, graphs, report_path, **options):
            return SchemaMiner.from_graph(data, graph_uris=graphs, report_path=report_path,
                                          delay=0, enrich=False)

        def member_evidence(schema, directory, name, suffix, **options):
            if name == "second":
                report = json.loads((output / "first/first_report.json").read_text())
                assert report["finished_at"] is None, "Member report finalized during group output"
                if failure == "member":
                    raise OSError("second member evidence failed")

        export = MinedSchema.to_jsonld

        def group_export(schema, **options):
            if failure == "group" and schema.about.dataset_name == "fixture":
                raise OSError("group export failed")
            return export(schema, **options)

        with monkeypatch.context() as patch:
            patch.setattr(stage, "_local_miner", local_miner)
            patch.setattr(stage, "_save_property_usage_evidence", member_evidence)
            patch.setattr(MinedSchema, "to_jsonld", group_export)
            if failure:
                with pytest.raises(PartialMiningError, match="failed"):
                    stage._mine_grouped("fixture", sources, 7000)
            else:
                assert stage._mine_grouped("fixture", sources, 7000) == ["first", "second"]

        report = json.loads(group_report.read_text())
        assert report["completion_state"] == ("partial" if failure else "complete")
        assert report["finished_at"], "Output attempt has no final timestamp"
        if failure:
            assert any("failed" in (phase["error"] or "") for phase in report["phases"])
        schema = MinedSchema.from_json(output / "grouped_fixture/fixture_schema.json")
        assert next(p.count for p in schema.patterns if p.property_uri == "urn:p") == 2
        if failure != "group":
            for name in ("first", "second"):
                saved = MinedSchema.from_json(output / name / f"{name}_schema.json")
                assert next(p.count for p in saved.patterns if p.property_uri == "urn:p") == 1
                member = json.loads((output / name / f"{name}_report.json").read_text())
                assert member == report, "Member report differs from the final group attempt"
