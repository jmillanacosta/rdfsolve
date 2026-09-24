import json

import pytest
import yaml
from rdflib import Graph

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.release.build import build_release_manifest, write_release_manifest
from rdfsolve.schema_models.pattern import SchemaPattern
from scripts.pipeline_stages.analysis import AnalysisStage, SSSOMSeedingStage
from scripts.pipeline_stages.config import PipelineConfig


def test_release_analysis_keeps_channels_partial_evidence_and_shapes(tmp_path, monkeypatch):
    def no_http(*args, **kwargs):
        raise AssertionError("Analysis must use archived evidence")
    monkeypatch.setattr("requests.sessions.Session.request", no_http)
    registry = tmp_path / "sources.yaml"
    registry.write_text(yaml.safe_dump([{"name": n} for n in ("typed", "untyped", "failed")]))
    for name, mode, triples in [
        ("typed", "local", '<urn:a> a <urn:A>; <urn:link> <urn:b> . <urn:b> a <urn:B> .'),
        ("typed", "remote", '<urn:a> a <urn:A>; <urn:remote> "x" .'),
        ("untyped", "local", '<urn:u> <urn:link> "untyped" .'),
    ]:
        directory = tmp_path / name
        directory.mkdir(exist_ok=True)
        report = directory / f"{name}_{mode}_report.json"
        with SchemaMiner.from_graph(Graph().parse(data=triples, format="turtle"),
                delay=0, report_path=report) as miner:
            schema = miner.mine(name)
        if name == "typed":
            schema.raw_patterns = [p.model_copy(deep=True) for p in schema.patterns]
            schema.term_patterns = [SchemaPattern(subject_class="urn:Term",
                subject_binding="term", property_uri="urn:termLink", object_class="urn:B",
                count=1)]
        if mode == "remote":
            raw = json.loads(report.read_text())
            raw["completion_state"] = "partial"
            report.write_text(json.dumps(raw))
        (directory / f"{name}_{mode}_schema.json").write_text(json.dumps(schema.to_dict()))
    failed = tmp_path / "failed"
    failed.mkdir()
    (failed / "failed_remote_report.json").write_text(
        json.dumps({"completion_state": "failed", "abort_reason": "offline"}))
    manifest = build_release_manifest(tmp_path)
    write_release_manifest(manifest, tmp_path)

    from rdfsolve.analysis.release import analyze_release, write_release_analysis
    result = analyze_release(tmp_path)
    inventory = {(r["dataset_id"], r["mode"]): r for r in result["extraction_inventory"]}
    assert len(inventory) == 4
    assert inventory["typed", "remote"]["completion_state"] == "partial"
    assert inventory["failed", "remote"]["views"] is None
    assert inventory["untyped", "local"]["views"]["structural_patterns"] == {
        "rows": 1, "subject_shapes": 1}
    assert inventory["typed", "local"]["views"]["term_patterns"]["rows"] == 1
    assert inventory["untyped", "local"]["coverage"][0]["uncovered_triples"] == 1
    assert result["paper_statistics"]["channels"]["local"]["schema_extractions"] == 2
    assert result["paper_statistics"]["channels"]["remote"]["schema_extractions"] == 1
    assert result["paper_statistics"]["completion_states"] == {
        "complete": 2, "partial": 1, "failed": 1}
    comparison = next(r for r in result["channel_comparisons"] if r["view"] == "patterns")
    assert comparison["shared_classes"] == 1
    assert comparison["completion_states"] == ["complete", "partial"]
    assert comparison["comparison_state"] == "incomplete"
    assert comparison["absence_supported"] is False
    assert comparison["shared_count_basis"] == "observed_lower_bound"
    assert {r["source_mode"] for r in result["schema_overlaps"]} == {"local"}
    structural = next(r for r in result["schema_overlaps"]
                      if r["view"] == "structural_patterns")
    assert structural["comparison_state"] == "not_recorded"
    assert structural["shared_predicates"] is None, "Missing collection is not an empty result"
    for mode, graph in result["class_connectivity"].items():
        assert {n["mode"] for n in graph["nodes"]} <= {mode}
        assert all(n["dataset"] == "typed" for n in graph["nodes"])
    manifest_bytes = (tmp_path / "release.json").read_bytes()
    exported = tmp_path / "analysis-check"
    write_release_analysis(result, exported)
    assert json.loads((exported / "extraction_inventory.json").read_text()) == result["extraction_inventory"]
    assert (tmp_path / "release.json").read_bytes() == manifest_bytes

    config = PipelineConfig(base_dir=tmp_path, sources_file=registry, output_dir=tmp_path)
    state = AnalysisStage(config).run()
    assert state["success"], state
    assert state["schema_extractions"] == 3
    current = build_release_manifest(tmp_path)
    saved = json.loads((tmp_path / "release.json").read_text())
    assert {a.path: a.sha256 for a in current.artifacts} == {
        a["path"]: a["sha256"] for a in saved["artifacts"]}
    mappings = tmp_path / "mapping-sources.yaml"
    mappings.write_text("[]")
    config.sssom_sources_file = mappings
    def retain(**kwargs):
        assert len(kwargs["schemas"]) == 3, "Both channels must reach mapping selection"
        return {"fixture": 1}
    monkeypatch.setattr("rdfsolve.mappings.enrichment.enrich_external_sssom_sources", retain)
    assert SSSOMSeedingStage(config).run()["success"]

    report_path = tmp_path / "typed/typed_remote_report.json"
    report_bytes = report_path.read_bytes()
    report_path.write_bytes(report_bytes + b" ")
    with pytest.raises(ValueError, match="hash"):
        analyze_release(tmp_path)
    report_path.write_bytes(report_bytes)
    schema_path = tmp_path / "typed/typed_local_schema.json"
    schema_path.write_text(schema_path.read_text() + " ")
    with pytest.raises(ValueError, match="hash"):
        analyze_release(tmp_path)
