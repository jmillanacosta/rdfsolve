"""scripts.pipeline_stages.grouped: sources mined as one group keep their own outputs and counts,
and failures are reported for the group."""

import configparser
import hashlib
import json
import subprocess
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError
from rdflib import Dataset, URIRef

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.models.source_model import SourceModel
from rdfsolve.release.build import build_release_manifest
from rdfsolve.schema_models import MinedSchema
from scripts.pipeline_stages.base import PartialMiningError
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.grouped import GroupedMiningStage
from scripts.pipeline_stages.local import LocalMiningStage


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
        config = PipelineConfig(
            base_dir=tmp_path,
            output_dir=output,
            enrich=False,
            navigation_hops=0,
            output_formats=["json-ld"],
        )
        stage = GroupedMiningStage(config)
        group_report = output / "grouped_fixture/fixture_report.json"

        def local_miner(port, graphs, report_path, **options):
            return SchemaMiner.from_graph(
                data, graph_uris=graphs, report_path=report_path, delay=0, enrich=False
            )

        def member_evidence(
            schema, directory, name, suffix, output=output, failure=failure, **options
        ):
            if name == "second":
                report = json.loads((output / "first/first_report.json").read_text())
                assert report["finished_at"] is None, "Member report finalized during group output"
                if failure == "member":
                    raise OSError("second member evidence failed")

        export = MinedSchema.to_jsonld

        def group_export(schema, failure=failure, export=export, **options):
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


def test_registry_graph_inputs_reach_one_index(tmp_path, monkeypatch):
    from rdfsolve.qlever.inputs import graph_input_directory

    row = {
        "name": "fixture",
        "graph_uris": ["urn:edges:a", "urn:edges:b"],
        "type_context_graph_uris": ["urn:types"],
        "ontology_graph_uris": ["urn:ontology"],
        "graph_sources": {
            graph: {"download_ttl": [f"https://example.org/{number}/data.ttl"]}
            for number, graph in enumerate(
                ["urn:edges:a", "urn:edges:b", "urn:types", "urn:ontology"]
            )
        },
        "sampled_graphs": {"urn:edges:b": "Sample: one of the files of urn:edges:b."},
    }
    registry = tmp_path / "sources.yaml"
    registry.write_text(yaml.safe_dump([row]))
    before = registry.read_bytes()
    from scripts.article_pilot import build_pilot_registries

    spec = tmp_path / "pilot.yaml"
    spec.write_text("local:\n  - name: fixture\n")
    pilot = build_pilot_registries(registry, spec, tmp_path / "pilot")
    assert pilot["modes"]["local"]["source_count"] == 1
    assert yaml.safe_load((tmp_path / "pilot/local/sources.yaml").read_text()) == [row]

    config = PipelineConfig(
        base_dir=tmp_path,
        sources_file=registry,
        no_download=True,
        delay=0,
        enrich=False,
        navigation_hops=0,
        extract_ontology=True,
    )
    config.load_sources()
    config.archive_run_inputs()
    assert [s.name for s in config.get_local_sources()] == ["fixture"]
    source = config.sources[0]
    stage = LocalMiningStage(config)
    workdir = config.data_dir / "qlever_workdirs" / source.name
    workdir.mkdir(parents=True)
    content = {
        "urn:edges:a": "<urn:a> a <urn:A>; <urn:link> <urn:b> .",
        "urn:edges:b": '<urn:b> a <urn:B>; <urn:value> "x" .',
        "urn:types": '<urn:b> a <urn:Linked> . <urn:decoy> a <urn:A>; <urn:leak> "x" .',
        "urn:ontology": "<urn:A> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <urn:Root> .",
    }
    paths = {}
    for graph, fields in row["graph_sources"].items():
        directory = graph_input_directory(workdir, graph) / "rdf"
        directory.mkdir(parents=True)
        paths[graph] = directory / (
            hashlib.sha256(fields["download_ttl"][0].encode()).hexdigest() + ".ttl"
        )
        paths[graph].write_text(content[graph])
    stage._prepare_qleverfile(workdir, source, 7020)
    run = subprocess.run
    calls = []

    def index(cmd, **kw):
        # As qlever-index: the index states its triples.
        calls.append(cmd)
        (workdir / "fixture.meta-data.json").write_text('{"num-triples": {"normal": 4}}')

    monkeypatch.setattr("scripts.pipeline_stages.local.subprocess.run", index)
    stage._execute_qleverfile(workdir, source)
    assert calls[-1][-1] == str(workdir / "index-command.sh")
    import shlex

    index = shlex.split((workdir / "index-command.sh").read_text().splitlines()[-1])
    observed = {}
    for offset, value in enumerate(index):
        if value == "-f":
            assert index[offset + 2] == "-F"
            assert index[offset + 4] == "-g"
            observed[index[offset + 5]] = (workdir / index[offset + 1]).read_text()
    assert set(observed) == set(row["graph_sources"]), "Index lost a graph assignment"
    assert observed == content
    data = Dataset(default_union=True)
    for graph, text in observed.items():
        data.graph(graph).parse(data=text, format="turtle")
    miners = []

    def local_miner(
        port, graph_uris, report_path, *, type_context_graph_uris, resume_checkpoint=None
    ):
        miner = SchemaMiner.from_graph(
            data,
            graph_uris=graph_uris,
            type_context_graph_uris=type_context_graph_uris,
            report_path=report_path,
            delay=0,
        )
        miners.append(miner)
        return miner

    monkeypatch.setattr(stage, "_local_miner", local_miner)
    monkeypatch.setattr(stage, "_has_qlever_index", lambda *args: True)
    monkeypatch.setattr(stage, "_ensure_qlever_image", lambda: None)
    monkeypatch.setattr(stage, "_qlever_start", lambda *args: 1)
    monkeypatch.setattr(stage, "_qlever_stop", lambda *args: None)
    try:
        result = stage.run()
    finally:
        for miner in miners:
            miner.close()
    assert result["state"] == "complete" and result["mined"] == ["fixture"], result
    schema = MinedSchema.from_json(config.output_dir / "fixture/fixture_schema.json")
    assert {p.object_class for p in schema.patterns if p.property_uri == "urn:link"} == {
        "urn:B",
        "urn:Linked",
    }
    assert all(p.property_uri != "urn:leak" for p in schema.patterns)
    manifest = build_release_manifest(config.output_dir)
    assert len(manifest.datasets) == 1
    record = manifest.datasets[0]
    assert record.graph_sources == row["graph_sources"]
    assert record.sampled_graphs == row["sampled_graphs"]
    assert record.graph_scope == row["graph_uris"]
    assert record.extractions[0].type_context_graph_scope == ["urn:types"]
    assert record.extractions[0].ontology_graph_scope == ["urn:ontology"]
    assert record.completion_state == "complete"
    assert record.input_manifest_artifact and record.inputs_recorded == "before_index"
    assert record.input_file_count == len(paths), "Each input file of the index is pinned"
    assert {item.path for item in record.input_downloads} == {
        path.relative_to(workdir).as_posix() for path in paths.values()
    }

    parser = configparser.ConfigParser(interpolation=None)
    parser.read(workdir / "Qleverfile")
    streams = json.loads(parser.get("index", "MULTI_INPUT_JSON"))
    assert {item["graph"] for item in streams} == set(observed)
    assert not parser.get("index", "CAT_INPUT_FILES")
    for stream in streams:
        observed_stream = run(
            ["bash"], input=stream["cmd"], text=True, capture_output=True, check=True
        )
        assert observed_stream.stdout == content[stream["graph"]]
    missing = paths["urn:types"]
    missing.unlink()
    with pytest.raises(ValueError, match="urn:types"):
        stage._execute_qleverfile(workdir, source)
    assert len(calls) == 1, "Index started with a missing companion graph"
    for invalid in [
        {**row, "graph_uris": ["urn:missing"]},
        {**row, "graph_sources": {"relative": {"download_ttl": ["https://example.org/data.ttl"]}}},
        {**row, "download_ttl": ["https://example.org/unassigned.ttl"]},
    ]:
        with pytest.raises(ValidationError):
            SourceModel.model_validate(invalid)
    inferred = Source.from_dict(
        {"name": "pubchem.fake", "download_ttl": ["https://ftp.ncbi.nlm.nih.gov/pubchem/data.ttl"]}
    )
    assert GroupedMiningStage(config)._identify_groups([inferred]) == {}
    assert registry.read_bytes() == before
