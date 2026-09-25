import hashlib
import subprocess
import configparser
import json
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError
from rdflib import Dataset
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.release.build import build_release_manifest
from rdfsolve.schema_models import MinedSchema

from rdfsolve.models.source_model import SourceModel
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.grouped import GroupedMiningStage
from scripts.pipeline_stages.local import LocalMiningStage


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

    config = PipelineConfig(base_dir=tmp_path, sources_file=registry, no_download=True, delay=0, enrich=False, navigation_hops=0, extract_ontology=True)
    config.load_sources()
    config.archive_run_inputs()
    assert [s.name for s in config.get_local_sources()] == ["fixture"]
    source = config.sources[0]
    stage = LocalMiningStage(config)
    workdir = config.data_dir / "qlever_workdirs" / source.name
    workdir.mkdir(parents=True)
    content = {
        "urn:edges:a": '<urn:a> a <urn:A>; <urn:link> <urn:b> .',
        "urn:edges:b": '<urn:b> a <urn:B>; <urn:value> "x" .',
        "urn:types": '<urn:b> a <urn:Linked> . <urn:decoy> a <urn:A>; <urn:leak> "x" .',
        "urn:ontology": '<urn:A> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <urn:Root> .',
    }
    paths = {}
    for graph, fields in row["graph_sources"].items():
        directory = graph_input_directory(workdir, graph) / "rdf"
        directory.mkdir(parents=True)
        paths[graph] = directory / (hashlib.sha256(fields["download_ttl"][0].encode()).hexdigest() + ".ttl")
        paths[graph].write_text(content[graph])
    stage._prepare_qleverfile(workdir, source, 7020)
    run = subprocess.run
    calls = []
    monkeypatch.setattr("scripts.pipeline_stages.local.subprocess.run", lambda cmd, **kw: calls.append(cmd))
    stage._execute_qleverfile(workdir, source)
    index = calls[-1]
    observed = {}
    for offset, value in enumerate(index):
        if value == "-f":
            assert index[offset + 2] == "-F"
            assert index[offset + 4] == "-g"
            observed[index[offset + 5]] = Path(index[offset + 1]).read_text()
    assert set(observed) == set(row["graph_sources"]), "Index lost a graph assignment"
    assert observed == content
    data = Dataset(default_union=True)
    for graph, text in observed.items():
        data.graph(graph).parse(data=text, format="turtle")
    miners = []

    def local_miner(port, graph_uris, report_path, *, type_context_graph_uris, resume_checkpoint=None):
        miner = SchemaMiner.from_graph(data, graph_uris=graph_uris, type_context_graph_uris=type_context_graph_uris, report_path=report_path, delay=0)
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
    assert {p.object_class for p in schema.patterns if p.property_uri == "urn:link"} == {"urn:B", "urn:Linked"}
    assert all(p.property_uri != "urn:leak" for p in schema.patterns)
    manifest = build_release_manifest(config.output_dir)
    assert len(manifest.datasets) == 1
    record = manifest.datasets[0]
    assert record.graph_sources == row["graph_sources"]
    assert record.graph_scope == row["graph_uris"]
    assert record.extractions[0].type_context_graph_scope == ["urn:types"]
    assert record.extractions[0].ontology_graph_scope == ["urn:ontology"]
    assert record.completion_state == "complete"

    parser = configparser.ConfigParser(interpolation=None)
    parser.read(workdir / "Qleverfile")
    streams = json.loads(parser.get("index", "MULTI_INPUT_JSON"))
    assert {item["graph"] for item in streams} == set(observed)
    assert not parser.get("index", "CAT_INPUT_FILES")
    for stream in streams:
        observed_stream = run(["bash"], input=stream["cmd"], text=True, capture_output=True, check=True)
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
    inferred = Source.from_dict({"name": "pubchem.fake", "download_ttl": ["https://ftp.ncbi.nlm.nih.gov/pubchem/data.ttl"]})
    assert GroupedMiningStage(config)._identify_groups([inferred]) == {}
    assert registry.read_bytes() == before
