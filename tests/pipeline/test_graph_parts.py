"""Per-graph schemas of a source mined across several named graphs, in the local and remote
channels: each data graph gets a schema named after the registry entry that is that graph, links
between graphs stay, and a graph with settings of its own is mined with them."""

import json

import yaml
from rdflib import Dataset, Graph, URIRef

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.void_strategy import PublishedVoid, VoidStrategy
from rdfsolve.release.build import build_release_manifest
from rdfsolve.release.summary import summarize_release
from rdfsolve.release.validate import validate_release
from rdfsolve.schema_models import AboutMetadata, MinedSchema
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.local import LocalMiningStage
from scripts.pipeline_stages.remote import RemoteMiningStage

ENDPOINT = "https://fixture.invalid/sparql"
A, B, R = "urn:graph:proteins", "urn:graph:taxa", "urn:graph:reactions"


def _source(**extra):
    return Source.from_dict(
        {
            "name": "fixture",
            "endpoint": ENDPOINT,
            "graph_uris": [A, B, R],
            "graph_sources": {g: {"download_ttl": [f"{g}.ttl"]} for g in (A, B, R)},
            "graph_settings": {R: {"classes_as_data": True}},
            **extra,
        }
    )


def _registry(source):
    # The entry for the proteins graph alone on the same endpoint is a graph scope of fixture.
    scope = Source.from_dict(
        {
            "name": "fixture.proteins_entry",
            "endpoint": ENDPOINT + "/",
            "graph_uris": [A],
            "skip_mining": True,
        }
    )
    return [source, scope]


def _config(tmp_path, suffix):
    return PipelineConfig(
        base_dir=tmp_path,
        output_dir=tmp_path / "run",
        output_suffix=suffix,
        enrich=False,
        navigation_hops=0,
        output_formats=["void"],
    )


def _mine_locally(tmp_path, monkeypatch):
    """Mine the fixture source in the local channel; return the run folder and the miners."""
    data = Dataset(default_union=False)
    data.graph(URIRef(A)).parse(
        data='<urn:p1> a <urn:Protein>; <urn:organism> <urn:t1>; <urn:name> "p" .', format="turtle"
    )
    data.graph(URIRef(B)).parse(data='<urn:t1> a <urn:Taxon>; <urn:name> "t" .', format="turtle")
    data.graph(URIRef(R)).parse(data='<urn:r1> a <urn:Reaction>; <urn:name> "r" .', format="turtle")
    source = _source()
    config = _config(tmp_path, "_local")
    config.registry = _registry(source)
    stage = LocalMiningStage(config)
    miners = []

    def local_miner(port, graphs, report_path, *, type_context_graph_uris=None, **options):
        miner = SchemaMiner.from_graph(
            data,
            graph_uris=graphs,
            type_context_graph_uris=type_context_graph_uris,
            report_path=report_path,
            delay=0,
            enrich=False,
        )
        miners.append((graphs, type_context_graph_uris, miner))
        return miner

    monkeypatch.setattr(stage, "_local_miner", local_miner)
    stage._mine_local(source, 7000)
    run = tmp_path / "run"
    registry = [entry.model_dump(exclude_defaults=True) for entry in config.registry]
    (run / "sources.yaml").write_text(yaml.safe_dump(registry))
    return run, miners


def test_the_local_stage_writes_a_schema_per_graph_and_mines_a_graph_with_its_settings(
    tmp_path, monkeypatch
):
    run, miners = _mine_locally(tmp_path, monkeypatch)
    assert (run / "fixture" / "fixture_local_schema.json").is_file(), "The whole schema is kept"
    index = json.loads((run / "fixture" / "fixture_local_graph_parts.json").read_text())
    parts = {row["name"]: row for row in index["parts"]}
    assert set(parts) == {"fixture.proteins_entry", "fixture.taxa", "fixture.reactions"}
    assert parts["fixture.proteins_entry"]["registry_entry"] == "fixture.proteins_entry"
    assert parts["fixture.taxa"]["derivation"] == "edge_graph_split"
    assert parts["fixture.reactions"]["derivation"] == "mined_with_graph_settings"

    def part(name):
        path = run / "fixture" / "graphs" / name / f"{name}_local_schema.json"
        return MinedSchema.from_json(path)

    proteins = part("fixture.proteins_entry")
    assert proteins.about.dataset_name == "fixture.proteins_entry"
    assert proteins.about.graph_uris == [A]
    # The link from a protein to a taxon typed in another graph stays in the proteins graph.
    assert ("urn:Protein", "urn:organism", "urn:Taxon") in {
        (p.subject_class, p.property_uri, p.object_class) for p in proteins.patterns
    }
    assert {p.subject_class for p in part("fixture.taxa").patterns} == {"urn:Taxon"}
    # The graph with its own settings is mined alone, its types resolved over the other graphs.
    graphs, context, miner = miners[-1]
    assert graphs == [R] and context == [A, B]
    assert miner.classes_as_data is True
    assert miners[0][2].classes_as_data is False
    assert {p.subject_class for p in part("fixture.reactions").patterns} == {"urn:Reaction"}
    assert (
        run / "fixture" / "graphs" / "fixture.reactions" / "fixture.reactions_local_report.json"
    ).is_file()


def test_the_release_links_a_graph_scope_entry_to_its_per_graph_schema(tmp_path, monkeypatch):
    run, _ = _mine_locally(tmp_path, monkeypatch)
    manifest = build_release_manifest(run)
    records = {record.dataset_id: record for record in manifest.datasets}
    parts = {part.name: part for part in records["fixture"].graph_parts}
    assert set(parts) == {"fixture.proteins_entry", "fixture.taxa", "fixture.reactions"}
    proteins = parts["fixture.proteins_entry"]
    assert proteins.graph_uri == A and proteins.registry_entry == "fixture.proteins_entry"
    [extraction] = proteins.extractions
    assert extraction.mode == "local" and extraction.derivation == "edge_graph_split"
    assert extraction.schema_path == (
        "fixture/graphs/fixture.proteins_entry/fixture.proteins_entry_local_schema.json"
    )
    assert extraction.schema_artifact_id and extraction.snapshot_id
    assert parts["fixture.reactions"].own_settings and parts["fixture.reactions"].classes_as_data
    # The graph-scope entry, not mined itself, resolves to the per-graph schema of its graph.
    assert records["fixture.proteins_entry"].graph_part_of == "fixture"
    assert records["fixture"].graph_part_of is None
    roles = {a.path: a.role for a in manifest.artifacts}
    assert roles[extraction.schema_path] == "graph_part_canonical_schema"
    assert roles["fixture/fixture_local_schema.json"] == "canonical_schema"
    assert roles["fixture/fixture_local_graph_parts.json"] == "graph_parts_index"
    # Only the source's own schema is counted: its parts repeat its patterns.
    assert [e.schema_path for e in records["fixture"].extractions] == [
        "fixture/fixture_local_schema.json"
    ]
    assert summarize_release(manifest, run)["observed_evidence"]["schema_artifacts"] == 1
    # A part's schema is validated as a schema.
    with monkeypatch.context() as patch:
        checked = []
        patch.setattr(MinedSchema, "from_json", classmethod(lambda cls, path: checked.append(path)))
        assert validate_release(manifest, run).valid
    assert run / extraction.schema_path in checked


VOID = """
@prefix void: <http://rdfs.org/ns/void#> .
@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
<urn:service> sd:defaultDataset <urn:default> .
<urn:default> sd:namedGraph <urn:named:a>, <urn:named:b>, <urn:named:r> .
<urn:named:a> sd:name <urn:graph:proteins> ; sd:graph <urn:void:a> .
<urn:named:b> sd:name <urn:graph:taxa> ; sd:graph <urn:void:b> .
<urn:named:r> sd:name <urn:graph:reactions> ; sd:graph <urn:void:r> .
<urn:void:a> void:classPartition <urn:void:a:protein> ; void:subset <urn:void:a:link> .
<urn:void:a:protein> void:class <urn:Protein> ; void:entities 7 .
<urn:void:a:link> a void:Linkset ; void:subjectsTarget <urn:void:a:protein> ;
    void:linkPredicate <urn:organism> ; void:objectsTarget <urn:void:b:taxon> ; void:triples 7 .
<urn:void:b> void:classPartition <urn:void:b:taxon> .
<urn:void:b:taxon> void:class <urn:Taxon> ; void:entities 3 .
<urn:void:r> void:classPartition <urn:void:r:reaction> .
<urn:void:r:reaction> void:class <urn:Reaction> ; void:entities 2 .
"""


def test_the_remote_stage_reads_each_graph_from_the_void_and_mines_a_graph_with_its_settings(
    tmp_path, monkeypatch
):
    source = _source()
    config = _config(tmp_path, "_remote")
    config.registry = _registry(source)
    stage = RemoteMiningStage(config)
    published = PublishedVoid("urn:void", Graph().parse(data=VOID, format="turtle"), "2026-10-01")
    stage._published_void = {ENDPOINT: published}
    # The source's own settings keep the VoID-first reading of the whole source.
    strategy = stage._void_strategy(source, source.graph_uris)
    assert isinstance(strategy, VoidStrategy)

    mined = []

    def mine_graph_part(source, part, part_dir, delay):
        mined.append((part.graph, part.classes_as_data))
        miner = type("M", (), {"last_report": type("R", (), {"completion_state": "complete"})()})
        about = AboutMetadata(dataset_name=part.name, graph_uris=[part.graph])
        return MinedSchema(about=about, patterns=[]), miner

    monkeypatch.setattr(stage, "_mine_graph_part", mine_graph_part)
    whole = MinedSchema(about=AboutMetadata(dataset_name="fixture", graph_uris=[A, B, R]))
    miner = type("M", (), {"last_report": None, "declared_classes": frozenset()})()
    stage._save_graph_parts(source, whole, miner, strategy, 0.0)

    assert mined == [(R, True)]
    out = tmp_path / "run" / "fixture"
    index = json.loads((out / "fixture_remote_graph_parts.json").read_text())
    assert {row["name"]: row["derivation"] for row in index["parts"]} == {
        "fixture.proteins_entry": "void_scoped_to_graph",
        "fixture.taxa": "void_scoped_to_graph",
        "fixture.reactions": "mined_with_graph_settings",
    }
    name = "fixture.proteins_entry"
    proteins = MinedSchema.from_json(out / "graphs" / name / f"{name}_remote_schema.json")
    link = next(p for p in proteins.patterns if p.property_uri == "urn:organism")
    # The other end of a link to another graph keeps the class that graph's VoID gives it.
    assert (link.subject_class, link.object_class, link.count) == ("urn:Protein", "urn:Taxon", 7)
    assert link.graphs == {A: 7} and link.count_semantics == "triples_in_graph"
    assert proteins.about.graph_uris == [A]
    assert proteins.about.class_entity_counts == {"urn:Protein": 7}


def test_a_source_level_setting_still_turns_void_first_off(tmp_path):
    source = _source(classes_as_data=True, graph_settings={})
    stage = RemoteMiningStage(_config(tmp_path, "_remote"))
    assert stage._void_strategy(source, source.graph_uris) is None


def test_each_graph_part_names_the_pinned_inputs_of_its_source(tmp_path, monkeypatch):
    from rdfsolve.qlever.inputs import graph_input_directory

    run, _ = _mine_locally(tmp_path, monkeypatch)
    paths = {g: (graph_input_directory(run.parent, g) / "rdf" / "x.ttl") for g in (A, B, R)}
    files = [
        {"path": p.relative_to(run.parent).as_posix(), "bytes": 1, "sha256": "d" * 64}
        for p in paths.values()
    ]
    pins = {"source": "fixture", "recorded": "before_index", "downloads": [], "files": files}
    (run / "fixture" / "fixture_local_inputs.json").write_text(json.dumps(pins))
    records = {r.dataset_id: r for r in build_release_manifest(run).datasets}
    source = records["fixture"]
    assert source.input_manifest_artifact and source.input_file_count == 3
    parts = {part.graph_uri: part for part in source.graph_parts}
    for graph, part in parts.items():
        assert part.input_manifest_artifact == source.input_manifest_artifact
        assert part.input_paths == [paths[graph].relative_to(run.parent).as_posix()]
    entry = records["fixture.proteins_entry"]
    assert entry.input_manifest_artifact == source.input_manifest_artifact, "The source's pins"
    assert entry.input_manifest_path == "fixture/fixture_local_inputs.json"
