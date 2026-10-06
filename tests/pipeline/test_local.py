"""scripts.pipeline_stages.local: the local stage mines a source's index with its graph scope and
settings, sets aside a source whose downloads were updated, and starts local servers without delay."""

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

import rdfsolve
from rdfsolve.qlever import downloads
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.local import LocalMiningStage, local_graph_scope

URL = "https://example.org/data.ttl.gz"


def _stage(tmp_path, update):
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(
        base_dir=tmp_path,
        repo_dir=tmp_path,
        sources_file=registry,
        output_dir=tmp_path / "run",
        update_downloads=update,
    )
    workdir = tmp_path / "fixture"
    (workdir / "rdf").mkdir(parents=True)
    (workdir / "rdf" / "data.ttl").write_text("<urn:a> <urn:p> <urn:b> .")
    (workdir / "fixture.meta-data.json").write_text("{}")
    return (
        LocalMiningStage(config),
        workdir,
        Source.from_dict({"name": "fixture", "download_ttl": [URL]}),
    )


def _folders(tmp_path):
    return sorted(
        p.name.split("-update-")[0] for p in tmp_path.iterdir() if p.is_dir() and p.name != "run"
    )


def test_a_source_with_an_update_is_set_aside_and_made_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(downloads, "updated_urls", lambda urls, record, built_at: [URL])
    stage, workdir, source = _stage(tmp_path, update=True)
    stage._set_aside_when_updated(workdir, source)
    assert _folders(tmp_path) == ["fixture", "fixture.before"]
    assert not any(workdir.iterdir()), "The source is downloaded and indexed again"
    kept = next(p for p in tmp_path.iterdir() if ".before-update-" in p.name)
    assert (kept / "rdf" / "data.ttl").exists(), "The old folder is kept"


def test_a_source_without_an_update_keeps_its_folder(tmp_path, monkeypatch):
    monkeypatch.setattr(downloads, "updated_urls", lambda urls, record, built_at: [])
    stage, workdir, source = _stage(tmp_path, update=True)
    stage._set_aside_when_updated(workdir, source)
    assert _folders(tmp_path) == ["fixture"] and (workdir / "rdf" / "data.ttl").exists()


def test_a_changed_registry_entry_is_an_update(tmp_path, monkeypatch):
    def not_asked(urls, record, built_at):
        raise AssertionError("The server is not asked when the entry changed")

    monkeypatch.setattr(downloads, "updated_urls", not_asked)
    stage, workdir, source = _stage(tmp_path, update=True)
    downloads.write_record(workdir, [URL, "https://example.org/dropped.ttl"], lambda url: None)
    stage._set_aside_when_updated(workdir, source)
    assert _folders(tmp_path) == ["fixture", "fixture.before"]


def test_without_the_option_the_server_is_not_asked(tmp_path, monkeypatch):
    def not_asked(urls, record, built_at):
        raise AssertionError("A run without the option asks nothing of the server")

    monkeypatch.setattr(downloads, "updated_urls", not_asked)
    stage, workdir, source = _stage(tmp_path, update=False)
    stage._set_aside_when_updated(workdir, source)
    assert _folders(tmp_path) == ["fixture"]


def test_the_miner_of_a_local_server_does_not_wait(tmp_path, monkeypatch):
    seen = {}

    def miner(**options):
        seen.update(options)
        return object()

    monkeypatch.setattr(rdfsolve, "SchemaMiner", miner)
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(
        base_dir=tmp_path, repo_dir=tmp_path, sources_file=registry, output_dir=tmp_path / "run"
    )
    assert config.delay > 0, "Public endpoints keep their wait"
    LocalMiningStage(config)._local_miner(7000, None, tmp_path / "report.json")
    assert seen["endpoint_url"] == "http://localhost:7000" and seen["delay"] == 0


SCOPE = ["http://rdfportal.org/dataset/medgen"]


def test_the_scope_of_the_endpoint_is_not_applied_to_files_without_graphs():
    assert local_graph_scope(SCOPE, {"download_nt": ["https://example.org/a.nt.gz"]}, {}) is None
    fields = {
        "download_ttl": ["https://example.org/a.ttl"],
        "download_owl": "https://example.org/o.owl",
    }
    assert local_graph_scope(SCOPE, fields, {}) is None


def test_the_scope_is_kept_where_the_files_can_hold_graphs():
    assert local_graph_scope(SCOPE, {"download_nq": ["https://example.org/a.nq.gz"]}, {}) == SCOPE
    assert local_graph_scope(SCOPE, {"download_tgz": "https://example.org/a.tgz"}, {}) == SCOPE
    mapped = {"urn:g": {"download_ttl": ["https://example.org/a.ttl"]}}
    assert local_graph_scope(["urn:g"], {}, mapped) == ["urn:g"]
    assert local_graph_scope(SCOPE, {}, {}) == SCOPE, (
        "No files are known: the miner checks the scope"
    )
    assert local_graph_scope(None, {"download_nt": ["https://example.org/a.nt"]}, {}) is None


@pytest.fixture
def pipeline():
    import subprocess
    from importlib import import_module
    from types import SimpleNamespace

    scripts = str(Path(__file__).resolve().parents[2] / "scripts")
    sys.path.insert(0, scripts)
    modules = [
        import_module("pipeline_stages." + name)
        for name in ("config", "base", "remote", "local", "grouped", "cloud", "cli")
    ]
    return SimpleNamespace(
        subprocess=subprocess,
        **{
            name: getattr(module, name)
            for module in modules
            for name in (
                "Source",
                "SourceMode",
                "PipelineConfig",
                "Stage",
                "RemoteMiningStage",
                "LocalMiningStage",
                "GroupedMiningStage",
                "LsLodCloudStage",
                "Pipeline",
                "preflight",
            )
            if hasattr(module, name)
        },
    )


def test_grouped_mining_mines_all_graphs_once_and_splits_per_dataset(
    pipeline, tmp_path, monkeypatch
):
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    one, two = ("urn:provider:one", "urn:provider:two")
    config = pipeline.PipelineConfig(base_dir=tmp_path)
    stage = pipeline.GroupedMiningStage(config)
    sources = [
        pipeline.Source.from_dict(
            {
                "name": "one",
                "graph_uris": one,
                "type_context_graph_uris": "urn:types",
                "ontology_graph_uris": "urn:ontology",
            }
        ),
        pipeline.Source(name="two", graph_uris=[two]),
    ]
    miners = []
    miner = Mock(declared_classes=frozenset(), subsumed_classes=frozenset())
    miner.count_class_entities.return_value = ({"urn:A": 2}, {"urn:A": "complete"})
    miner.last_report.completion_state = "complete"

    def local_miner(
        port, graph_uris, report_path, *, type_context_graph_uris, resume_checkpoint=None
    ):
        assert type_context_graph_uris == ["urn:types"]
        miners.append(graph_uris)
        return miner

    group = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri="urn:p",
                object_class="urn:B",
                count=3,
                graphs={one: 3},
            ),
            SchemaPattern(
                subject_class="urn:B",
                property_uri="urn:q",
                object_class="Literal",
                count=1,
                graphs={two: 1},
            ),
        ],
        about=AboutMetadata(dataset_name="provider", graph_uris=[one, two]),
    )
    saved = {}
    monkeypatch.setattr(stage, "_local_miner", local_miner)

    def mine_schema(miner, name, output_dir, *, ontology_graph_uris):
        assert ontology_graph_uris == ["urn:ontology"]
        return group

    monkeypatch.setattr(stage, "_mine_schema", mine_schema)
    monkeypatch.setattr(stage, "_save_schema_outputs", Mock())
    monkeypatch.setattr(
        stage,
        "_save_dataset_outputs",
        lambda source, part, output_dir, helper, context: saved.setdefault(
            source.name, (part, context)
        ),
    )
    assert stage._mine_grouped("provider", sources, 7019) == ["one", "two"]
    assert miners == [[one, two]]
    part, context = saved["one"]
    assert context == "grouped_local_distribution"
    assert [(p.subject_class, p.object_class, p.count) for p in part.patterns] == [
        ("urn:A", "urn:B", 3)
    ]
    assert part.about.dataset_name == "one" and part.about.graph_uris == [one]
    assert part.about.class_entity_counts == {"urn:A": 2}
    miner.count_class_entities.assert_any_call(["urn:A", "urn:B"], [one])
    assert [p.property_uri for p in saved["two"][0].patterns] == ["urn:q"]


class _Server:
    """A server process that is running until *dies* is set."""

    def __init__(self, pid):
        self.pid, self.status = pid, None

    def poll(self):
        return self.status


def _restart_stage(tmp_path, monkeypatch, *, restarts=1):
    stage, workdir, source = _stage(tmp_path, update=False)
    stage.config.qlever_restarts = restarts
    started, stopped, mined = [], [], []

    def start(workdir, name, port, *, port_wait=0):
        server = _Server(len(started) + 1)
        started.append((port, port_wait))
        stage._servers[server.pid] = server
        return server.pid

    monkeypatch.setattr(stage, "_qlever_start", start)
    monkeypatch.setattr(
        stage, "_qlever_stop", lambda pid: stopped.append(stage._servers.pop(pid, None) and pid)
    )
    return stage, workdir, source, started, stopped, mined


def test_a_dead_server_is_started_again_and_the_source_resumes_from_its_checkpoint(
    tmp_path, monkeypatch
):
    stage, workdir, source, started, stopped, mined = _restart_stage(tmp_path, monkeypatch)
    pid = stage._qlever_start(workdir, source.name, 7000)
    suffix = stage.config.output_suffix
    checkpoint = tmp_path / "run" / "fixture" / f"fixture{suffix}_report.checkpoint.jsonl"

    def mine(source, port, *, resume_checkpoint=None):
        mined.append(resume_checkpoint)
        if len(mined) == 1:
            checkpoint.parent.mkdir(parents=True)
            checkpoint.write_text('{"phase": "census"}\n')
            stage._servers[pid].status = -11  # the server died during a query
            raise RuntimeError("Endpoint unreachable")

    monkeypatch.setattr(stage, "_mine_local", mine)
    stage._mine_with_restarts(source, workdir, 7000, pid)
    assert mined == [None, checkpoint], "The second run resumes from the first run's checkpoint"
    assert started == [(7000, 0), (7000, 300)], "Same port, waited for while it is released"
    assert stopped == [1, 2] and not stage._servers


def test_an_error_while_the_server_runs_is_not_retried(tmp_path, monkeypatch):
    stage, workdir, source, started, stopped, mined = _restart_stage(tmp_path, monkeypatch)
    pid = stage._qlever_start(workdir, source.name, 7000)

    def mine(source, port, *, resume_checkpoint=None):
        mined.append(resume_checkpoint)
        raise ValueError("a mining error")

    monkeypatch.setattr(stage, "_mine_local", mine)
    with pytest.raises(ValueError, match="a mining error"):
        stage._mine_with_restarts(source, workdir, 7000, pid)
    assert len(mined) == 1 and len(started) == 1 and stopped == [1]


def test_without_restarts_left_the_error_names_how_the_server_ended(tmp_path, monkeypatch):
    stage, workdir, source, started, stopped, _ = _restart_stage(tmp_path, monkeypatch, restarts=0)
    pid = stage._qlever_start(workdir, source.name, 7000)

    def mine(source, port, *, resume_checkpoint=None):
        stage._servers[pid].status = 137
        raise RuntimeError("Endpoint unreachable")

    monkeypatch.setattr(stage, "_mine_local", mine)
    with pytest.raises(RuntimeError, match=r"exited with status 137 \(SIGKILL\)"):
        stage._mine_with_restarts(source, workdir, 7000, pid)
    assert len(started) == 1 and stopped == [1]


def test_the_inputs_of_an_index_are_carried_into_the_run_and_held_to_their_pins(tmp_path):
    import json

    stage, workdir, source = _stage(tmp_path, update=False)
    stage._carry_inputs(workdir, source)
    carried = tmp_path / "run" / "fixture" / "fixture_inputs.json"
    pinned = json.loads(carried.read_text())
    assert [item["path"] for item in pinned["files"]] == ["rdf/data.ttl"]
    assert pinned["recorded"] == "after_index", "An index built before inputs were pinned"
    stage.config.pinned_inputs = tmp_path / "run"
    stage._carry_inputs(workdir, source)
    (workdir / "rdf" / "data.ttl").write_text("<urn:a> <urn:p> <urn:c> .")
    (workdir / downloads.INPUTS).unlink()
    with pytest.raises(ValueError, match="not those pinned"):
        stage._carry_inputs(workdir, source)
    stage.config.pinned_inputs = tmp_path / "elsewhere"
    with pytest.raises(ValueError, match="no pinned inputs"):
        stage._carry_inputs(workdir, source)


def test_a_folder_of_other_downloads_is_set_aside_without_the_update_option(tmp_path):
    """L19: an index built from earlier downloads was mined after the registry entry changed."""
    stage, workdir, source = _stage(tmp_path, update=False)
    downloads.write_record(workdir, ["https://example.org/old.ttl.gz"], lambda url: None)
    stage._set_aside_when_differs(workdir, source)
    assert sorted(
        p.name.split("-2")[0] for p in tmp_path.iterdir() if p.is_dir() and p.name != "run"
    ) == [
        "fixture",
        "fixture.set-aside",
    ]
    assert not any(workdir.iterdir()), "The source is downloaded and indexed again"
    kept = next(p for p in tmp_path.iterdir() if ".set-aside-" in p.name)
    assert "other downloads" in (kept / "SET-ASIDE.txt").read_text()


def test_a_folder_of_other_downloads_is_refused_when_the_run_cannot_rebuild_it(tmp_path):
    stage, workdir, source = _stage(tmp_path, update=False)
    (workdir / "rdf" / "dropped.ttl").write_text("<urn:a> <urn:p> <urn:c> .")
    downloads.write_record(workdir, [URL], lambda url: None)
    stage.config.no_download = True
    with pytest.raises(ValueError, match="1 index inputs come from no download"):
        stage._set_aside_when_differs(workdir, source)
    assert _folders(tmp_path) == ["fixture"], "Nothing is moved"


def test_a_folder_of_the_listed_downloads_is_kept(tmp_path):
    stage, workdir, source = _stage(tmp_path, update=False)
    downloads.write_record(workdir, [URL], lambda url: None)
    stage._set_aside_when_differs(workdir, source)
    assert _folders(tmp_path) == ["fixture"] and (workdir / "rdf" / "data.ttl").exists()


def test_the_files_an_entry_leaves_out_reach_the_qleverfile(tmp_path):
    """archive_members_left_out passes the pipeline's boundary to the Qleverfile."""
    from rdfsolve.qlever import QleverConfig, build_qleverfile

    source = Source.from_dict(
        {
            "name": "dump",
            "download_tgz": "https://example.org/dump_ttl.tar.gz",
            "archive_members_left_out": ["queries.ttl"],
        }
    )
    qleverfile = build_qleverfile(
        source.qlever_entry(),
        tmp_path,
        7019,
        runtime="singularity",
        cfg=QleverConfig(),
        workdir=tmp_path / "dump",
    )
    assert "for f in queries.ttl;" in qleverfile and "left_out/" in qleverfile


CLINVAR = (
    "_:b <http://www.w3.org/2000/01/rdf-schema#seeAlso> "
    "<http://ncbi.nlm.nih.gov/snp/rsc.2899A>C> .\n"
)
TRUNCATED = (
    "2026-10-06 11:32:53.175 - INFO: Parsing of line has Failed, but parseInput is not yet "
    "exhausted. Remaining bytes: 128,436,885\n"
)


def _built(tmp_path, logs, monkeypatch):
    """A local source whose qlever-index writes the given logs, one per run."""
    import gzip
    import json

    config = PipelineConfig(base_dir=tmp_path, data_dir=tmp_path, no_download=True)
    source = Source.from_dict({"name": "fixture", "download_nt": ["https://example.org/d.nt.gz"]})
    workdir = tmp_path / "qlever_workdirs" / "fixture"
    (workdir / "rdf").mkdir(parents=True)
    data = workdir / "rdf" / "d.nt.gz"
    xsd = "http://www.w3.org/2001/XMLSchema#"
    with gzip.open(data, "wt") as stream:
        stream.write(f'<urn:a> <urn:n> "1"^^<{xsd}integer> .\n' + CLINVAR + "<urn:b> <urn:p> <urn:c> .\n")
    stage = LocalMiningStage(config)
    stage._prepare_qleverfile(workdir, source, 7020)
    runs = []

    def index(cmd, **options):
        runs.append(cmd)
        (workdir / "fixture.meta-data.json").write_text(json.dumps({"num-triples": {"normal": 3}}))
        (workdir / "fixture.index-log.txt").write_text(logs[len(runs) - 1])

    monkeypatch.setattr("scripts.pipeline_stages.local.subprocess.run", index)
    return stage, workdir, source, data, runs


def test_an_index_of_inputs_that_qlever_stopped_reading_is_repaired_and_built_again(
    tmp_path, monkeypatch
):
    import gzip
    import json

    from rdfsolve.qlever.datatypes import CENSUS_FILE
    from rdfsolve.qlever.repair import REPAIRS_FILE

    stage, workdir, source, data, runs = _built(tmp_path, [TRUNCATED, "done\n"], monkeypatch)
    stage._execute_qleverfile(workdir, source)
    assert len(runs) == 2, "qlever-index succeeds on a truncated input; the build is not kept"
    with gzip.open(data, "rt") as stream:
        assert "rsc.2899A%3EC>" in stream.read()
    repairs = json.loads((workdir / REPAIRS_FILE).read_text())
    assert repairs["lines"][0]["line"] == 2 and repairs["lines"][0]["changes"] == ["iri"]
    census = json.loads((workdir / CENSUS_FILE).read_text())
    assert census["properties"] == {"urn:n": {"http://www.w3.org/2001/XMLSchema#integer": 1}}
    assert census["unread"]["lines"] == 1, "The census of the published files records the line"
    assert census["unread"]["sample"][0]["line"] == 2


def test_an_index_that_stays_truncated_fails_and_is_set_aside_on_the_next_run(
    tmp_path, monkeypatch
):
    from rdfsolve.qlever.index_check import TruncatedIndexError, has_cached_index

    stage, workdir, source, data, runs = _built(tmp_path, [TRUNCATED, TRUNCATED], monkeypatch)
    with pytest.raises(TruncatedIndexError, match="128,436,885 bytes not parsed"):
        stage._execute_qleverfile(workdir, source)
    assert len(runs) == 2
    for permutation in ("pso", "pos"):
        for suffix in ("", ".meta"):
            (workdir / f"fixture.index.{permutation}{suffix}").write_text("x")
    (workdir / "fixture.meta-data.json").write_text(
        '{"num-subjects": {"normal": 1, "internal": 0}, "num-predicates": {"normal": 1, '
        '"internal": 0}, "num-objects": {"normal": 1, "internal": 0}, "num-triples": '
        '{"normal": 1, "internal": 0}, "has-all-permutations": false, "index-format-version": '
        '{}, "vocabulary-type": "x"}'
    )
    with pytest.raises(TruncatedIndexError):
        has_cached_index(workdir, "fixture")
    assert stage._has_usable_index(workdir, source) is False
    (kept,) = workdir.glob("set-aside-index-*")
    assert sorted(p.name for p in kept.iterdir()) == [
        "SET-ASIDE.txt",
        "fixture.index-log.txt",
        "fixture.index.pos",
        "fixture.index.pos.meta",
        "fixture.index.pso",
        "fixture.index.pso.meta",
        "fixture.meta-data.json",
    ]
    assert data.is_file() and (workdir / "fixture.settings.json").is_file(), "Inputs are kept"
    assert has_cached_index(workdir, "fixture") is False
