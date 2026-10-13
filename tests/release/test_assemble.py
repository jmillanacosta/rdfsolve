"""rdfsolve.release.assemble: a release is assembled from its parts."""

import json
import pytest
import yaml
from rdfsolve.release.assemble import assemble_runs


def run(root, name, datasets, commit="abc123", state="complete"):
    path = root / name
    path.mkdir()
    (path / "code_commit.txt").write_text(commit + "\n")
    (path / "sources.yaml").write_text("- name: a\n- name: b\n- name: c\n")
    (path / "pipeline_config.yaml").write_text(
        yaml.safe_dump({"output_dir": str(path), "selected_sources": datasets, "chunk_size": 100,
                        "base_port": 7000 + int(name[-1])})  # set per job
    )
    results = {"local_mining": {"mined": datasets, "partial": [], "failed": [], "state": state}}
    (path / "pipeline_results_local.json").write_text(json.dumps(results))
    for dataset in datasets:
        (path / dataset).mkdir()
        (path / dataset / f"{dataset}_local_schema.json").write_text("{}")
    return path


def test_chunks_are_joined_with_the_run_of_each_dataset(tmp_path):
    first = run(tmp_path, "chunk-1", ["a", "b"])
    second = run(tmp_path, "chunk-2", ["c"], state="partial")
    out = assemble_runs([first, second], tmp_path / "release")
    assert {p.name for p in out.iterdir() if p.is_dir()} == {"a", "b", "c"}
    results = json.loads((out / "pipeline_results_local.json").read_text())["local_mining"]
    assert (sorted(results["mined"]), results["state"]) == (["a", "b", "c"], "partial")
    record = json.loads((out / "assembly.json").read_text())
    assert record["dataset_runs"] == {"a": "chunk-1", "b": "chunk-1", "c": "chunk-2"}
    assert record["code_commit"] == "abc123"
    assert yaml.safe_load((out / "pipeline_config.yaml").read_text())["selected_sources"] == ["a", "b", "c"]


def test_runs_of_different_code_or_the_same_dataset_are_refused(tmp_path):
    first = run(tmp_path, "chunk-1", ["a"])
    with pytest.raises(ValueError, match="code commit"):
        assemble_runs([first, run(tmp_path, "chunk-2", ["b"], commit="other")], tmp_path / "x")
    with pytest.raises(ValueError, match="in more than one run"):
        assemble_runs([first, run(tmp_path, "chunk-3", ["a"])], tmp_path / "y")


def test_datasets_of_runs_of_different_code_are_joined_with_their_commits(tmp_path):
    from rdflib import Graph, URIRef
    from rdflib.namespace import PROV

    from rdfsolve.release.assemble import assemble_selected
    from rdfsolve.release.build import build_release_manifest
    from rdfsolve.release.rdf import release_to_rdf

    first = run(tmp_path, "chunk-1", ["a", "b"], commit="old1")
    second = run(tmp_path, "chunk-2", ["c"], commit="old2")
    registry = tmp_path / "sources.yaml"
    registry.write_text("- name: a\n- name: b\n- name: c\n")
    rewrites = tmp_path / "rewrites" / "a"
    rewrites.mkdir(parents=True)
    (rewrites / "a_local_schema.json").write_text('{"rewritten": true}')
    out = assemble_selected(
        {"a": first / "a", "b": first / "b", "c": second / "c"},
        tmp_path / "release",
        {"sources.yaml": registry},
        rewritten={"a": rewrites},
        rewritten_by_commit="new1",
        code_commit="new1",
    )
    assert json.loads((out / "a" / "a_local_schema.json").read_text()) == {"rewritten": True}
    assert (first / "a" / "a_local_schema.json").read_text() == "{}", "Runs are not changed"
    record = json.loads((out / "assembly.json").read_text())["datasets"]
    assert {d: r["code_commit"] for d, r in record.items()} == {"a": "old1", "b": "old1", "c": "old2"}
    assert record["a"]["rewritten_files"] == ["a_local_schema.json"]
    with pytest.raises(FileExistsError):
        assemble_selected({"a": first / "a"}, out, {"sources.yaml": registry})

    manifest = build_release_manifest(out, release_id="r1")
    by_id = {d.dataset_id: d for d in manifest.datasets}
    assert (manifest.code_commit, by_id["c"].code_commit) == ("new1", "old2")
    assert by_id["a"].rewritten_by_commit == "new1" and by_id["b"].rewritten_files == []
    graph = release_to_rdf(manifest)
    used = {str(o) for o in graph.objects(None, PROV.used)}
    assert {f"https://github.com/jmillanacosta/rdfsolve/commit/{c}" for c in ("old1", "old2", "new1")} <= used
