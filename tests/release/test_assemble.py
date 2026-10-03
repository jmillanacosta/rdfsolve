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
