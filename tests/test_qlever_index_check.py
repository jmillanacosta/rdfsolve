"""Index identity covers immutable files."""

import json

from rdfsolve.qlever.index_check import index_artifact_files


def test_index_inventory_ignores_runtime_logs(tmp_path):
    name = "fixture"
    metadata = {
        "num-subjects": {"normal": 1, "internal": 0},
        "num-predicates": {"normal": 1, "internal": 0},
        "num-objects": {"normal": 1, "internal": 0},
        "num-triples": {"normal": 1, "internal": 0},
        "has-all-permutations": True,
        "index-format-version": {},
        "vocabulary-type": "compressed",
    }
    expected = {name + ".meta-data.json"}
    (tmp_path / (name + ".meta-data.json")).write_text(json.dumps(metadata))
    for suffix in [
        "index." + permutation + ending
        for permutation in ("spo", "sop", "osp", "ops", "pso", "pos")
        for ending in ("", ".meta")
    ] + ["index.patterns", "internal.index.pos", "vocabulary.words.internal"]:
        expected.add(name + "." + suffix)
        (tmp_path / (name + "." + suffix)).write_bytes(b"index")
    logs = ["metrics-log.jsonl", "server.resource-usage-log.tsv",
            "index.resource-usage-log.tsv", "index.log"]
    for suffix in logs:
        (tmp_path / (name + "." + suffix)).write_text("before")
    (tmp_path / (name + ".settings.json")).write_text("{}")
    found = index_artifact_files(tmp_path, name)
    assert {p.name for p in found} == expected, "Inventory includes logs or omits index data"
    before = {p.name: p.read_bytes() for p in found}
    for suffix in logs:
        (tmp_path / (name + "." + suffix)).write_text("after a server run")
    assert {p.name: p.read_bytes() for p in index_artifact_files(tmp_path, name)} == before
    target = tmp_path / (name + ".index.spo")
    target.write_bytes(b"changed")
    assert {p.name: p.read_bytes() for p in index_artifact_files(tmp_path, name)} != before
    target.unlink()
    import pytest
    with pytest.raises(ValueError, match="Incomplete index"):
        index_artifact_files(tmp_path, name)
