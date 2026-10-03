"""The link analysis reads the local release and the remote run together and takes one schema for
each dataset, the local one when a dataset has both, as the link stage does: a link from a source
mined only remotely has its endpoints in the loaded schemas (rehearsal attempt 11: the endpoint
('rhea', owl:Class) was absent from the local release)."""

import json

import pytest
import yaml
from rdflib import Graph

from rdfsolve.analysis.io import load_channel_schemas
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.release.build import build_release_manifest, write_release_manifest


def release(root, extractions):
    registry = [{"name": name, "endpoint": "https://example.org/sparql"} for name, _ in extractions]
    root.mkdir()
    (root / "sources.yaml").write_text(yaml.safe_dump(registry))
    for name, mode in extractions:
        directory = root / name
        directory.mkdir(exist_ok=True)
        data = Graph().parse(data=f'<urn:s> a <urn:C>; <urn:{mode}> "x" .', format="turtle")
        with SchemaMiner.from_graph(data, delay=0, report_path=directory / f"{name}_{mode}_report.json") as miner:
            schema = miner.mine(name)
        (directory / f"{name}_{mode}_schema.json").write_text(json.dumps(schema.to_dict()))
    write_release_manifest(build_release_manifest(root), root)
    return root


def test_one_schema_per_dataset_the_local_one_first(tmp_path):
    local = release(tmp_path / "local", [("both", "local")])
    remote = release(tmp_path / "remote", [("both", "remote"), ("rhea", "remote")])
    schemas = load_channel_schemas([remote, local])
    assert sorted(schemas) == ["both", "rhea"]
    assert any(p.property_uri == "urn:local" for p in schemas["both"].patterns), "Local first"
    assert any(p.property_uri == "urn:remote" for p in schemas["rhea"].patterns)
    other = release(tmp_path / "other", [("both", "local")])
    with pytest.raises(ValueError, match="one extraction"):
        load_channel_schemas([local, other])
