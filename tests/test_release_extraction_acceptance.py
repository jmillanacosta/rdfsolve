import json

import pytest
import yaml
from rdflib import Graph

from rdfsolve.analysis.io import load_schemas
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.release.build import build_release_manifest, write_release_manifest


def test_release_selects_verified_extraction(tmp_path):
    registry = [{"name": "fixture", "endpoint": "https://example.org/sparql"}]
    (tmp_path / "sources.yaml").write_text(yaml.safe_dump(registry))
    directory = tmp_path / "fixture"
    directory.mkdir()
    snapshots = {}
    for mode, value in [("local", "local"), ("remote", "remote")]:
        data = Graph().parse(data=f'<urn:s> a <urn:C>; <urn:{value}> "x" .', format="turtle")
        with SchemaMiner.from_graph(data, delay=0, report_path=directory / f"fixture_{mode}_report.json") as miner:
            schema = miner.mine("fixture")
        (directory / f"fixture_{mode}_schema.json").write_text(json.dumps(schema.to_dict()))
        snapshots[mode] = schema.about.snapshot_id
    manifest = build_release_manifest(tmp_path)
    record = manifest.datasets[0]
    assert {r.mode: r.snapshot_id for r in record.extractions} == snapshots
    assert all(r.schema_path for r in record.extractions)
    assert record.snapshot_id is None, "Dataset record selected an arbitrary extraction"
    write_release_manifest(manifest, tmp_path)
    with pytest.raises(ValueError, match="extraction"):
        load_schemas(tmp_path)
    chosen = load_schemas(tmp_path, extraction_mode="local")
    assert chosen["fixture"].about.snapshot_id == snapshots["local"]
    assert any(p.property_uri == "urn:local" for p in chosen["fixture"].patterns)
    stray = directory / "ignored_schema.json"
    stray.write_text("invalid json")
    assert load_schemas(tmp_path, extraction_mode="local")["fixture"].patterns == chosen["fixture"].patterns
    path = directory / "fixture_local_schema.json"
    raw = json.loads(path.read_text())
    path.write_text(json.dumps(raw, indent=4))
    with pytest.raises(ValueError, match="hash|digest"):
        load_schemas(tmp_path, extraction_mode="local")
