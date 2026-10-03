from pathlib import Path

import yaml
from scripts.article_pilot import build_pilot_registries


def test_pilot_keeps_remote_and_local_access_evidence_separate(tmp_path: Path):
    sources = tmp_path / "sources.yaml"
    sources.write_text(
        yaml.safe_dump(
            [
                {
                    "name": "both",
                    "endpoint": "https://example.org/sparql",
                    "download_ttl": "https://example.org/data.ttl",
                    "download_owl": "https://example.org/schema.owl",
                },
                {"name": "remote", "endpoint": "https://remote.example/sparql"},
            ]
        ),
        encoding="utf-8",
    )
    spec = tmp_path / "pilot.yaml"
    spec.write_text(
        yaml.safe_dump(
            {
                "remote": [
                    {"name": "both", "rationale": "remote evidence"},
                    {"name": "remote", "rationale": "endpoint"},
                ],
                "local": [{"name": "both", "rationale": "distribution evidence"}],
            }
        ),
        encoding="utf-8",
    )
    out = tmp_path / "pilot"
    manifest = build_pilot_registries(sources, spec, out)
    remote = yaml.safe_load((out / "remote" / "sources.yaml").read_text())
    local = yaml.safe_load((out / "local" / "sources.yaml").read_text())
    assert manifest["modes"]["remote"]["source_count"] == 2
    assert manifest["modes"]["local"]["source_count"] == 1
    assert remote[0]["download_owl"] == "https://example.org/schema.owl"
    assert local[0]["download_owl"] == "https://example.org/schema.owl"
    assert remote[0]["endpoint"] == local[0]["endpoint"]
    assert (out / "remote" / "identity_overrides.yaml").exists()
    assert (out / "local" / "identity_overrides.yaml").exists()
