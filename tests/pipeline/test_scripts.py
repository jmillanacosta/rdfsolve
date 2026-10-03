"""scripts: the article pilot, the endpoint check and the census of literal datatypes."""

import json
from pathlib import Path
from unittest.mock import Mock

import yaml

from rdfsolve.endpoint_health import EndpointHealthCheck
from scripts import check_endpoints
from scripts.article_pilot import build_pilot_registries
from scripts.literal_datatypes import download_urls


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


def test_endpoint_check_writes_status_without_changing_sources(tmp_path, monkeypatch):
    path = tmp_path / "sources.yaml"
    original = "- name: demo\n  endpoint: https://example.org/sparql\n"
    path.write_text(original)
    output = tmp_path / "status.json"
    monkeypatch.setattr(
        "sys.argv", ["check_endpoints", "--sources", str(path), "--output", str(output)]
    )
    health = Mock(
        return_value=EndpointHealthCheck(
            "https://example.org/sparql", "rate_limited", 0.125, "HTTP 429", "2026-09-22"
        )
    )
    monkeypatch.setattr(check_endpoints, "check_endpoint_health", health)
    check_endpoints.main()
    assert json.loads(output.read_text())["endpoints"] == {
        "demo": {
            "endpoint": "https://example.org/sparql",
            "hostname": "example.org",
            "status": "rate_limited",
            "response_time_ms": 125,
            "error": "HTTP 429",
        }
    }, "Pipeline status report"
    assert path.read_text() == original, "Source specification"
    path.write_text("[]\n")
    check_endpoints.main()
    assert json.loads(output.read_text())["endpoints"] == {}, "Empty source set"
    health.assert_called_once_with("https://example.org/sparql")


def test_download_urls_are_read_from_strings_and_lists():
    entry = {
        "name": "x",
        "download_zip": "https://a/x.zip",
        "download_owl": ["https://a/b.owl", "https://a/c.owl"],
    }
    assert download_urls(entry) == ["https://a/x.zip", "https://a/b.owl", "https://a/c.owl"]
    assert download_urls({"name": "y"}) == []
