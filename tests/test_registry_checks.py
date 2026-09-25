import csv
import sys

import pytest
import yaml

from rdfsolve.registry_checks import check_registry
from scripts.check_registry import main


def test_registry_report_identifies_bad_inputs_without_writing_them(tmp_path, monkeypatch):
    sources = tmp_path / "sources.yaml"
    sources.write_text(
        yaml.safe_dump(
            [
                {
                    "name": "demo.a",
                    "endpoint": "HTTPS://EXAMPLE.ORG/sparql/",
                    "graph_uris": [],
                    "download_nquads": "https://example.org/data.nq",
                    "last_checked": "yesterday",
                },
                {"name": "demo.b", "endpoint": "https://example.org/sparql", "graph_uris": []},
                {
                    "name": "demo.c",
                    "endpoint": "https://example.org/sparql",
                    "graph_uris": ["urn:g"],
                    "local_provider": "demo",
                },
                {"name": "broken", "counts": "maybe"},
            ]
        )
    )
    overrides = tmp_path / "identity_overrides.yaml"
    overrides.write_text("- left: demo.a\n  right: absent\n  relation: same_dataset\n")
    original = (sources.read_bytes(), overrides.read_bytes())
    findings = check_registry(sources, overrides)
    errors = {(r["check_id"], r["sources"]) for r in findings if r["severity"] == "error"}
    assert errors == {
        ("A1", "broken"),
        ("A2", "demo.a"),
        ("A6", "demo.a"),
        ("B1", "demo.a;demo.b"),
        ("B2", "demo.a;demo.b;demo.c"),
        ("D3", ""),
    }, findings
    assert any(r["check_id"] == "B7" and "not checked" in r["detail"] for r in findings), (
        "Grouped input scope must stay unverified"
    )
    output = tmp_path / "report.tsv"
    monkeypatch.setattr(
        sys,
        "argv",
        ["check_registry", str(sources), "--overrides", str(overrides), "--output", str(output)],
    )
    assert main() == 1, "Registry errors block the freeze"
    with output.open() as stream:
        assert list(csv.DictReader(stream, delimiter="\t")) == findings, "Saved findings"
    with pytest.raises(FileExistsError):
        main()
    assert (sources.read_bytes(), overrides.read_bytes()) == original, (
        "Registry inputs are read-only"
    )
    clean = tmp_path / "clean.yaml"
    clean.write_text(
        "- name: demo.clean\n  endpoint: https://example.org/sparql\n  graph_uris: [urn:g]\n"
        "- name: demo.service\n  endpoint: https://example.org/sparql\n  source_role: service\n  graph_uris: []\n"
    )
    assert [r for r in check_registry(clean) if r["check_id"] != "A5"] == [
        {
            "check_id": "D3",
            "sources": "",
            "endpoint": "",
            "severity": "info",
            "detail": "0 resolver candidates; 0 unreviewed upstream pairs; 0 invalid rows; canonical count=1",
        }
    ], "Clean registry"

    held = tmp_path / "held.yaml"
    held.write_text(
        "- name: release.one\n  endpoint: https://example.org/sparql\n"
        "  skip_mining: true\n  graph_uris: [urn:unknown]\n"
        "- name: release.two\n  endpoint: https://example.org/sparql\n"
        "  skip_mining: true\n  graph_uris: [urn:unknown]\n"
    )
    held_findings = check_registry(held)
    assert not any(r["severity"] == "error" for r in held_findings), held_findings
    assert any(r["check_id"] == "B1" for r in held_findings), "Held conflicts stay visible"
    assert any("canonical count=None" in r["detail"] for r in held_findings)
