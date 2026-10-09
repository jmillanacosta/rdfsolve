#!/usr/bin/env python
"""Synchronise data/sources.yaml and its metadata sidecar with KG-Registry (rdfsolve.kg_registry).

The KG-Registry file is read at one commit of its repository (the latest that changed
registry/kgs.yml, unless --commit is given), which the sidecar records. The review report lists
every match, addition and kept curated value.

usage: sync_kg_registry.py [--commit SHA] [--registry data/sources.yaml] [--out DIR] REPORT_TSV
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path

import requests
import yaml

from rdfsolve.kg_registry import patch_registry, sync

REPOSITORY = "Knowledge-Graph-Hub/kg-registry"


def latest_commit() -> str:
    """Return the latest commit of KG-Registry that changed registry/kgs.yml."""
    url = f"https://api.github.com/repos/{REPOSITORY}/commits"
    answer = requests.get(url, params={"path": "registry/kgs.yml", "per_page": 1}, timeout=60)
    answer.raise_for_status()
    return str(answer.json()[0]["sha"])


def main() -> None:
    """Run the synchronisation and write the registry, the sidecar and the report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--commit")
    parser.add_argument("--registry", type=Path, default=Path("data/sources.yaml"))
    parser.add_argument("--out", type=Path, help="write here instead of over the registry")
    args = parser.parse_args()
    commit = args.commit or latest_commit()
    url = f"https://raw.githubusercontent.com/{REPOSITORY}/{commit}/registry/kgs.yml"
    answer = requests.get(url, timeout=300)
    answer.raise_for_status()
    resources = yaml.safe_load(answer.text)["resources"]
    text = args.registry.read_text(encoding="utf-8")
    sidecar_path = args.registry.with_suffix(".metadata.json")
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    entries, document, report = sync(
        yaml.safe_load(text),
        sidecar,
        resources,
        commit=commit,
        retrieved_at=datetime.now(UTC).date().isoformat(),
    )
    out = args.out or args.registry.parent
    out.mkdir(parents=True, exist_ok=True)
    (out / args.registry.name).write_text(patch_registry(text, entries), encoding="utf-8")
    (out / sidecar_path.name).write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    with args.report.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=["name", "kg_registry_id", "action", "detail"], delimiter="\t"
        )
        writer.writeheader()
        writer.writerows(report)
    print(f"KG-Registry {commit}: {len(resources)} resources; {len(report)} report rows")


if __name__ == "__main__":
    main()
