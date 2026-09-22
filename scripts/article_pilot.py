#!/usr/bin/env python
"""Sample analysis."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml


def _load_registry(path: Path) -> tuple[list[dict[str, Any]], bool]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    wrapped = isinstance(raw, dict) and "sources" in raw
    rows = raw.get("sources", []) if wrapped else raw
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"Expected a source-registry list in {path}")
    return rows, wrapped


def _load_spec(path: Path) -> dict[str, list[dict[str, str]]]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Expected a pilot mapping in {path}")
    result: dict[str, list[dict[str, str]]] = {}
    for mode in ("remote", "local", "grouped"):
        rows = raw.get(mode, [])
        if not isinstance(rows, list):
            raise ValueError(f"{mode} pilot entries must be a list")
        result[mode] = [dict(row) for row in rows]
    return result


def build_pilot_registries(
    registry_path: Path,
    spec_path: Path,
    output_dir: Path,
    identity_overrides_path: Path | None = None,
) -> dict[str, Any]:
    """Write exact registry subsets and a machine-readable pilot manifest."""
    registry, _ = _load_registry(registry_path)
    by_name = {str(row.get("name")): row for row in registry if row.get("name")}
    spec = _load_spec(spec_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    override_path = identity_overrides_path or registry_path.with_name("identity_overrides.yaml")
    overrides = []
    if override_path.exists():
        loaded = yaml.safe_load(override_path.read_text(encoding="utf-8")) or []
        if not isinstance(loaded, list):
            raise ValueError(f"Expected an identity override list in {override_path}")
        overrides = [dict(row) for row in loaded]
    manifest: dict[str, Any] = {
        "source_registry": str(registry_path),
        "pilot_spec": str(spec_path),
        "modes": {},
    }
    for mode, requested in spec.items():
        names = [row["name"] for row in requested]
        missing = sorted(set(names) - set(by_name))
        if missing:
            raise ValueError(f"Unknown {mode} pilot sources: {missing}")
        selected = [dict(by_name[name]) for name in names]
        for row in selected:
            name = str(row["name"])
            if mode == "remote" and not row.get("endpoint"):
                raise ValueError(f"Remote pilot source {name!r} has no endpoint")
            local_fields = [
                key
                for key, value in row.items()
                if value and (key.startswith("download_") or key == "local_tar_url")
            ]
            if mode in ("local", "grouped") and not local_fields and not row.get("local_provider"):
                raise ValueError(f"{mode.title()} pilot source {name!r} has no configured local input")
        mode_dir = output_dir / mode
        mode_dir.mkdir(parents=True, exist_ok=True)
        target = mode_dir / "sources.yaml"
        target.write_text(yaml.safe_dump(selected, sort_keys=False), encoding="utf-8")
        selected_names = set(names)
        filtered_overrides = [
            row
            for row in overrides
            if row.get("left") in selected_names and row.get("right") in selected_names
        ]
        override_target = mode_dir / "identity_overrides.yaml"
        override_target.write_text(
            yaml.safe_dump(filtered_overrides, sort_keys=False), encoding="utf-8"
        )
        manifest["modes"][mode] = {
            "registry": f"{mode}/sources.yaml",
            "identity_overrides": f"{mode}/identity_overrides.yaml",
            "sources": requested,
            "source_count": len(selected),
            "identity_override_count": len(filtered_overrides),
        }
    manifest_path = output_dir / "pilot_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", type=Path, default=Path("data/sources.yaml"))
    parser.add_argument("--spec", type=Path, default=Path("data/article_pilot.yaml"))
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = build_pilot_registries(args.sources, args.spec, args.output_dir)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
