"""Update sources.yaml with discovered graph information.

This module provides utilities to update sources.yaml based on graph discovery results.
It uses the SourceEntry schema defined in sources.py to ensure consistency.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def _read_sources(path: Path) -> tuple[bytes | None, list[dict[str, Any]]]:
    """Read YAML entries without dropping fields not yet in the typed model."""
    original = path.read_bytes() if path.exists() else None
    raw = yaml.safe_load(original) if original else []
    if not isinstance(raw, list) or any(
        not isinstance(item, dict)
        or not isinstance(item.get("name"), str)
        or not item["name"].strip()
        for item in raw
    ):
        raise ValueError("Registry must be a YAML list of named source entries")
    names = [item["name"] for item in raw]
    if len(names) != len(set(names)):
        raise ValueError("Registry contains duplicate source names")
    return original, raw


def _write_sources(
    path: Path,
    entries: list[dict[str, Any]],
    *,
    original: bytes | None,
    backup: bool = True,
) -> None:
    """Back up and replace YAML; reject edits made since it was read."""
    if path.is_symlink():
        raise ValueError("Write to the registry file itself, not a symbolic link")
    path.parent.mkdir(parents=True, exist_ok=True)
    current = path.read_bytes() if path.exists() else None
    if current != original:
        raise RuntimeError("Registry changed during enrichment; reload it before saving")
    content = yaml.safe_dump(entries, sort_keys=False, allow_unicode=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    if backup and original is not None:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f"{path.name}.{stamp}.",
            suffix=".bak",
            delete=False,
        ) as saved:
            saved.write(original)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if (path.read_bytes() if path.exists() else None) != original:
            raise RuntimeError("Registry changed before saving; reload it")
        if path.exists():
            temporary.chmod(path.stat().st_mode & 0o777)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def update_sources_yaml_with_graphs(
    sources_file: str | Path,
    source_name: str,
    discovered_graphs: dict[str, Any],
    void_partition_graphs: list[str] | None = None,
    backup: bool = True,
) -> bool:
    """Save graph discoveries for an existing entry; preserve unqueried fields."""
    return update_multiple_sources(
        sources_file,
        {
            source_name: {
                "discovered_graphs": discovered_graphs,
                "void_partition_graphs": void_partition_graphs,
            }
        },
        backup=backup,
    )[source_name]


def update_multiple_sources(
    sources_file: str | Path,
    updates: dict[str, dict[str, Any]],
    backup: bool = True,
) -> dict[str, bool]:
    """Apply graph discoveries in one atomic write with a unique backup."""
    path = Path(sources_file)
    try:
        original, sources = _read_sources(path)
        by_name = {entry["name"]: entry for entry in sources}
        results = {}
        changed = False
        for name, update in updates.items():
            if name not in by_name:
                results[name] = False
                continue
            entry = by_name[name]
            values = {
                "void_graphs": update.get("discovered_graphs", {}).get("void_graphs"),
                "void_schema": update.get("void_partition_graphs"),
            }
            for key, value in values.items():
                if value is None:
                    continue  # This field was not queried.
                if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
                    raise ValueError(f"{key} must be a list of graph IRIs")
                if entry.get(key) != value:
                    entry[key] = value
                    changed = True
            results[name] = True
        if changed:
            _write_sources(path, sources, original=original, backup=backup)
        return results
    except (OSError, ValueError, RuntimeError, yaml.YAMLError) as error:
        logger.error("Could not update %s: %s", path, error)
        return dict.fromkeys(updates, False)


__all__ = ["update_multiple_sources", "update_sources_yaml_with_graphs"]
