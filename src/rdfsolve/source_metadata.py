"""Read generated source metadata without changing curated access settings."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def with_source_metadata(entries: list[dict[str, Any]], registry: Path) -> list[dict[str, Any]]:
    """Add matching Bioregistry observations from the adjacent metadata sidecar."""
    path = registry.with_suffix(".metadata.json")
    if not path.exists():
        return entries
    document = json.loads(path.read_text(encoding="utf-8"))
    records = document.get("sources") if isinstance(document, dict) else None
    if not isinstance(records, dict):
        raise ValueError(f"Expected source metadata records in {path}")
    result = []
    for entry in entries:
        name = entry.get("name")
        metadata = records.get(name, {})
        if not isinstance(metadata, dict) or any(
            not key.startswith("bioregistry_") for key in metadata
        ):
            raise ValueError(f"Invalid source metadata for {name}")
        if metadata and metadata.get("bioregistry_prefix", "") != entry.get(
            "bioregistry_prefix", ""
        ):
            raise ValueError(f"Source metadata prefix differs for {name}")
        result.append({**metadata, **entry})
    return result
