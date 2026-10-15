"""Checkpoints of the scan's counting and structural phases, so that a retried run reuses them.

Counting the patterns of a large store and its structural census are the longest phases of a
scan run. Their results are written next to the row store as each phase ends
(``checkpoints/<phase>-<key>.json``) and read back by a later run of the same work: the same
index and store (its manifest and when it was written), the same rdfsolve version and commit,
and the same settings of the
phase (graph scope, grouping before counting, and for the census the counted patterns). Any
difference gives another key, so a checkpoint is never read for other work. Set
RDFSOLVE_SCAN_CHECKPOINTS=0 to neither read nor write them.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any

__all__ = ["read_checkpoint", "write_checkpoint"]

logger = logging.getLogger(__name__)

# Raised when the layout of a checkpoint changes, so that older files are not read.
CHECKPOINT_FORMAT = 1


def _enabled() -> bool:
    """Return whether checkpoints are read and written (RDFSOLVE_SCAN_CHECKPOINTS)."""
    return os.environ.get("RDFSOLVE_SCAN_CHECKPOINTS", "1") not in ("0", "false", "no")


def _path(store: Any, phase: str, settings: dict[str, Any]) -> Path:
    """Return the checkpoint file of *phase* for this store, code and *settings*."""
    from rdfsolve.version import get_version

    base = store.base
    identity = {
        "format": CHECKPOINT_FORMAT,
        "phase": phase,
        "version": get_version(with_git_hash=True),
        "index": base.manifest.get("index"),
        "store_version": base.manifest.get("version"),
        "membership": base.manifest.get("membership"),
        # A store written again in the same place is other work.
        "written": (Path(base.path) / "store.json").stat().st_mtime_ns,
        "settings": settings,
    }
    text = json.dumps(identity, sort_keys=True, default=str)
    key = hashlib.sha256(text.encode()).hexdigest()[:16]
    return Path(base.path) / "checkpoints" / f"{phase}-{key}.json"


def read_checkpoint(store: Any, phase: str, settings: dict[str, Any]) -> dict[str, Any] | None:
    """Return the data written by write_checkpoint for this work, or None when there is none."""
    if not _enabled():
        return None
    path = _path(store, phase, settings)
    if not path.is_file():
        return None
    try:
        data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        logger.warning("Scan checkpoint %s cannot be read (%s): the phase runs again", path, error)
        return None
    logger.info("Scan checkpoint: %s read from %s", phase, path)
    return data


def write_checkpoint(
    store: Any, phase: str, settings: dict[str, Any], data: dict[str, Any]
) -> Path | None:
    """Write *data* (JSON) as the checkpoint of *phase*; return its path, or None when off.

    The file is written beside and then renamed, so that a run stopped while writing leaves
    no partial checkpoint.
    """
    if not _enabled():
        return None
    path = _path(store, phase, settings)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_suffix(".partial")
        partial.write_text(json.dumps(data, default=str), encoding="utf-8")
        partial.replace(path)
    except OSError as error:
        logger.warning("Scan checkpoint %s not written (%s)", path, error)
        return None
    logger.info("Scan checkpoint: %s written to %s", phase, path)
    return path
