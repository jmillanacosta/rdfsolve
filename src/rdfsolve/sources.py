"""Load data-source definitions from ``data/sources.yaml``.

The canonical source registry is a YAML file containing a flat list
of mappings, one per SPARQL data source.  Each mapping carries:

* **name** - unique human-readable identifier.
* **endpoint** - SPARQL endpoint URL.
* **graph_uris** - named graphs to query.
* **use_graph** - whether to wrap queries in a ``GRAPH`` clause.
* **two_phase** - use two-phase mining (default ``True``).
* Optional tuning knobs: *chunk_size*, *class_batch_size*,
  *class_chunk_size*, *timeout*, *delay*, *counts*, *unsafe_paging*.

Legacy CSV files (``data/sources.csv``) and JSON-LD files are still
accepted: the reader auto-detects the format by extension.

Typical usage::

    from rdfsolve.sources import load_sources

    for src in load_sources("data/sources.yaml"):
        print(src["name"], src["endpoint"])
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


class SourceEntry(TypedDict, total=False):
    """Typed dictionary for a single data-source definition."""

    name: str
    endpoint: str
    void_iri: str
    graph_uris: list[str]
    use_graph: bool
    two_phase: bool
    chunk_size: int
    class_batch_size: int
    class_chunk_size: int | None
    timeout: float
    delay: float
    counts: bool
    unsafe_paging: bool
    notes: str


# ── default path ──────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SOURCES_YAML = _REPO_ROOT / "data" / "sources.yaml"
DEFAULT_SOURCES_JSONLD = _REPO_ROOT / "data" / "sources.jsonld"
DEFAULT_SOURCES_CSV = _REPO_ROOT / "data" / "sources.csv"


def _default_sources_path() -> Path:
    """Return the default sources file, preferring YAML."""
    if DEFAULT_SOURCES_YAML.exists():
        return DEFAULT_SOURCES_YAML
    if DEFAULT_SOURCES_JSONLD.exists():
        return DEFAULT_SOURCES_JSONLD
    return DEFAULT_SOURCES_CSV


# ── loading ───────────────────────────────────────────────────────


def load_sources(
    path: str | Path | None = None,
) -> list[SourceEntry]:
    """Load data-source definitions from a YAML, JSON-LD, or CSV file.

    Parameters
    ----------
    path:
        Path to the sources file.  When ``None`` the default
        ``data/sources.yaml`` (or ``.jsonld`` / ``.csv`` fallback)
        is used.

    Returns
    -------
    list[SourceEntry]
        One dict per data source, keys normalised to snake_case.
        Sources without an ``endpoint`` are included (callers may
        skip them).
    """
    p = Path(path) if path is not None else _default_sources_path()
    suffix = p.suffix.lower()

    if suffix in (".yaml", ".yml"):
        return _load_yaml(p)
    if suffix in (".jsonld", ".json"):
        return _load_jsonld(p)
    if suffix == ".csv":
        return _load_csv(p)
    raise ValueError(
        f"Unsupported sources file format {suffix!r}: expected .yaml, .yml, .jsonld, .json, or .csv"
    )


# ── YAML reader ───────────────────────────────────────────────────


def _load_yaml(path: Path) -> list[SourceEntry]:
    with open(path, encoding="utf-8") as fh:
        nodes = yaml.safe_load(fh)

    if not isinstance(nodes, list):
        raise ValueError(f"Expected a YAML list of source mappings in {path}")

    entries: list[SourceEntry] = []
    for node in nodes:
        entry = _yaml_node_to_entry(node)
        entries.append(entry)

    logger.info("Loaded %d sources from %s", len(entries), path)
    return entries


def _yaml_node_to_entry(node: dict[str, Any]) -> SourceEntry:
    """Convert a single YAML mapping to a SourceEntry."""
    e: SourceEntry = {}

    e["name"] = node.get("name", "")
    e["endpoint"] = node.get("endpoint", "")
    e["void_iri"] = node.get("void_iri", "")

    raw_g = node.get("graph_uris", [])
    if isinstance(raw_g, str):
        raw_g = [raw_g]
    e["graph_uris"] = list(raw_g)

    e["use_graph"] = bool(node.get("use_graph", False))
    e["two_phase"] = bool(node.get("two_phase", True))
    e["counts"] = bool(node.get("counts", True))
    e["unsafe_paging"] = bool(node.get("unsafe_paging", False))

    for int_key in (
        "chunk_size",
        "class_batch_size",
        "class_chunk_size",
    ):
        if int_key in node and node[int_key] is not None:
            e[int_key] = int(node[int_key])

    for float_key in ("timeout", "delay"):
        if float_key in node and node[float_key] is not None:
            e[float_key] = float(node[float_key])

    if "notes" in node:
        e["notes"] = str(node["notes"])

    # Pass through download_*, local_endpoint, and provider fields so
    # that scripts (e.g. mine_local.py generate-qleverfile) can see them.
    passthrough = {"local_endpoint", "local_provider", "local_tar_url"}
    for key in node:
        if key.startswith("download_") or key in passthrough:
            e[key] = node[key]  # type: ignore[literal-required]

    return e


# ── JSON-LD reader ────────────────────────────────────────────────


def _load_jsonld(path: Path) -> list[SourceEntry]:
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)

    graph = doc.get("@graph", [])
    entries: list[SourceEntry] = []

    for node in graph:
        entry = _node_to_entry(node)
        entries.append(entry)

    logger.info("Loaded %d sources from %s", len(entries), path)
    return entries


def _node_to_entry(node: dict[str, Any]) -> SourceEntry:
    """Convert a single JSON-LD ``@graph`` node to a SourceEntry."""
    e: SourceEntry = {}

    e["name"] = node.get("name", "")

    # endpoint can be a plain string or {"@id": "…"}
    ep = node.get("endpoint", "")
    if isinstance(ep, dict):
        ep = ep.get("@id", "")
    e["endpoint"] = ep

    # void_iri - same treatment
    vi = node.get("void_iri", "")
    if isinstance(vi, dict):
        vi = vi.get("@id", "")
    e["void_iri"] = vi

    # graph_uris- normalise to list[str]
    raw_g = node.get("graph_uris", [])
    if isinstance(raw_g, str):
        raw_g = [raw_g]
    e["graph_uris"] = [(g["@id"] if isinstance(g, dict) else g) for g in raw_g]

    # booleans
    e["use_graph"] = bool(node.get("use_graph", False))
    e["two_phase"] = bool(node.get("two_phase", True))
    e["counts"] = bool(node.get("counts", True))
    e["unsafe_paging"] = bool(node.get("unsafe_paging", False))

    # optional numeric overrides (only set when present)
    for int_key in ("chunk_size", "class_batch_size", "class_chunk_size"):
        if int_key in node and node[int_key] is not None:
            e[int_key] = int(node[int_key])

    for float_key in ("timeout", "delay"):
        if float_key in node and node[float_key] is not None:
            e[float_key] = float(node[float_key])

    if "notes" in node:
        e["notes"] = str(node["notes"])

    return e


# ── CSV reader (deprecated now) ──────────────────────────────────────────


def _load_csv(path: Path) -> list[SourceEntry]:
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    entries: list[SourceEntry] = []
    for row in rows:
        e: SourceEntry = {}

        e["name"] = (row.get("dataset_name") or "").strip()
        e["endpoint"] = (row.get("endpoint_url") or "").strip()
        e["void_iri"] = (row.get("void_iri") or "").strip()

        graph_uri = (row.get("graph_uri") or "").strip()
        e["graph_uris"] = [graph_uri] if graph_uri else []

        e["use_graph"] = (row.get("use_graph") or "").strip().lower() in ("true", "1", "yes")
        # two_phase defaults to True unless explicitly off
        tp = (row.get("two_phase") or "").strip().lower()
        e["two_phase"] = tp not in ("false", "0", "no")

        entries.append(e)

    logger.info("Loaded %d sources from CSV %s", len(entries), path)
    return entries


# ── DataFrame conversion (for instance_matcher compat) ────────────


def load_sources_dataframe(
    path: str | Path | None = None,
) -> pd.DataFrame:
    """Load sources and return a :class:`~pandas.DataFrame`.

    The DataFrame has columns compatible with
    :func:`~rdfsolve.instance_matcher.probe_resource`:
    ``dataset_name``, ``endpoint_url``, ``graph_uri``, ``use_graph``,
    ``void_iri``.

    Parameters
    ----------
    path:
        Path to the sources file.  ``None`` = auto-detect default.
    """
    entries = load_sources(path)
    rows = []
    for e in entries:
        rows.append(
            {
                "dataset_name": e.get("name", ""),
                "endpoint_url": e.get("endpoint", ""),
                "graph_uri": e["graph_uris"][0] if e.get("graph_uris") else "",
                "void_iri": e.get("void_iri", ""),
                "use_graph": e.get("use_graph", False),
            }
        )
    return pd.DataFrame(rows)
