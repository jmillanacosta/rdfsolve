"""SSSOM (Simple Standard for Sharing Ontology Mappings) importer.

Downloads SSSOM bundles listed in ``data/sssom_sources.yaml``, extracts
every ``.sssom.tsv`` file, converts each one to a
:class:`~rdfsolve.models.SsomMapping` JSON-LD file, and writes the results
to a configurable output directory (default: ``docker/mappings/sssom/``).

Typical usage
-------------
From Python::

    from rdfsolve.sssom_importer import import_sssom_source, seed_sssom_mappings

    # Import a single source defined in data/sssom_sources.yaml
    result = import_sssom_source(
        entry={
            "name": "ols_mappings",
            "provider": "EMBL-EBI (UK)",
            "url": "https://ftp.ebi.ac.uk/pub/databases/spot/ols/latest/mappings_sssom.tgz",
        },
        output_dir="docker/mappings/sssom/",
    )

    # Seed all sources in the YAML
    results = seed_sssom_mappings(
        sssom_sources_yaml="data/sssom_sources.yaml",
        output_dir="docker/mappings/sssom/",
    )

From the CLI::

    python scripts/seed_sssom_mappings.py
    python scripts/seed_sssom_mappings.py --name ols_mappings
    python scripts/seed_sssom_mappings.py --output-dir /tmp/sssom/

SSSOM TSV format
----------------
Each ``.sssom.tsv`` file has a YAML front-matter block (lines starting with
``#``) followed by a TSV header row and data rows.  Mandatory mapping columns
(SSSOM v0.15 and later):

    subject_id  predicate_id  object_id  mapping_justification

Optional columns used when present:

    subject_label  object_label  confidence  license  mapping_set_id
    mapping_set_title  subject_source  object_source

The front-matter may also carry ``mapping_set_id``, ``mapping_set_title``,
``license``, and a ``curie_map`` (prefix → namespace) block.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Predicate normalisation
# ──────────────────────────────────────────────────────────────────────────────

# Map common SSSOM / OWL predicates to their full URIs.
# The importer resolves short-forms via the file's own curie_map first; this
# table serves as a fallback for the most common cases.
_PREDICATE_FALLBACK: Dict[str, str] = {
    # SKOS
    "skos:exactMatch":       "http://www.w3.org/2004/02/skos/core#exactMatch",
    "skos:closeMatch":       "http://www.w3.org/2004/02/skos/core#closeMatch",
    "skos:broadMatch":       "http://www.w3.org/2004/02/skos/core#broadMatch",
    "skos:narrowMatch":      "http://www.w3.org/2004/02/skos/core#narrowMatch",
    "skos:relatedMatch":     "http://www.w3.org/2004/02/skos/core#relatedMatch",
    # OWL
    "owl:equivalentClass":   "http://www.w3.org/2002/07/owl#equivalentClass",
    "owl:sameAs":            "http://www.w3.org/2002/07/owl#sameAs",
    # SSSOM
    "sssom:NoTermFound":     "https://w3id.org/sssom/NoTermFound",
}

# Default predicate when none is provided
_DEFAULT_PREDICATE = "http://www.w3.org/2004/02/skos/core#exactMatch"


# ──────────────────────────────────────────────────────────────────────────────
# SSSOM TSV parsing helpers
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_curie(curie: str, curie_map: Dict[str, str]) -> str:
    """Expand a CURIE to a full URI using *curie_map*, or return as-is."""
    if curie.startswith("http://") or curie.startswith("https://"):
        return curie
    if ":" in curie:
        prefix, local = curie.split(":", 1)
        namespace = curie_map.get(prefix)
        if namespace:
            return namespace + local
    # Look up known fallback predicates verbatim
    return curie


def _parse_sssom_header(lines: List[str]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Parse the YAML front-matter from a list of comment lines.

    Returns ``(metadata_dict, curie_map)`` where *metadata_dict* contains
    the scalar key→value pairs from the SSSOM header and *curie_map* is the
    ``curie_map`` sub-dictionary (or an empty dict if absent).
    """
    # Strip leading '#' and collect only lines before the TSV header
    yaml_lines = [line[1:].lstrip(" ") for line in lines if line.startswith("#")]
    if not yaml_lines:
        return {}, {}

    raw = yaml.safe_load("\n".join(yaml_lines)) or {}
    if not isinstance(raw, dict):
        return {}, {}

    curie_map: Dict[str, str] = raw.pop("curie_map", {}) or {}
    if not isinstance(curie_map, dict):
        curie_map = {}

    return raw, curie_map


def _parse_sssom_tsv(content: str) -> Tuple[Dict[str, Any], Dict[str, str], List[Dict[str, str]]]:
    """Parse a complete SSSOM TSV string.

    Returns ``(header_meta, curie_map, rows)`` where *rows* is a list of
    dicts (one per data row) keyed by column name.
    """
    lines = content.splitlines()
    comment_lines: List[str] = []
    data_lines: List[str] = []

    for line in lines:
        if line.startswith("#"):
            comment_lines.append(line)
        else:
            data_lines.append(line)

    header_meta, curie_map = _parse_sssom_header(comment_lines)

    # Parse the TSV section
    rows: List[Dict[str, str]] = []
    if data_lines:
        reader = csv.DictReader(io.StringIO("\n".join(data_lines)), delimiter="\t")
        rows = [row for row in reader if any(v.strip() for v in row.values())]

    return header_meta, curie_map, rows


# ──────────────────────────────────────────────────────────────────────────────
# MappingEdge conversion
# ──────────────────────────────────────────────────────────────────────────────

def _rows_to_edges(
    rows: List[Dict[str, str]],
    curie_map: Dict[str, str],
    source_name: str,
) -> List[Dict[str, Any]]:
    """Convert TSV rows to a list of MappingEdge-compatible dicts.

    Each dict has the same keys as :class:`~rdfsolve.models.MappingEdge`.
    We defer importing the Pydantic model to avoid circular import issues at
    module load time; callers that need actual ``MappingEdge`` objects can
    do ``MappingEdge(**d)`` on each dict.
    """
    edges: List[Dict[str, Any]] = []
    skipped = 0

    for row in rows:
        subject_id = row.get("subject_id", "").strip()
        object_id = row.get("object_id", "").strip()
        predicate_id = row.get("predicate_id", "").strip()

        if not subject_id or not object_id:
            skipped += 1
            continue

        # Resolve CURIEs → full URIs
        src_uri = _resolve_curie(subject_id, curie_map)
        tgt_uri = _resolve_curie(object_id, curie_map)

        if predicate_id:
            pred_uri = (
                _PREDICATE_FALLBACK.get(predicate_id)
                or _resolve_curie(predicate_id, curie_map)
            )
        else:
            pred_uri = _DEFAULT_PREDICATE

        # Derive a dataset label from the CURIE prefix when available
        def _dataset_label(curie: str, uri: str) -> str:
            if ":" in curie and not curie.startswith("http"):
                return curie.split(":")[0]
            # fall back to the source bundle name
            return source_name

        src_dataset = _dataset_label(subject_id, src_uri)
        tgt_dataset = _dataset_label(object_id, tgt_uri)

        # Optional confidence
        conf_str = row.get("confidence", "").strip()
        confidence: Optional[float] = None
        if conf_str:
            try:
                confidence = float(conf_str)
            except ValueError:
                pass

        edge: Dict[str, Any] = {
            "source_class": src_uri,
            "target_class": tgt_uri,
            "predicate": pred_uri,
            "source_dataset": src_dataset,
            "target_dataset": tgt_dataset,
        }
        if confidence is not None:
            edge["confidence"] = confidence

        edges.append(edge)

    if skipped:
        logger.debug("Skipped %d rows with missing subject_id or object_id", skipped)

    return edges


# ──────────────────────────────────────────────────────────────────────────────
# Archive download + extraction
# ──────────────────────────────────────────────────────────────────────────────

def _download_to_tempfile(url: str) -> Path:
    """Download *url* to a temporary file and return its path.

    Tries ``requests`` first (handles TLS + redirects well); falls back to
    ``urllib`` if requests is not installed.
    """
    suffix = Path(url.split("?")[0]).suffix or ".tmp"
    fd, tmp_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    logger.info("Downloading %s …", url)

    try:
        import requests  # optional dependency

        with requests.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
    except ImportError:
        # Fall back to urllib with a browser-like User-Agent so servers
        # don't block the request
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 rdfsolve-sssom-importer/1.0"},
        )
        with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310
            with open(tmp_path, "wb") as fh:
                while True:
                    chunk = resp.read(1 << 20)
                    if not chunk:
                        break
                    fh.write(chunk)

    logger.info("Download complete → %s", tmp_path)
    return Path(tmp_path)


def _iter_sssom_tsv_from_archive(archive_path: Path) -> Iterator[Tuple[str, str]]:
    """Yield ``(filename, content)`` for every ``.sssom.tsv`` in an archive.

    Supports ``.tgz``, ``.tar.gz``, ``.tar.bz2``, ``.zip``.  For plain
    ``.tsv`` / ``.sssom.tsv`` inputs (not archives) just reads the file.
    """
    name = archive_path.name.lower()

    if name.endswith(".tgz") or name.endswith(".tar.gz") or name.endswith(".tar.bz2"):
        with tarfile.open(archive_path) as tar:
            for member in tar.getmembers():
                if member.name.endswith(".sssom.tsv") and member.isfile():
                    f = tar.extractfile(member)
                    if f:
                        content = f.read().decode("utf-8", errors="replace")
                        yield Path(member.name).name, content

    elif name.endswith(".zip"):
        with zipfile.ZipFile(archive_path) as zf:
            for info in zf.infolist():
                if info.filename.endswith(".sssom.tsv") and not info.is_dir():
                    content = zf.read(info).decode("utf-8", errors="replace")
                    yield Path(info.filename).name, content

    elif name.endswith(".tsv") or name.endswith(".sssom.tsv"):
        yield archive_path.name, archive_path.read_text(encoding="utf-8", errors="replace")

    else:
        raise ValueError(
            f"Unsupported archive format: {archive_path.name!r}. "
            "Expected .tgz, .tar.gz, .tar.bz2, .zip, or .sssom.tsv"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def import_sssom_source(
    entry: Dict[str, Any],
    output_dir: str = "docker/mappings/sssom",
) -> Dict[str, Any]:
    """Download and convert one SSSOM source entry to JSON-LD files.

    For each ``.sssom.tsv`` file found in the archive at ``entry["url"]``,
    one JSON-LD file is written to *output_dir*::

        {source_name}__{sssom_filename_stem}.jsonld

    Args:
        entry: A dict with at least ``name`` and ``url`` keys, as found in
               ``data/sssom_sources.yaml``.
        output_dir: Directory to write output JSON-LD files.

    Returns:
        Summary dict::

            {
                "succeeded": ["ols_mappings__hp.sssom.tsv", ...],
                "failed":    [{"file": "...", "error": "..."}],
                "skipped":   [],
            }
    """
    from rdfsolve.models import AboutMetadata, MappingEdge, SsomMapping

    source_name: str = entry["name"]
    url: str = entry["url"]

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    succeeded: List[str] = []
    failed: List[Dict[str, str]] = []

    # Download the archive
    try:
        archive_path = _download_to_tempfile(url)
    except Exception as exc:
        logger.error("Failed to download %s: %s", url, exc)
        return {
            "succeeded": [],
            "failed": [{"source": source_name, "error": str(exc)}],
            "skipped": [],
        }

    try:
        for sssom_filename, content in _iter_sssom_tsv_from_archive(archive_path):
            label = f"{source_name}__{sssom_filename}"
            try:
                header_meta, curie_map, rows = _parse_sssom_tsv(content)

                edge_dicts = _rows_to_edges(rows, curie_map, source_name)
                edges = [MappingEdge(**d) for d in edge_dicts]

                stem = Path(sssom_filename).stem  # strip .sssom.tsv → stem
                dataset_name = f"{source_name}__{stem}"

                about = AboutMetadata.build(
                    dataset_name=dataset_name,
                    pattern_count=len(edges),
                    strategy="sssom_import",
                )

                mapping = SsomMapping(
                    edges=edges,
                    about=about,
                    source_name=source_name,
                    sssom_file=sssom_filename,
                    mapping_set_id=header_meta.get("mapping_set_id"),
                    mapping_set_title=header_meta.get("mapping_set_title"),
                    license=header_meta.get("license") or entry.get("license"),
                    curie_map=curie_map,
                )

                out_path = out / f"{dataset_name}.jsonld"
                out_path.write_text(
                    json.dumps(mapping.to_jsonld(), indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                logger.info(
                    "Wrote %s  (%d edges)", out_path.name, len(edges)
                )
                succeeded.append(label)

            except Exception as exc:
                logger.warning("Failed to convert %s: %s", sssom_filename, exc)
                failed.append({"file": label, "error": str(exc)})
    finally:
        try:
            archive_path.unlink()
        except OSError:
            pass

    return {"succeeded": succeeded, "failed": failed, "skipped": []}


def seed_sssom_mappings(
    sssom_sources_yaml: str = "data/sssom_sources.yaml",
    output_dir: str = "docker/mappings/sssom",
    names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Seed SSSOM mapping files for all (or selected) sources.

    Reads *sssom_sources_yaml*, optionally filters to *names*, and calls
    :func:`import_sssom_source` for each entry.

    Args:
        sssom_sources_yaml: Path to the SSSOM sources YAML file.
        output_dir: Directory for output JSON-LD files.
        names: Optional list of source names to restrict processing;
               if ``None`` (default), all entries are processed.

    Returns:
        Aggregated summary with keys ``"succeeded"``, ``"failed"``,
        ``"skipped"``.
    """
    yaml_path = Path(sssom_sources_yaml)
    if not yaml_path.exists():
        raise FileNotFoundError(
            f"SSSOM sources YAML not found: {yaml_path}. "
            "Create data/sssom_sources.yaml first."
        )

    entries: List[Dict[str, Any]] = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or []

    if names:
        name_set = set(names)
        unknown = name_set - {e["name"] for e in entries}
        if unknown:
            logger.warning("Unknown SSSOM source name(s): %s", ", ".join(sorted(unknown)))
        entries = [e for e in entries if e["name"] in name_set]

    if not entries:
        logger.warning("No SSSOM sources to process.")
        return {"succeeded": [], "failed": [], "skipped": []}

    all_succeeded: List[str] = []
    all_failed: List[Dict[str, str]] = []
    all_skipped: List[str] = []

    for entry in entries:
        logger.info("Processing SSSOM source: %s", entry["name"])
        result = import_sssom_source(entry, output_dir=output_dir)
        all_succeeded.extend(result.get("succeeded", []))
        all_failed.extend(result.get("failed", []))
        all_skipped.extend(result.get("skipped", []))

    return {
        "succeeded": all_succeeded,
        "failed": all_failed,
        "skipped": all_skipped,
    }
