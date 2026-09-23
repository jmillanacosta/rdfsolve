"""Check cached index structure without loading or changing it."""

from __future__ import annotations

import json
from pathlib import Path

from rdfsolve.qlever.lifecycle import index_name


def has_cached_index(workdir: Path, fallback: str) -> bool:
    """Reject known incomplete indices; file checks do not prove loadability."""
    name = index_name(workdir, fallback)
    metadata_path = workdir / f"{name}.meta-data.json"
    if not metadata_path.is_file():
        if any(workdir.glob(f"{name}.index.*")):
            raise ValueError(f"Incomplete index in {workdir}: missing {metadata_path.name}")
        return False
    try:
        metadata = json.loads(metadata_path.read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"Unreadable index metadata: {metadata_path}") from error
    required = {
        "num-subjects",
        "num-predicates",
        "num-objects",
        "num-triples",
        "has-all-permutations",
        "index-format-version",
        "vocabulary-type",
    }
    missing = sorted(required - metadata.keys()) if isinstance(metadata, dict) else sorted(required)
    if missing:
        raise ValueError(f"Incomplete index metadata in {metadata_path}: missing {missing}")
    for key in ("num-subjects", "num-predicates", "num-objects", "num-triples"):
        counts = metadata[key]
        if not isinstance(counts, dict) or any(
            type(counts.get(kind)) is not int or counts[kind] < 0 for kind in ("normal", "internal")
        ):
            raise ValueError(f"Invalid {key} in {metadata_path}")
    if not metadata["num-triples"]["normal"]:
        raise ValueError(f"Empty index in {workdir}: {metadata_path.name} reports no triples")
    if not isinstance(metadata["has-all-permutations"], bool):
        raise ValueError(f"Invalid has-all-permutations in {metadata_path}")
    permutations = ["pso", "pos"]
    if metadata["has-all-permutations"]:
        permutations += ["spo", "sop", "osp", "ops"]
    missing_files = [
        f"{name}.index.{permutation}{suffix}"
        for permutation in permutations
        for suffix in ("", ".meta")
        if not (workdir / f"{name}.index.{permutation}{suffix}").is_file()
        or (workdir / f"{name}.index.{permutation}{suffix}").stat().st_size == 0
    ]
    if missing_files:
        raise ValueError(f"Incomplete index in {workdir}: missing or empty {missing_files}")
    return True


def index_artifact_files(workdir: Path, fallback: str) -> list[Path]:
    """List index data files after structural checks; exclude runtime logs."""
    if not has_cached_index(workdir, fallback):
        raise ValueError(f"No cached index in {workdir}")
    name = index_name(workdir, fallback)
    files = []
    for path in sorted(workdir.glob(f"{name}.*")):
        suffix = path.name.removeprefix(f"{name}.")
        if suffix.endswith((".log", "-log.tsv", "-log.jsonl")):
            continue
        if path.is_file() and (
            suffix == "meta-data.json"
            or suffix.startswith(("index.", "internal.index.", "vocabulary."))
        ):
            files.append(path)
    return files
