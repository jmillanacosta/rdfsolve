"""Check cached index structure without loading or changing it."""

from __future__ import annotations

import json
import re
from pathlib import Path

from rdfsolve.qlever.inputs import INDEX_LOG
from rdfsolve.qlever.lifecycle import index_name

# qlever-index (parallel = false) skips the rest of an input after a statement it cannot read,
# logs this at INFO level and succeeds, so the rest of the file is lost without an error.
_UNPARSED = re.compile(r"Parsing of line has Failed.*?Remaining bytes: ([0-9,]+)")


# qlever-index fails when one statement is longer than its parser buffer (-b, 10M by default):
# the build log then says that the end of a statement "was not found in the current input batch".
_BUFFER = re.compile(r"marks the end of a statement was not found in the current input batch")
# The largest parser buffer asked for, in MB.
MAX_PARSER_BUFFER_MB = 2048


def statement_over_buffer(log: Path) -> bool:
    """Return whether a build log reports a statement longer than the parser buffer."""
    if not log.is_file():
        return False
    with log.open(encoding="utf-8", errors="replace") as stream:
        return any(_BUFFER.search(line) for line in stream)


def larger_buffer(size: str) -> str | None:
    """Return a parser buffer eight times SIZE (such as 10M), or None past the largest."""
    match = re.fullmatch(r"([0-9]+)\s*([KMG]?)B?", size.strip(), re.IGNORECASE)
    if match is None:
        return None
    megabytes = (
        int(match.group(1))
        * {"K": 1 / 1024, "": 1 / 2**20, "M": 1, "G": 1024}[match.group(2).upper()]
    )
    if megabytes >= MAX_PARSER_BUFFER_MB:
        return None
    return f"{min(MAX_PARSER_BUFFER_MB, max(1, int(megabytes * 8)))}M"


class TruncatedIndexError(ValueError):
    """An index whose build log reports input that qlever-index did not parse."""


def unparsed_input(log: Path) -> list[int]:
    """Return the bytes that qlever-index left unparsed, one number per input it stopped in.

    An empty list when the log reports none (or there is no log).
    """
    if not log.is_file():
        return []
    with log.open(encoding="utf-8", errors="replace") as stream:
        return [
            int(match.group(1).replace(",", ""))
            for line in stream
            for match in _UNPARSED.finditer(line)
        ]


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
    unparsed = unparsed_input(workdir / INDEX_LOG.format(name=name))
    if unparsed:
        raise TruncatedIndexError(
            f"Truncated index in {workdir}: its build log reports {len(unparsed)} inputs that "
            f"qlever-index stopped reading ({sum(unparsed):,} bytes not parsed)"
        )
    return True
