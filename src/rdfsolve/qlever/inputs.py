"""Find prepared RDF inputs without silently dropping a directory layout."""

import gzip
import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class InputCheck:
    """Content digest and exact decoded size; not an RDF syntax check."""

    path: str
    stored_bytes: int
    decoded_bytes: int
    decoded_sha256: str


def check_cached_input(path: Path) -> InputCheck:
    """Stream the whole input and check gzip CRC without writing decoded data."""
    before = path.stat()
    if not path.is_file() or not before.st_size:
        raise ValueError(f"Missing or empty cached input: {path}")
    digest = hashlib.sha256()
    decoded_bytes = 0
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            decoded_bytes += len(chunk)
            digest.update(chunk)
    after = path.stat()
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino, after.st_size, after.st_mtime_ns
    ):
        raise ValueError(f"Cached input changed during inspection: {path}")
    if not decoded_bytes:
        raise ValueError(f"Cached input has no decoded content: {path}")
    return InputCheck(str(path), before.st_size, decoded_bytes, digest.hexdigest())


def rdf_input_files(workdir: Path) -> list[Path]:
    """Read root or rdf/ inputs. Reject ambiguous copies and empty files."""
    found: dict[str, Path] = {}
    for directory in (workdir, workdir / "rdf"):
        for suffix in ("ttl", "nt", "nq"):
            for path in sorted(directory.glob(f"*.{suffix}")):
                if not path.is_file() or path.stat().st_size == 0:
                    raise ValueError(f"Missing or empty RDF input: {path}")
                previous = found.get(path.name)
                if previous is not None and not previous.samefile(path):
                    raise ValueError(f"Ambiguous RDF inputs: {previous} and {path}")
                found[path.name] = path
    return sorted(found.values())
