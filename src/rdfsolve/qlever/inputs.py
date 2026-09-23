"""Find prepared RDF inputs without silently dropping a directory layout."""

import gzip
import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path

RDF_SUFFIXES = ("ttl", "nt", "nq", "trig")

_QLEVER_FORMATS = {"ttl": "ttl", "nt": "nt", "nq": "nq", "trig": "ttl"}

_GZIP_MAGIC = b"\x1f\x8b"


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
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ValueError(f"Cached input changed during inspection: {path}")
    if not decoded_bytes:
        raise ValueError(f"Cached input has no decoded content: {path}")
    return InputCheck(str(path), before.st_size, decoded_bytes, digest.hexdigest())


def qlever_format(path: Path) -> str:
    """Map a prepared input to a QLever input format."""
    suffix = path.suffix.lstrip(".").lower()
    try:
        return _QLEVER_FORMATS[suffix]
    except KeyError:
        raise ValueError(f"Unsupported RDF input format: {path}") from None


def _is_gzip(path: Path) -> bool:
    with path.open("rb") as stream:
        return stream.read(2) == _GZIP_MAGIC


def cached_archives(workdir: Path) -> list[Path]:
    """List cached compressed inputs in the root or rdf/ layout."""
    found: dict[str, Path] = {}
    for directory in (workdir, workdir / "rdf"):
        for suffix in RDF_SUFFIXES:
            for path in sorted(directory.glob(f"*.{suffix}.gz")):
                if path.is_file():
                    found.setdefault(path.name, path)
    return sorted(found.values())


def unusable_inputs(workdir: Path) -> list[tuple[Path, str]]:
    """Report cached inputs that cannot be parsed, without decompressing them."""
    problems: list[tuple[Path, str]] = []
    for directory in (workdir, workdir / "rdf"):
        for suffix in RDF_SUFFIXES:
            for path in sorted([*directory.glob(f"*.{suffix}"), *directory.glob(f"*.{suffix}.gz")]):
                if not path.is_file():
                    problems.append((path, "missing target"))
                elif not path.stat().st_size:
                    problems.append((path, "empty file"))
                elif path.suffix == ".gz" and not _is_gzip(path):
                    problems.append((path, "not gzip data"))
    return problems


def expand_inputs(workdir: Path) -> list[Path]:
    """Decompress cached inputs beside the archive. Return the files created."""
    created: list[Path] = []
    for archive in cached_archives(workdir):
        target = archive.with_suffix("")
        if target.exists():
            continue
        if not _is_gzip(archive):
            raise ValueError(f"Cached input is not gzip data: {archive}")
        partial = target.with_name(f"{target.name}.part")
        try:
            with gzip.open(archive, "rb") as stream, partial.open("wb") as output:
                shutil.copyfileobj(stream, output, 8 * 1024 * 1024)
            partial.replace(target)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
        created.append(target)
    return created


def rdf_input_files(workdir: Path) -> list[Path]:
    """Read root or rdf/ inputs. Reject ambiguous copies and empty files."""
    found: dict[str, Path] = {}
    for directory in (workdir, workdir / "rdf"):
        for suffix in RDF_SUFFIXES:
            for path in sorted(directory.glob(f"*.{suffix}")):
                if not path.is_file() or path.stat().st_size == 0:
                    raise ValueError(f"Missing or empty RDF input: {path}")
                previous = found.get(path.name)
                if previous is not None and not previous.samefile(path):
                    raise ValueError(f"Ambiguous RDF inputs: {previous} and {path}")
                found[path.name] = path
    return sorted(found.values())


def graph_input_directory(workdir: Path, graph: str) -> Path:
    """Return the input directory for a named graph."""
    return workdir / "graphs" / hashlib.sha256(graph.encode()).hexdigest()


def mapped_input_files(workdir: Path, graphs: list[str]) -> list[tuple[Path, str]]:
    """Require prepared triple files for every mapped graph."""
    inputs: list[tuple[Path, str]] = []
    for graph in graphs:
        files = rdf_input_files(graph_input_directory(workdir, graph))
        if not files:
            raise ValueError(f"No prepared inputs for graph {graph}")
        if any(path.suffix not in {".ttl", ".nt"} for path in files):
            raise ValueError(f"Mapped graph {graph} requires triple inputs")
        inputs.extend((path, graph) for path in files)
    return inputs
