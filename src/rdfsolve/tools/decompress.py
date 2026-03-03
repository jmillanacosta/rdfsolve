"""
Decompress RDF data files (.gz, .xz) in bulk.

Supports the two compression formats found across RDF data repositories:

* **.gz**  – used by RDFPortal (``*.ttl.gz``), Bio2RDF (``*.nq.gz``)
* **.xz**  – used by UniProt FTP (``*.rdf.xz``, ``*.owl.xz``)

Usage
-----
    # Decompress a single file (auto-detects format)
    python -m rdfsolve.tools.decompress data/medgen/MGCONSO.ttl.gz

    # Decompress everything under a directory
    python -m rdfsolve.tools.decompress data/uniprot_local/ --recursive

    # Dry-run: just show what would be decompressed
    python -m rdfsolve.tools.decompress data/ --recursive --dry-run

    # Keep the original compressed files
    python -m rdfsolve.tools.decompress data/ --recursive --keep
"""

from __future__ import annotations

import argparse
import gzip
import lzma
import logging
import shutil
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

__all__ = [
    "decompress_file",
    "decompress_directory",
    "SUPPORTED_EXTENSIONS",
]

# ── supported formats ────────────────────────────────────────────────────────

SUPPORTED_EXTENSIONS: dict[str, str] = {
    ".gz": "gzip",
    ".xz": "xz",
}


# ── core functions ───────────────────────────────────────────────────────────


def decompress_file(
    src: Path,
    *,
    dest: Path | None = None,
    keep: bool = False,
    chunk_size: int = 64 * 1024,
) -> Path | None:
    """Decompress a single ``.gz`` or ``.xz`` file.

    Parameters
    ----------
    src
        Path to the compressed file.
    dest
        Explicit output path.  When *None* the suffix is stripped
        (e.g. ``foo.ttl.gz`` → ``foo.ttl``).
    keep
        If *True* the original compressed file is kept; otherwise it is
        removed after successful decompression.
    chunk_size
        Read/write buffer size in bytes (default 64 KiB).

    Returns
    -------
    Path | None
        Path to the decompressed file, or *None* on error.
    """
    suffix = src.suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        log.warning("Unsupported extension %r — skipping %s", suffix, src)
        return None

    if dest is None:
        dest = src.with_suffix("")  # strip the .gz / .xz

    if dest.exists():
        log.info("  ✓ Already decompressed: %s", dest.name)
        return dest

    fmt = SUPPORTED_EXTENSIONS[suffix]
    opener = gzip.open if fmt == "gzip" else lzma.open

    src_mb = src.stat().st_size / (1024 * 1024)
    log.info("  ⬇  Decompressing %s (%.1f MB, %s) …", src.name, src_mb, fmt)

    try:
        with opener(src, "rb") as f_in, open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out, length=chunk_size)
        dest_mb = dest.stat().st_size / (1024 * 1024)
        log.info("  ✓ %s → %.1f MB", dest.name, dest_mb)
    except Exception as exc:
        log.error("  ✗ Failed to decompress %s: %s", src.name, exc)
        if dest.exists():
            dest.unlink()
        return None

    if not keep:
        src.unlink()
        log.debug("  🗑  Removed %s", src.name)

    return dest


def decompress_directory(
    directory: Path,
    *,
    recursive: bool = False,
    keep: bool = False,
    extensions: set[str] | None = None,
) -> list[Path]:
    """Decompress all supported compressed files in a directory.

    Parameters
    ----------
    directory
        Root directory to scan.
    recursive
        If *True*, walk subdirectories as well.
    keep
        Passed to :func:`decompress_file`.
    extensions
        Restrict to a subset of extensions (e.g. ``{".gz"}``).
        Defaults to all supported extensions.

    Returns
    -------
    list[Path]
        Paths to successfully decompressed files.
    """
    if extensions is None:
        extensions = set(SUPPORTED_EXTENSIONS)

    pattern_fn = directory.rglob if recursive else directory.glob
    files = sorted(
        f for ext in extensions for f in pattern_fn(f"*{ext}") if f.is_file()
    )

    if not files:
        log.info("No compressed files found in %s", directory)
        return []

    log.info("Found %d compressed file(s) in %s", len(files), directory)
    results: list[Path] = []
    for f in files:
        out = decompress_file(f, keep=keep)
        if out is not None:
            results.append(out)

    log.info("Decompressed %d / %d files", len(results), len(files))
    return results


# ── CLI entry-point ──────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Decompress RDF data files (.gz, .xz).",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="File or directory to decompress.",
    )
    parser.add_argument(
        "--recursive", "-r",
        action="store_true",
        help="Recurse into subdirectories.",
    )
    parser.add_argument(
        "--keep", "-k",
        action="store_true",
        help="Keep the original compressed files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list files that would be decompressed.",
    )
    parser.add_argument(
        "--extensions",
        nargs="*",
        default=None,
        help="Restrict to specific extensions (e.g. .gz .xz).",
    )
    args = parser.parse_args()

    exts = set(args.extensions) if args.extensions else None
    target: Path = args.path.resolve()

    if target.is_file():
        if args.dry_run:
            print(f"Would decompress: {target}")
        else:
            decompress_file(target, keep=args.keep)
    elif target.is_dir():
        if exts is None:
            exts = set(SUPPORTED_EXTENSIONS)
        pattern_fn = target.rglob if args.recursive else target.glob
        files = sorted(
            f for ext in exts for f in pattern_fn(f"*{ext}") if f.is_file()
        )
        if args.dry_run:
            print(f"Would decompress {len(files)} file(s):")
            for f in files:
                print(f"  {f}")
        else:
            decompress_directory(
                target,
                recursive=args.recursive,
                keep=args.keep,
                extensions=exts,
            )
    else:
        log.error("Path does not exist: %s", target)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
