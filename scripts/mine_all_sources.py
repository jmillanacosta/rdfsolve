#!/usr/bin/env python
r"""Mine JSON-LD schemas for every source in ``data/sources.csv``.

Usage::

    # From the repository root (with the venv activated):
    python scripts/mine_all_sources.py

    # Mine only QLever-hosted sources:
    python scripts/mine_all_sources.py --qlever

    # Mine everything (default CSV + QLever):
    python scripts/mine_all_sources.py --all-sources

    # Customise output directory and format:
    python scripts/mine_all_sources.py \\
        --sources data/sources.csv \\
        --output-dir mined_schemas \\
        --format jsonld \\
        --no-counts

The script delegates to :func:`rdfsolve.api.mine_all_sources` and
prints a progress line for each dataset.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure the ``src/`` tree is importable when running the script
# directly (without ``pip install -e .``).
_repo_root = Path(__file__).resolve().parent.parent
_src = _repo_root / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

_DEFAULT_CSV = _repo_root / "data" / "sources.csv"
_QLEVER_CSV = _repo_root / "data" / "qlever.csv"

from rdfsolve.api import mine_all_sources  # noqa: E402


def _cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mine RDF schemas for all sources in a CSV file.",
    )

    # ── Source selection (mutually exclusive) ─────────────────────
    src = p.add_mutually_exclusive_group()
    src.add_argument(
        "--sources",
        default=None,
        help="Path to sources CSV (default: data/sources.csv)",
    )
    src.add_argument(
        "--qlever",
        action="store_true",
        help="Mine only QLever-hosted sources (data/qlever.csv)",
    )
    src.add_argument(
        "--all-sources",
        action="store_true",
        help="Mine both default and QLever sources",
    )

    p.add_argument(
        "--output-dir",
        default=str(_repo_root / "mined_schemas"),
        help="Directory for output files (default: mined_schemas/)",
    )
    p.add_argument(
        "--format",
        choices=["jsonld", "void", "all"],
        default="all",
        help="Export format (default: all)",
    )
    p.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        help="SPARQL pagination page size (default: 10000)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout per request in seconds (default: 120)",
    )
    p.add_argument(
        "--no-counts",
        action="store_true",
        help="Skip triple-count queries (faster)",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    return p.parse_args()


def _on_progress(
    name: str, idx: int, total: int, error: str | None,
) -> None:
    if error == "skipped":
        pass
    elif error:
        pass
    else:
        pass


def main() -> None:
    args = _cli()

    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        force=True,
    )

    # Resolve which CSV file(s) to mine
    if args.qlever:
        csv_files = [str(_QLEVER_CSV)]
    elif args.all_sources:
        csv_files = [str(_DEFAULT_CSV), str(_QLEVER_CSV)]
    elif args.sources:
        csv_files = [args.sources]
    else:
        csv_files = [str(_DEFAULT_CSV)]

    all_succeeded: list[str] = []
    all_failed: list[dict[str, str]] = []

    for csv_file in csv_files:
        logging.info("Mining sources from %s", csv_file)
        result = mine_all_sources(
            sources_csv=csv_file,
            output_dir=args.output_dir,
            fmt=args.format,
            chunk_size=args.chunk_size,
            timeout=args.timeout,
            counts=not args.no_counts,
            on_progress=_on_progress,
        )
        all_succeeded.extend(result["succeeded"])
        all_failed.extend(result["failed"])

    logging.info(
        "Done: %d succeeded, %d failed",
        len(all_succeeded), len(all_failed),
    )

    # Exit with non-zero if anything failed
    if all_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
