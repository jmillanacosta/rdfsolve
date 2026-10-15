#!/usr/bin/env python
"""Check that every download URL of the registry exists (rdfsolve.download_check).

Every download_* field, the downloads of each graph of graph_sources and local_tar_url are
asked once with HEAD, politely: one request at a time per host, spaced by --interval, through
the host gate shared with mining ($RDFSOLVE_HTTP_LOCK_DIR). Writes a TSV with one row per URL
and the per-source JSON that the pipeline reads with --download-status-file (a source with a
missing, empty or unreachable download is not downloaded).

    python scripts/check_downloads.py --tsv downloads.tsv --json output/download_status.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import threading
from dataclasses import asdict, fields
from datetime import UTC, datetime
from pathlib import Path

import yaml

from rdfsolve.download_check import (
    BAD,
    DownloadCheck,
    check_downloads,
    registry_downloads,
    source_status,
)


def main(argv: list[str] | None = None) -> int:
    """Check the registry's download URLs; exit 1 when one cannot be downloaded."""
    repo = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sources", type=Path, default=repo / "data" / "sources.yaml")
    parser.add_argument("--tsv", type=Path, default=repo / "output" / "download_status.tsv")
    parser.add_argument("--json", type=Path, default=repo / "output" / "download_status.json")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between requests")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--only", nargs="*", help="registry names to check")
    args = parser.parse_args(argv)

    entries = yaml.safe_load(args.sources.read_text(encoding="utf-8")) or []
    if args.only:
        entries = [entry for entry in entries if entry.get("name") in set(args.only)]
    items = list(registry_downloads(entries))
    print(f"Checking {len(items)} downloads of {len(entries)} entries", file=sys.stderr)
    lock, done = threading.Lock(), [0]

    def progress(check: DownloadCheck) -> None:
        with lock:
            done[0] += 1
            if check.status in BAD or done[0] % 100 == 0:
                print(
                    f"[{done[0]}] {check.status} {check.http_status} {check.url}", file=sys.stderr
                )

    checks = check_downloads(items, interval=args.interval, timeout=args.timeout, progress=progress)

    args.tsv.parent.mkdir(parents=True, exist_ok=True)
    with args.tsv.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        names = [f.name for f in fields(DownloadCheck)]
        writer.writerow(names)
        for check in checks:
            row = asdict(check)
            writer.writerow(["" if row[n] is None else row[n] for n in names])
    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "sources_file": str(args.sources),
        "downloads": source_status(checks),
    }
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(report, indent=1), encoding="utf-8")
    failing = [check for check in checks if check.status in BAD]
    sources = sorted({check.source for check in failing})
    print(
        f"{len(checks) - len(failing)} of {len(checks)} downloads ok; {len(failing)} failing in "
        f"{len(sources)} entries: {', '.join(sources)}\nTSV: {args.tsv}\nJSON: {args.json}",
        file=sys.stderr,
    )
    return 1 if failing else 0


if __name__ == "__main__":
    raise SystemExit(main())
