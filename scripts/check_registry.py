#!/usr/bin/env python
"""Write offline registry findings to a new TSV file."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from rdfsolve.registry_checks import check_registry


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", type=Path)
    parser.add_argument("--overrides", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    findings = check_registry(args.sources, args.overrides)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=["check_id", "sources", "endpoint", "detail", "severity"],
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(findings)
    errors = sum(row["severity"] == "error" for row in findings)
    print(f"{len(findings)} findings; {errors} errors -> {args.output}")
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
