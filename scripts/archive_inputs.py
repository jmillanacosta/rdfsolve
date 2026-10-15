"""Keep the pinned downloads of a run as a BagIt bag, for the release (rdfsolve.release.
input_archive), or check a bag against its manifests.

usage: archive_inputs.py RUN_DIR BAG_DIR --workdirs DIR [--location URL] [--copy]
       archive_inputs.py --verify BAG_DIR

RUN_DIR holds <source>/<source>*_inputs.json (the pins); DIR holds the work folders
(<data_dir>/qlever_workdirs). The bag is made of hard links when it is on the same file system,
copies otherwise (--copy always copies). RUN_DIR/input_archive.json is written; build the
release after it, so that the release names the archive. The licence of each source is taken
from RUN_DIR/sources.yaml (bioregistry_license), to show which sources may be deposited.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from rdfsolve.release.input_archive import archive_inputs, verify_archive


def _licenses(run: Path) -> dict[str, str]:
    """Return the licence of each source of the run's registry copy."""
    import yaml

    path = run / "sources.yaml"
    if not path.is_file():
        return {}
    rows = yaml.safe_load(path.read_text(encoding="utf-8"))
    rows = rows.get("sources", []) if isinstance(rows, dict) else rows or []
    return {r["name"]: r["bioregistry_license"] for r in rows if r.get("bioregistry_license")}


def main() -> int:
    """Archive the inputs of a run, or verify a bag."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("run", type=Path, nargs="?")
    parser.add_argument("bag", type=Path, nargs="?")
    parser.add_argument("--workdirs", type=Path)
    parser.add_argument("--location", help="the bag's URL or DOI once deposited")
    parser.add_argument("--copy", action="store_true")
    parser.add_argument("--verify", type=Path, metavar="BAG_DIR")
    args = parser.parse_args()
    if args.verify:
        problems = verify_archive(args.verify)
        sys.stdout.write("".join(f"{p}\n" for p in problems) or "bag matches its manifests\n")
        return 1 if problems else 0
    if not (args.run and args.bag and args.workdirs):
        parser.error("RUN_DIR, BAG_DIR and --workdirs are required")
    record = archive_inputs(
        args.run,
        args.workdirs,
        args.bag,
        location=args.location,
        copy=args.copy,
        licenses=_licenses(args.run),
    )
    summary = {k: record[k] for k in ("location", "file_count", "byte_size", "placed")}
    sys.stdout.write(json.dumps(summary, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
