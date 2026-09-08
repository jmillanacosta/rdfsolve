#!/usr/bin/env python
"""Check a frozen input manifest without downloads or decoded files on disk."""

import argparse
import json
import sys
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from rdfsolve.qlever.inputs import check_cached_input


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if not 1 <= args.workers <= 16:
        parser.error("--workers must be between 1 and 16")
    manifest = json.loads(args.manifest.read_text())
    inputs = manifest["inputs"]
    if not inputs or len({item["backup_path"] for item in inputs}) != len(inputs):
        parser.error("Manifest has no inputs or repeats an input path")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Reserve the report before a long scan. Never replace an existing report.
    with args.output.open("x") as output:
        started = datetime.now(timezone.utc).isoformat()

        def check(item):
            try:
                path = Path(item["backup_path"])
                stat = path.stat()
                if stat.st_size != item["bytes"] or stat.st_mtime_ns != item["mtime_ns"]:
                    raise ValueError("Input differs from the preparation manifest")
                result = asdict(check_cached_input(path))
                return {**result, "source": item["source"], "state": "complete"}
            except (OSError, EOFError, ValueError, zlib.error) as error:
                return {"path": item["backup_path"], "source": item["source"],
                        "state": "failed", "error": str(error)}

        results = []
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for index, result in enumerate(pool.map(check, inputs), 1):
                results.append(result)
                print(json.dumps({"input": index, "total": len(inputs), **result}), flush=True)
        failures = sum(result["state"] != "complete" for result in results)
        report = {
            "state": "failed" if failures else "complete",
            "manifest": str(args.manifest.resolve()),
            "started": started,
            "finished": datetime.now(timezone.utc).isoformat(),
            "checks": "Full stream, gzip CRC, decoded SHA-256; not RDF parsing",
            "input_count": len(inputs),
            "failed_count": failures,
            "stored_bytes": sum(r.get("stored_bytes", 0) for r in results),
            "decoded_bytes": sum(r.get("decoded_bytes", 0) for r in results),
            "inputs": results,
        }
        json.dump(report, output, indent=2)
        output.write("\n")
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
