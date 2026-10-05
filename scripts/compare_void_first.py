#!/usr/bin/env python
"""Compare the schema that the remote channel read from a published VoID with a schema mined from
the same data (rdfsolve.analysis.void_comparison), for each pair given.

Each pair is VOID_SCHEMA=MINED_SCHEMA: the paths of two schema JSON files; their mining reports
(..._report.json beside them) give the cost. The results are written as one JSON document.

usage: compare_void_first.py OUT.json VOID_SCHEMA=MINED_SCHEMA [...]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rdfsolve.analysis.void_comparison import compare_void_with_mined
from rdfsolve.schema_models import MinedSchema


def _report(schema: Path) -> dict | None:
    """Return the mining report beside a schema, if there is one."""
    path = schema.with_name(schema.name.replace("_schema.json", "_report.json"))
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def main() -> None:
    """Compare each pair and write the results."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("pairs", nargs="+")
    args = parser.parse_args()
    results = {}
    for pair in args.pairs:
        void_path, mined_path = (Path(p) for p in pair.split("=", 1))
        comparison = compare_void_with_mined(
            MinedSchema.from_json(str(void_path)).patterns,
            MinedSchema.from_json(str(mined_path)).patterns,
            void_report=_report(void_path),
            mined_report=_report(mined_path),
        )
        results[f"{void_path} = {mined_path}"] = comparison.model_dump()
        p = comparison.patterns
        print(
            f"{void_path.parent.name}: patterns both {p.both}, VoID only {p.void_only}, "
            f"mined only {p.mined_only}; counts within 10% {comparison.counts.within_10_percent}"
            f"/{comparison.counts.compared}"
        )
    args.out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
