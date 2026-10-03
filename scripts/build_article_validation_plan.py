#!/usr/bin/env python
"""Build deterministic pattern and route checks from a frozen release."""

from __future__ import annotations

import argparse
from pathlib import Path

from rdfsolve.release.model import ReleaseManifest
from rdfsolve.release.scientific_validation import (
    build_scientific_validation_plan,
    write_scientific_validation_plan,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release", type=Path)
    parser.add_argument("--patterns-per-schema", type=int, default=3)
    parser.add_argument("--routes-per-schema", type=int, default=3)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    manifest_path = args.release / "release.json"
    manifest = ReleaseManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    plan = build_scientific_validation_plan(
        manifest,
        args.release,
        patterns_per_schema=args.patterns_per_schema,
        routes_per_schema=args.routes_per_schema,
    )
    target = args.output or args.release / "validation" / "scientific_checks.json"
    write_scientific_validation_plan(plan, target)
    print(
        f"{len(plan.pattern_checks)} pattern checks; {len(plan.route_checks)} route checks -> {target}"
    )


if __name__ == "__main__":
    main()
