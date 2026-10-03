#!/usr/bin/env python
"""Execute saved scientific checks for one dataset and extraction mode."""

from __future__ import annotations

import argparse
from pathlib import Path

from rdfsolve.release.scientific_execution import execute_scientific_validation_plan
from rdfsolve.release.scientific_validation import ScientificValidationPlan
from rdfsolve.sparql_helper import SparqlHelper


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--mode", choices=["remote", "local", "grouped"], required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--index-reference")
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = ScientificValidationPlan.model_validate_json(args.plan.read_text())
    with args.output.open("x", encoding="utf-8") as output:
        with SparqlHelper(args.endpoint, timeout=args.timeout, max_retries=1, inter_request_delay=2) as helper:
            results = execute_scientific_validation_plan(
                plan, helper, dataset_id=args.dataset, extraction_mode=args.mode,
                index_reference=args.index_reference,
            )
        output.write(results.model_dump_json(indent=2) + "\n")
    print(f"{len(results.results)} checks -> {args.output}")
    if any(row.state in {"partial", "timeout", "error"} or row.comparison == "differs" for row in results.results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
