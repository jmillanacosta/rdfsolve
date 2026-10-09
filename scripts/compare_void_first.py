#!/usr/bin/env python
"""Compare the schema that the remote channel read from a published VoID with a schema mined from
the same data (rdfsolve.analysis.void_comparison), for each pair given.

Each pair is VOID_SCHEMA=MINED_SCHEMA: the paths of two schema JSON files; their mining reports
(..._report.json beside them) give the cost. The results are written as one JSON document.

A mined schema of a local index (``_local_`` in its file name, as the release names the mode) is
compared with the registry's ``sampled_graphs`` of its source: the counts of a sampled graph are
counts of the sample and are not compared with the VoID's, and the result says so
(mined_count_basis, void_only_absence_supported). The source is the registry entry named by the
schema's dataset_name, or the source of the per-graph schema (rdfsolve.graph_parts) of that name.

usage: compare_void_first.py [--registry SOURCES.yaml] OUT.json VOID_SCHEMA=MINED_SCHEMA [...]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rdfsolve.analysis.void_comparison import compare_void_with_mined
from rdfsolve.graph_parts import graph_parts, has_graph_parts
from rdfsolve.models.source_model import SourceModel
from rdfsolve.schema_models import MinedSchema
from rdfsolve.sources import load_sources


def _report(schema: Path) -> dict | None:
    """Return the mining report beside a schema, if there is one."""
    path = schema.with_name(schema.name.replace("_schema.json", "_report.json"))
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def sampled_graphs_by_name(registry: list[SourceModel]) -> dict[str, dict[str, str]]:
    """Return the sampled graphs of each source and of each per-graph schema of a source."""
    found: dict[str, dict[str, str]] = {}
    for source in registry:
        if not source.sampled_graphs:
            continue
        found[source.name] = dict(source.sampled_graphs)
        if has_graph_parts(source):
            for part in graph_parts(source, registry):
                found.setdefault(part.name, {})
                if part.graph in source.sampled_graphs:
                    found[part.name][part.graph] = source.sampled_graphs[part.graph]
    return found


def main() -> None:
    """Compare each pair and write the results."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "sources.yaml",
        help="default: data/sources.yaml of this checkout",
    )
    parser.add_argument("out", type=Path)
    parser.add_argument("pairs", nargs="+")
    args = parser.parse_args()
    sampled = sampled_graphs_by_name(load_sources(args.registry))
    results = {}
    for pair in args.pairs:
        void_path, mined_path = (Path(p) for p in pair.split("=", 1))
        mined = MinedSchema.from_json(str(mined_path))
        local = "_local_" in mined_path.name
        comparison = compare_void_with_mined(
            MinedSchema.from_json(str(void_path)).patterns,
            mined.patterns,
            void_report=_report(void_path),
            mined_report=_report(mined_path),
            sampled_graphs=sampled.get(mined.about.dataset_name or "") if local else None,
        )
        results[f"{void_path} = {mined_path}"] = comparison.model_dump()
        p, counts = comparison.patterns, comparison.counts
        note = (
            f"; mined from a sample of {len(comparison.sampled_graphs)} graph(s), "
            f"{counts.sampled_not_compared} counts not compared"
            if comparison.sampled_graphs
            else ""
        )
        print(
            f"{void_path.parent.name}: patterns both {p.both}, VoID only {p.void_only}, "
            f"mined only {p.mined_only}; counts within 10% {counts.within_10_percent}"
            f"/{counts.compared}{note}"
        )
    args.out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
