#!/usr/bin/env python
"""Read the registry's VoID catalogs, give each described entry its VoID, and measure agreement.

A catalog (registry dataset_kind: catalog, such as okn-void) is not mined: its VoID is read
(from its local files, else its endpoint), split by the datasets it describes and matched to
the registry (rdfsolve.mining.void_catalog). For each catalog this writes, in OUT:

- <catalog>.json: the datasets described, their matches, and per matched entry the file
  <catalog>/<entry>.void.nt with the catalog's VoID of it, which the remote stage reads with
  --void-catalogs (VoID-first: gaps, samples and drift measured on the entry's endpoint);
- <catalog>/<entry>.void_schema.json: the schema read from that VoID;
- <catalog>_agreement.json and .tsv: for each matched entry with a mined schema (given with
  --mined NAME=SCHEMA or --mined-index), its agreement with the VoID
  (rdfsolve.analysis.void_comparison: classes, properties, class-property pairs, patterns,
  pattern counts and class members); the mined schema is cut to the graphs that the catalog
  describes. An entry without a mined schema has the VoID's schema only, marked
  "published VoID, not verified".

usage: void_catalogs.py [--registry SOURCES.yaml] [--catalog NAME] [--mined NAME=SCHEMA]
                        [--mined-index TSV] OUT
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any

from rdflib import Graph

from rdfsolve.analysis.void_comparison import compare_void_with_mined, restrict_to_graphs
from rdfsolve.mining.void_catalog import load_catalogs, write_catalog
from rdfsolve.models.source_model import SourceModel
from rdfsolve.schema_models import MinedSchema
from rdfsolve.schema_models.readers.void import void_graph_to_minedschema
from rdfsolve.sources import load_sources

COLUMNS = [
    "catalog", "dataset", "version", "updated", "entry", "basis", "schema_basis", "mined",
    "void_patterns", "void_classes", "void_properties", "mined_patterns", "classes_both",
    "classes_void_only", "classes_mined_only", "properties_both", "properties_void_only",
    "properties_mined_only", "pairs_both", "pairs_void_only", "pairs_mined_only",
    "patterns_both", "patterns_void_only", "patterns_mined_only", "counts_compared",
    "counts_within_1pct", "counts_within_10pct", "counts_median_ratio", "class_counts_compared",
    "class_counts_within_1pct", "class_counts_median_ratio", "partitions_left_out",
    "left_out_reasons", "findings",
]  # fmt: skip


def _mined_paths(pairs: list[str], index: Path | None) -> dict[str, Path]:
    """Return the mined schema of each entry, from NAME=PATH pairs and a NAME<TAB>PATH file."""
    found = {}
    if index is not None:
        for line in index.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#"):
                name, path = line.split("\t")[:2]
                found[name] = Path(path)
    for pair in pairs:
        name, path = pair.split("=", 1)
        found[name] = Path(path)
    return found


def agreement_row(
    catalog: str,
    entry: SourceModel,
    record: dict[str, Any],
    void: Graph,
    mined_path: Path | None,
    out: Path,
) -> dict[str, Any]:
    """Return one entry's VoID schema statistics and, when it was mined, the agreement."""
    left_out: list[dict[str, Any]] = []
    read = void_graph_to_minedschema(void, report_untyped=False, left_out=left_out)
    reasons: dict[str, int] = {}
    for item in left_out:
        reasons[item["reason"]] = reasons.get(item["reason"], 0) + 1
    schema_path = out / catalog / f"{entry.name}.void_schema.json"
    schema_path.write_text(json.dumps(read.to_dict(), indent=1) + "\n", encoding="utf-8")
    row: dict[str, Any] = {
        "catalog": catalog,
        "dataset": " ".join(record["datasets"]),
        "version": " ".join(v or "" for v in record["versions"]),
        "updated": record["updated"],
        "entry": entry.name,
        "void_patterns": len(read.patterns),
        "void_classes": len(read.get_classes()),
        "void_properties": len(read.get_properties()),
        "schema_basis": "published VoID, not verified"
        + (
            ""
            if record.get("void_first", True)
            else f" (graphs not described: {' '.join(record['graphs_not_described'])})"
        ),
        "mined": "",
        "partitions_left_out": len(left_out),
        "left_out_reasons": "; ".join(f"{r}: {n}" for r, n in sorted(reasons.items())),
        "left_out": left_out,
        "findings": "; ".join(
            f"{n} partitions ({sum(i['triples'] or 0 for i in left_out if i['reason'] == r):,}"
            f" triples) left out, {r}, e.g. "
            f"<{next(i['term'] for i in left_out if i['reason'] == r)}>"
            for r, n in sorted(reasons.items())
        ),
    }
    if mined_path is None or not mined_path.is_file():
        return row
    mined = MinedSchema.from_json(str(mined_path))
    described = {d for d in record["datasets"] if d in entry.graph_uris}
    patterns = restrict_to_graphs(mined.patterns, described) if described else mined.patterns
    comparison = compare_void_with_mined(
        read.patterns,
        patterns,
        void_class_counts=dict(read.about.class_entity_counts),
        mined_class_counts=dict(mined.about.class_entity_counts),
    )
    detail = comparison.model_dump()
    detail["mined_graphs_compared"] = sorted(described) or "all"
    (out / catalog / f"{entry.name}.agreement.json").write_text(
        json.dumps(detail, indent=2) + "\n", encoding="utf-8"
    )
    counts, classes = comparison.counts, comparison.class_counts
    row.update(
        schema_basis="published VoID, compared with the mined schema"
        + ("" if described else " (whole mined schema)"),
        mined=str(mined_path),
        mined_patterns=len(patterns),
        counts_compared=counts.compared,
        counts_within_1pct=counts.within_1_percent,
        counts_within_10pct=counts.within_10_percent,
        counts_median_ratio=counts.median_ratio,
        class_counts_compared=classes.compared if classes else None,
        class_counts_within_1pct=classes.within_1_percent if classes else None,
        class_counts_median_ratio=classes.median_ratio if classes else None,
    )
    for name, part in (
        ("classes", comparison.classes),
        ("properties", comparison.properties),
        ("pairs", comparison.class_properties),
        ("patterns", comparison.patterns),
    ):
        row[f"{name}_both"] = part.both
        row[f"{name}_void_only"] = part.void_only
        row[f"{name}_mined_only"] = part.mined_only
    return row


def main() -> None:
    """Read the catalogs, write each entry's VoID and the agreement report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "sources.yaml",
        help="default: data/sources.yaml of this checkout",
    )
    parser.add_argument("--catalog", action="append", default=[], help="only these catalogs")
    parser.add_argument("--mined", action="append", default=[], help="NAME=SCHEMA_JSON")
    parser.add_argument("--mined-index", type=Path, help="file of NAME<TAB>SCHEMA_JSON lines")
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    registry = load_sources(args.registry)
    selected = [
        s
        for s in registry
        if s.dataset_kind != "catalog" or not args.catalog or s.name in args.catalog
    ]
    mined = _mined_paths(args.mined, args.mined_index)
    by_name = {s.name: s for s in registry}
    for catalog in load_catalogs(selected):
        record = write_catalog(catalog, registry, args.out)
        rows = []
        for name, entry in sorted(record["entries"].items()):
            if not entry.get("void"):
                rows.append(
                    {"catalog": catalog.name, "entry": name, "schema_basis": entry["reason"]}
                )
                continue
            void = Graph().parse(args.out / entry["void"], format="nt")
            row = agreement_row(catalog.name, by_name[name], entry, void, mined.get(name), args.out)
            basis = {m.name: "+".join(m.basis) for d in catalog.datasets for m in d.entries}
            row["basis"] = basis.get(name, "")
            rows.append(row)
        for d in catalog.datasets:
            if not d.entries and not d.describes_catalog:
                rows.append(
                    {
                        "catalog": catalog.name,
                        "dataset": d.iri,
                        "version": d.version,
                        "updated": d.updated,
                        "schema_basis": "described, no registry entry"
                        + (
                            f" (candidates: {', '.join(c.name for c in d.candidates)})"
                            if d.candidates
                            else ""
                        ),
                    }
                )
        (args.out / f"{catalog.name}_agreement.json").write_text(
            json.dumps(rows, indent=2) + "\n", encoding="utf-8"
        )
        with (args.out / f"{catalog.name}_agreement.tsv").open("w", encoding="utf-8") as f:
            writer = csv.DictWriter(f, COLUMNS, delimiter="\t", extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        print(
            f"{catalog.name}: {record['datasets_described']} datasets described, "
            f"{record['datasets_matched']} matched to {len(record['entries_matched'])} entries; "
            f"{sum(bool(r.get('mined')) for r in rows)} compared with a mined schema"
        )


if __name__ == "__main__":
    main()
