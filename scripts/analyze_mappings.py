#!/usr/bin/env python
"""Compare schemas using explicit class mappings and indexed entity evidence."""

import argparse
import csv
import gzip
import json
from dataclasses import asdict
from pathlib import Path

from networkx import node_link_data

from rdfsolve.analysis import build_connectivity, compare_schemas, load_schemas, read_class_mappings
from rdfsolve.mappings import (
    ClassIndex,
    EntityClassInfo,
    derive_class_mappings,
    shared_entity_links,
)
from rdfsolve.mappings.sssom import project_mappings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("schemas", type=Path)
    parser.add_argument("--instances", type=Path)
    parser.add_argument("--class-mappings", nargs="*", type=Path, default=[])
    parser.add_argument("--entity-mappings", nargs="*", type=Path, default=[])
    parser.add_argument("--min-support", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--extraction-mode", choices=["remote", "local", "grouped", "unknown"])
    args = parser.parse_args()
    schemas = load_schemas(args.schemas, extraction_mode=args.extraction_mode)
    if not schemas:
        parser.error("Supply canonical schema snapshots")
    indices = {}
    for path in sorted(args.instances.glob("*_instances.tsv.gz")) if args.instances else []:
        name = path.name.removesuffix("_instances.tsv.gz")
        if name not in schemas:
            parser.error(f"Instance dump has no selected schema: {name}")
        data = indices[name] = ClassIndex(endpoint_url="urn:saved-index")
        with gzip.open(path, "rt") as stream:
            for row in csv.DictReader(stream, delimiter="\t"):
                iri, cls = row["instance_iri"], row["class_iri"]
                info = data.entities.setdefault(iri, EntityClassInfo(entity_iri=iri))
                classes = info.graph_classes.setdefault(name, [])
                if cls not in classes:
                    classes.append(cls)
    if args.entity_mappings and not indices:
        parser.error("Entity mappings need instance dumps with type evidence")
    associations, shared = shared_entity_links(
        indices, min_instance_count=args.min_support, include_witnesses=True
    )
    witness_rows = list(shared.pop("witnesses", []))
    mappings, reports = [], {"shared_entities": shared}
    for path in args.class_mappings:
        edges, reports[str(path)] = read_class_mappings(path, schemas)
        mappings.extend(edges)
    for path in args.entity_mappings:
        edges, report = project_mappings(
            path, {name: set(i.entities) for name, i in indices.items()}
        )
        pairs, report["derivation"] = derive_class_mappings(
            edges,
            indices,
            min_instance_count=args.min_support,
            include_witnesses=True,
        )
        witness_rows.extend(report["derivation"].pop("witnesses", []))
        associations.extend(pairs)
        reports[str(path)] = report
    graph = build_connectivity(schemas, class_mappings=mappings, associations=associations)
    args.output.mkdir(parents=True, exist_ok=True)
    for name, value in (
        ("class_connectivity", node_link_data(graph)),
        ("schema_overlaps", compare_schemas(schemas)),
        ("class_associations", [asdict(p) for p in associations]),
        ("evidence_report", reports),
    ):
        (args.output / f"{name}.json").write_text(json.dumps(value, indent=2, default=sorted))
    if witness_rows:
        witness_path = args.output / "class_association_witnesses.tsv.gz"
        fields = [
            "source_dataset",
            "source_class",
            "target_dataset",
            "target_class",
            "source_entity",
            "target_entity",
            "supporting_entity_predicates",
        ]
        with gzip.open(witness_path, "wt", newline="") as stream:
            writer = csv.DictWriter(stream, delimiter="\t", fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            for row in witness_rows:
                out = dict(row)
                out["supporting_entity_predicates"] = json.dumps(
                    out.get("supporting_entity_predicates") or [], separators=(",", ":")
                )
                writer.writerow({field: out.get(field, "") for field in fields})
    print(
        f"{len(schemas)} schemas, {len(mappings)} explicit class links, {len(associations)} entity-supported associations"
    )


if __name__ == "__main__":
    main()
