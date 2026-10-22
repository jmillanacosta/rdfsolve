"""Analyse retained schema evidence by extraction and access channel.

A dataset whose local index holds a sample of some graphs (the release record's sampled_graphs)
has local schemas whose counts are counts of the sample: their extraction rows say so
(count_basis), and an absence in them is not evidence (absence_supported), in comparisons and
corpus statistics alike. The remote channel reads the endpoint, which holds the whole data.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import replace
from itertools import combinations
from pathlib import Path
from typing import Any

from rdfsolve.analysis.connectivity import build_connectivity, read_network_paths
from rdfsolve.analysis.io import iter_extractions, read_class_mappings
from rdfsolve.analysis.overlap import jaccard_similarity
from rdfsolve.mappings.instances import read_derived_mappings, read_instance_links
from rdfsolve.release.build import sha256_file
from rdfsolve.release.model import ReleaseManifest
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.core import MinedSchema, iter_section_rows
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.structural import StructuralPattern

_VIEWS = ("patterns", "raw_patterns", "term_patterns", "structural_patterns")


def _terms(rows: Sequence[SchemaPattern | StructuralPattern]) -> dict[str, set[str]]:
    """Collect vocabulary with class and exact-term bindings kept separate."""
    terms: dict[str, set[str]] = {"classes": set(), "predicates": set(), "terms": set()}
    for row in rows:
        terms["predicates"].add(row.property_uri)
        if isinstance(row, SchemaPattern):
            for value, binding in (
                (row.subject_class, row.subject_binding),
                (row.object_class, row.object_binding),
            ):
                if value not in _SENTINEL_OBJECTS and binding != "untyped":
                    terms["terms" if binding == "term" else "classes"].add(value)
    return terms


# The views that can hold far more rows than memory: read row by row from the schema file
# (_streamed_views), never held.
_STREAMED = ("raw_patterns", "term_patterns", "structural_patterns")


def _add_terms(terms: dict[str, set[str]], view: str, row: dict[str, Any]) -> None:
    """Add the vocabulary of one row read from a schema file to *terms* (as _terms)."""
    terms["predicates"].add(row["property_uri"])
    if view == "structural_patterns":
        return
    for value, binding in (
        (row["subject_class"], row.get("subject_binding", "type")),
        (row["object_class"], row.get("object_binding", "type")),
    ):
        if value not in _SENTINEL_OBJECTS and binding != "untyped":
            terms["terms" if binding == "term" else "classes"].add(value)


def _streamed_views(path: Path, excluded: set[str]) -> dict[str, dict[str, Any]]:
    """Return, for each streamed view present in a schema file, its rows, the rows kept once
    the excluded subject classes are left out (patterns views only), the vocabulary of the kept
    rows, and the distinct exact subject shapes (structural census).
    """
    found: dict[str, dict[str, Any]] = {}
    for view, row in iter_section_rows(path, _STREAMED):
        if row is None:
            found[view] = {
                "rows": 0,
                "kept": 0,
                "terms": {"classes": set(), "predicates": set(), "terms": set()},
                "shapes": set(),
            }
            continue
        record = found[view]
        record["rows"] += 1
        if view == "structural_patterns":
            if row.get("shape_semantics", "exact_property_sets") == "exact_property_sets":
                record["shapes"].add(
                    (row.get("graph_uri"), row["subject_kind"], tuple(row["subject_properties"]))
                )
        elif row.get("subject_binding", "type") == "type" and row["subject_class"] in excluded:
            continue
        record["kept"] += 1
        _add_terms(record["terms"], view, row)
    return found


def _view_counts(
    schema: MinedSchema, streamed: dict[str, dict[str, Any]], *, kept: bool
) -> dict[str, dict[str, int] | None]:
    """Count retained rows and graph-local exact subject shapes; the streamed views from
    *streamed* (all their rows, or with *kept* those left after the exclusions).
    """
    result: dict[str, dict[str, int] | None] = {}
    for view in (*_VIEWS, "collections"):
        if view in _STREAMED:
            record = streamed.get(view)
            result[view] = None if record is None else {"rows": record["kept" if kept else "rows"]}
        else:
            rows = getattr(schema, view)
            result[view] = None if rows is None else {"rows": len(rows)}
    structural = streamed.get("structural_patterns")
    if structural is not None:
        result["structural_patterns"] = {
            "rows": structural["rows"],
            "subject_shapes": len(structural["shapes"]),
        }
    return result


def _compare(
    left: dict[str, Any],
    right: dict[str, Any],
    vocabulary: dict[str, dict[str, dict[str, set[str]] | None]],
) -> list[dict[str, Any]]:
    """Compare observed sets without treating incomplete retrieval as absence."""
    result = []
    for view in _VIEWS:
        a = vocabulary[left["schema_path"]][view]
        b = vocabulary[right["schema_path"]][view]
        recorded = a is not None and b is not None
        complete = recorded and left["completion_state"] == right["completion_state"] == "complete"
        sampled = bool(left["sampled_graphs"] or right["sampled_graphs"])
        row: dict[str, Any] = {
            "source_dataset": left["dataset_id"],
            "target_dataset": right["dataset_id"],
            "source_mode": left["mode"],
            "target_mode": right["mode"],
            "source_schema": left["schema_path"],
            "target_schema": right["schema_path"],
            "source_snapshot": left["snapshot_id"],
            "target_snapshot": right["snapshot_id"],
            "view": view,
            "completion_states": [left["completion_state"], right["completion_state"]],
            "comparison_state": "not_recorded"
            if not recorded
            else "complete"
            if complete
            else "incomplete",
            "count_bases": [left["count_basis"], right["count_basis"]],
            "absence_supported": complete and not sampled,
            "shared_count_basis": "unavailable"
            if not recorded
            else "retained_view"
            if complete and not sampled
            else "observed_lower_bound",
        }
        for field in ("classes", "predicates", "terms"):
            first = a[field] if a is not None else set()
            second = b[field] if b is not None else set()
            row[f"shared_{field}"] = len(first & second) if recorded else None
            row[f"observed_{field}_jaccard"] = (
                jaccard_similarity(first, second) if recorded else None
            )
        result.append(row)
    return result


def analyze_release(
    directory: str | Path, *, excluded_subject_classes: frozenset[str] = frozenset()
) -> dict[str, Any]:
    """Read a frozen release and retain each attempt in channel-specific analyses."""
    from networkx import node_link_data

    root = Path(directory).resolve()
    manifest = ReleaseManifest.model_validate_json((root / "release.json").read_text())
    artifacts = {artifact.path: artifact for artifact in manifest.artifacts}

    def verified_path(relative: str) -> Path:
        """Resolve an inventoried artifact and verify its hash."""
        artifact = artifacts.get(relative)
        path = (root / relative).resolve()
        if artifact is None or not path.is_relative_to(root):
            raise ValueError(f"Artifact path differs from release: {relative}")
        if sha256_file(path) != artifact.sha256:
            raise ValueError(f"Artifact hash differs from release: {relative}")
        return path

    kinds = {dataset.dataset_id: dataset.dataset_kind for dataset in manifest.datasets}
    samples = {dataset.dataset_id: dataset.sampled_graphs for dataset in manifest.datasets}
    inventory: list[dict[str, Any]] = []
    schemas: dict[str, MinedSchema] = {}
    streamed: dict[str, dict[str, dict[str, Any]]] = {}
    network: dict[str, list[Any]] = {}
    for dataset, attempt, schema in iter_extractions(root, leave_out=(*_STREAMED, "navigation")):
        # Only an index built from the source's files holds the sample.
        sampled = dict(samples[dataset]) if attempt.mode != "remote" else {}
        row: dict[str, Any] = {
            "dataset_id": dataset,
            "dataset_kind": kinds[dataset],
            **attempt.model_dump(),
            "sampled_graphs": sampled,
            "count_basis": "sample" if sampled else "full_data",
            "views": None,
            "coverage": None,
        }
        if attempt.report_path:
            report = json.loads(verified_path(attempt.report_path).read_text())
            row["coverage"] = report.get("config", {}).get("structural_coverage")
        if schema is not None:
            if attempt.schema_path is None:
                raise ValueError("Schema extraction has no artifact path")
            if attempt.schema_path in schemas:
                raise ValueError(
                    f"Schema used by multiple extraction records: {attempt.schema_path}"
                )
            views = _streamed_views(
                verified_path(attempt.schema_path), set(excluded_subject_classes)
            )
            streamed[attempt.schema_path] = views
            # The paths a network keeps, read from the file without holding all tested paths.
            network[attempt.schema_path] = read_network_paths(verified_path(attempt.schema_path))
            row["retained_views"] = _view_counts(schema, views, kept=False)
            row["coverage_scope"] = "retained_extraction"
            view = schema.model_copy(deep=True)
            excluded_counts: dict[str, int] = {
                name: views[name]["rows"] - views[name]["kept"]
                for name in ("raw_patterns", "term_patterns")
                if name in views
            }
            for name in ("patterns",):
                patterns = getattr(view, name)
                if patterns is not None:
                    kept = [
                        p
                        for p in patterns
                        if p.subject_binding != "type"
                        or p.subject_class not in excluded_subject_classes
                    ]
                    excluded_counts[name] = len(patterns) - len(kept)
                    setattr(view, name, kept)
            if view.collections is not None:
                kept_collections = [
                    p for p in view.collections if p.subject_class not in excluded_subject_classes
                ]
                excluded_counts["collections"] = len(view.collections) - len(kept_collections)
                view.collections = kept_collections
            row["view_exclusions"] = excluded_counts
            schemas[attempt.schema_path] = view
            row["views"] = _view_counts(view, views, kept=True)
        row["coverage_basis"] = (
            "unavailable"
            if row["coverage"] is None
            else "complete_extraction"
            if attempt.completion_state == "complete"
            else "incomplete_extraction"
        )
        inventory.append(row)

    dataset_inventory: list[dict[str, Any]] = []
    for dataset_record in manifest.datasets:
        attempts = [row for row in inventory if row["dataset_id"] == dataset_record.dataset_id]
        dataset_inventory.append(
            {
                "dataset_id": dataset_record.dataset_id,
                "dataset_kind": dataset_record.dataset_kind,
                "extraction_records": len(attempts),
                "schema_extractions": sum(row["views"] is not None for row in attempts),
                "completion_states": dict(Counter(row["completion_state"] for row in attempts)),
                "channels": sorted({row["mode"] for row in attempts}),
            }
        )

    mappings, derived_paths, link_paths = [], [], []
    for artifact in manifest.artifacts:
        relative = Path(artifact.path)
        if relative.parent == Path("mappings/enriched") and relative.name.endswith(".sssom.tsv"):
            mappings.append(verified_path(artifact.path))
        if relative.parent == Path("mappings/derived") and relative.name.endswith(".sssom.tsv"):
            derived_paths.append(verified_path(artifact.path))
        if relative == Path("mappings/derived/instance-links.tsv"):
            link_paths.append(verified_path(artifact.path))
    # Class mappings and record links derived from instances (rdfsolve.mappings.instances),
    # by dataset name.
    derived = [m for path in derived_paths for m in read_derived_mappings(path)]
    derived += [m for path in link_paths for m in read_instance_links(path)]

    channels, graphs = {}, {}
    for mode in sorted({row["mode"] for row in inventory}):
        attempts = [row for row in inventory if row["mode"] == mode]
        selected = {
            row["schema_path"]: schemas[row["schema_path"]]
            for row in attempts
            if row["views"] is not None
        }
        edges, imports = [], {}
        for path in mappings:
            found, report = read_class_mappings(path, selected)
            edges.extend(found)
            imports[str(path.relative_to(root))] = report
        by_name = {
            row["dataset_id"]: row["schema_path"] for row in attempts if row["views"] is not None
        }
        derived_here = [
            replace(
                m,
                source_dataset=by_name[m.source_dataset],
                target_dataset=by_name[m.target_dataset],
            )
            for m in derived
            if m.source_dataset in by_name and m.target_dataset in by_name
        ]
        graph = build_connectivity(
            selected,
            class_mappings=edges,
            derived=derived_here,
            paths={key: network[key] for key in selected},
        )
        metadata = {row["schema_path"]: row for row in attempts}
        for (key, _), data in graph.nodes(data=True):
            data.update(
                dataset=metadata[key]["dataset_id"],
                dataset_kind=metadata[key]["dataset_kind"],
                mode=mode,
                schema_path=key,
                snapshot_id=metadata[key]["snapshot_id"],
                completion_state=metadata[key]["completion_state"],
                count_basis=metadata[key]["count_basis"],
            )
        graph.graph.update(view="patterns", mode=mode, primary_view="pending_review")
        graphs[mode] = node_link_data(graph)
        channels[mode] = {
            "extraction_attempts": len(attempts),
            "completion_states": dict(Counter(row["completion_state"] for row in attempts)),
            "schema_extractions": len(selected),
            "datasets_with_schemas": len(
                {r["dataset_id"] for r in attempts if r["views"] is not None}
            ),
            "class_nodes": len(graph),
            "schema_edges": sum(d["kind"] == "schema" for _, _, d in graph.edges(data=True)),
            "explicit_mapping_edges": len(edges),
            "instance_derived_edges": sum(
                d["kind"] == "instance_derived" and d["edge_type"] == "INSTANCE_DERIVED"
                for _, _, d in graph.edges(data=True)
            ),
            "instance_link_edges": sum(
                d["kind"] == "instance_derived" and d["edge_type"] == "INSTANCE_LINK"
                for _, _, d in graph.edges(data=True)
            ),
            "sampled_schema_extractions": sum(
                bool(r["sampled_graphs"]) for r in attempts if r["views"] is not None
            ),
            "mapping_imports": imports,
        }

    vocabulary = {
        key: {
            view: (streamed[key][view]["terms"] if view in streamed[key] else None)
            if view in _STREAMED
            else _terms(rows)
            if (rows := getattr(schema, view)) is not None
            else None
            for view in _VIEWS
        }
        for key, schema in schemas.items()
    }
    overlaps, comparisons = [], []
    retained = [row for row in inventory if row["views"] is not None]
    for left, right in combinations(retained, 2):
        if left["mode"] == right["mode"]:
            overlaps.extend(_compare(left, right, vocabulary))
        elif left["dataset_id"] == right["dataset_id"]:
            comparisons.extend(_compare(left, right, vocabulary))
    return {
        "paper_statistics": {
            "release_id": manifest.release_id,
            "primary_view": "pending_review",
            "excluded_subject_classes": sorted(excluded_subject_classes),
            "structural_view_scope": "retained_extraction",
            "dataset_unit": "registry_entry",
            "registry_dataset_entries": len(dataset_inventory),
            "dataset_kinds": {
                kind: sum(value == kind for value in kinds.values())
                for kind in ("instance", "ontology", "unknown")
            },
            "service_records": len(manifest.service_records),
            "identity_review_complete": manifest.identity_review_complete,
            "canonical_dataset_count": manifest.canonical_dataset_count,
            "datasets_with_extraction_records": sum(
                row["extraction_records"] > 0 for row in dataset_inventory
            ),
            "datasets_without_extraction_records": sum(
                row["extraction_records"] == 0 for row in dataset_inventory
            ),
            "extraction_attempts": len(inventory),
            "schema_extractions": len(schemas),
            "completion_states": dict(Counter(row["completion_state"] for row in inventory)),
            # Local schemas of these datasets are mined from a sample of the named graphs.
            "sampled_datasets": {
                d: dict(sorted(graphs.items())) for d, graphs in sorted(samples.items()) if graphs
            },
            "sampled_schema_extractions": sum(
                bool(row["sampled_graphs"]) for row in inventory if row["views"] is not None
            ),
            "channels": channels,
        },
        "dataset_inventory": dataset_inventory,
        "extraction_inventory": inventory,
        "schema_overlaps": overlaps,
        "channel_comparisons": comparisons,
        "class_connectivity": graphs,
    }


def write_release_analysis(result: dict[str, Any], output_dir: str | Path) -> None:
    """Write analysis tables without rebuilding their input manifest."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    for name, value in result.items():
        (output / f"{name}.json").write_text(json.dumps(value, indent=2), encoding="utf-8")
