"""The exact per-term layer of a scan run, written as release files for client-side grouping.

The release ships exact terms (owner decision, 2026-10-06): the JSON schema keeps the default
grouping (rdfsolve's representatives, counted exactly), and two Parquet files per source let a
client group the terms again at any budget or under chosen ancestors without the data
(rdfsolve.client.terms):

``<stem>_terms.parquet``
    one row per (subject term, property, object term or Literal/Resource/BlankNode, datatype,
    graph, subject_binding): triples, distinct_subjects, distinct_objects, counted from the
    per-term type table
    (minimal types when the grouping used them). subject_binding is "untyped" for the rows of
    IRI subjects without a type (subject rdfs:Resource, no class), else "type". The file's metadata (key ``rdfsolve``) holds
    the manifest: the grouping settings, the type table used, and the condition under which
    sums of rows are exact.
``<stem>_term_classes.parquet``
    one row per class of the type table or of its hierarchy: instances (distinct records),
    parents and their source (``data``: an rdfs:subClassOf row; ``file``: a hierarchy file),
    the default representative, whether the class can be grouped, and the groupable classes
    that share records with it (overlaps, overlap_records).

Parquet, not gzip TSV: the IRIs repeat in every row and Parquet's dictionary encoding stores
each once per column chunk (with zstd on top), the counts stay typed integers, the parents are a
list column instead of a second file, and polars, pyarrow, DuckDB and pandas read it directly.
Sizes measured in article/experiments/scan-summary-20261006/terms/ (FINDINGS.md, release sizes).

Exactness of a regrouping from these files: summing the rows of member terms counts a triple
twice when its subject (or object) has two types grouped into one class. With minimal types,
the types of a record are pairwise incomparable, so this needs two groupable types on one
record; the manifest records how many records have that (``records_with_several_groupable_
types``) and the classes table lists the pairs, so a client marks the affected rows.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS

if TYPE_CHECKING:
    import polars as pl

    from rdfsolve.mining.scan import RowStore, StoreView
    from rdfsolve.mining.scan_terms import TermGrouping
    from rdfsolve.schema_models.pattern import SchemaPattern

FORMAT = "rdfsolve-term-release"
VERSION = 1
METADATA_KEY = b"rdfsolve"
TERMS_SUFFIX = "_terms.parquet"
CLASSES_SUFFIX = "_term_classes.parquet"
CONDITION = (
    "Sums of per-term rows give exact triples when no record has two groupable types merged "
    "into one class (records_with_several_groupable_types = 0, or the merge keeps the pairs "
    "listed in overlaps apart); distinct subjects are exact when, in addition, each subject term "
    "contributes one row to the merged row; distinct objects likewise for each object term, "
    "otherwise their sum is an upper bound."
)

__all__ = ["CLASSES_SUFFIX", "TERMS_SUFFIX", "term_rows", "write_term_release"]


def term_rows(patterns: Sequence[SchemaPattern]) -> pl.DataFrame:
    """Return the per-term patterns as rows, one per graph when they were counted per graph."""
    import polars as pl

    rows: list[
        tuple[str, str, str, str | None, str | None, int | None, int | None, int | None, str]
    ] = []
    for p in patterns:
        key = (p.subject_class, p.property_uri, p.object_class, p.datatype)
        if p.graphs:
            for graph, n in sorted(p.graphs.items()):
                rows.append(
                    (
                        *key,
                        graph,
                        n,
                        (p.graph_distinct_subjects or {}).get(graph),
                        (p.graph_distinct_objects or {}).get(graph),
                        p.subject_binding,
                    )
                )
        else:
            rows.append(
                (*key, None, p.count, p.distinct_subjects, p.distinct_objects, p.subject_binding)
            )
    return pl.DataFrame(
        rows,
        schema={
            "subject_class": pl.String,
            "property": pl.String,
            "object_class": pl.String,
            "datatype": pl.String,
            "graph": pl.String,
            "triples": pl.UInt64,
            "distinct_subjects": pl.UInt64,
            "distinct_objects": pl.UInt64,
            "subject_binding": pl.String,
        },
        orient="row",
    ).sort(
        "subject_class",
        "property",
        "object_class",
        "datatype",
        "graph",
        "subject_binding",
        nulls_last=True,
    )


def _write(frame: pl.DataFrame, path: Path, metadata: dict[str, Any] | None = None) -> None:
    import pyarrow.parquet as pq

    table = frame.to_arrow()
    if metadata is not None:
        table = table.replace_schema_metadata(
            {**(table.schema.metadata or {}), METADATA_KEY: json.dumps(metadata).encode()}
        )
    pq.write_table(table, path, compression="zstd", use_dictionary=True)


def write_term_release(
    store: RowStore | StoreView,
    grouping: TermGrouping,
    stem: str | Path,
    *,
    dataset: str | None = None,
) -> dict[str, Any]:
    """Write ``<stem>_terms.parquet`` and ``<stem>_term_classes.parquet``; return the manifest.

    *grouping* is the result of scan_terms.group_terms (its per-term layer, its type table and
    its settings); group_terms(minimal=True) is the release setting, so that a record's types
    are its minimal types and a regrouping can be exact.
    """
    import polars as pl

    from rdfsolve.mining.scan_terms import hierarchy_edges, type_table

    settings = dict(grouping.settings)
    table = (
        (grouping.base_types if grouping.base_types is not None else type_table(store))
        .select("s", "c")
        .unique()
        .filter(pl.col("c").str.starts_with("<"))
        .with_columns(c=pl.col("c").str.strip_prefix("<").str.strip_suffix(">"))
        .collect()
    )
    terms = term_rows(grouping.raw_patterns)
    type_classes = sorted(set(table["c"]))
    edges = hierarchy_edges(
        store,
        type_classes,
        ontology_graph_uris=settings.get("ontology_graph_uris"),
        hierarchy_files=settings.get("hierarchy_files") or (),
    )
    # Classes that a grouping can merge: those with a place in the hierarchy, and the members
    # of the default grouping (shape groups have no parent).
    groupable = set(edges["child"]) | set(edges["parent"]) | set(grouping.representative)
    groupable -= set(_SENTINEL_OBJECTS)
    typed = table.filter(pl.col("c").is_in(sorted(groupable)))
    pairs = (
        typed.join(typed.rename({"c": "other"}), on="s")
        .filter(pl.col("c") != pl.col("other"))
        .group_by("c", "other")
        .agg(records=pl.col("s").n_unique())
        .sort("c", "other")
    )
    several = typed.group_by("s").agg(n=pl.len()).filter(pl.col("n") > 1).height
    instances = table.group_by("c").agg(instances=pl.col("s").n_unique())
    parents = edges.group_by("child").agg(
        parents=pl.col("parent").sort_by("parent"),
        parent_sources=pl.col("source").sort_by("parent"),
    )
    overlaps = pairs.group_by("c").agg(
        overlaps=pl.col("other"), overlap_records=pl.col("records").cast(pl.UInt64)
    )
    names = sorted(
        set(type_classes)
        | set(edges["child"])
        | set(edges["parent"])
        | set(grouping.representative.values())
        | {
            c
            for c in (
                # Untyped subjects have no class: rdfs:Resource is not listed for them.
                *terms.filter(pl.col("subject_binding") != "untyped")["subject_class"],
                *terms["object_class"],
            )
            if c not in _SENTINEL_OBJECTS
        }
    )
    representative = pl.DataFrame(
        {
            "class": list(grouping.representative),
            "representative": list(grouping.representative.values()),
        },
        schema={"class": pl.String, "representative": pl.String},
    )
    classes = (
        pl.DataFrame({"class": names}, schema={"class": pl.String})
        .join(instances.rename({"c": "class"}), on="class", how="left")
        .join(parents.rename({"child": "class"}), on="class", how="left")
        .join(representative, on="class", how="left")
        .join(overlaps.rename({"c": "class"}), on="class", how="left")
        .with_columns(
            instances=pl.col("instances").fill_null(0).cast(pl.UInt64),
            groupable=pl.col("class").is_in(sorted(groupable)),
        )
        .select(
            "class",
            "instances",
            "parents",
            "parent_sources",
            "representative",
            "groupable",
            "overlaps",
            "overlap_records",
        )
    )
    stem = Path(stem)
    terms_path = stem.with_name(stem.name + TERMS_SUFFIX)
    classes_path = stem.with_name(stem.name + CLASSES_SUFFIX)
    manifest: dict[str, Any] = {
        "format": FORMAT,
        "version": VERSION,
        "dataset": dataset,
        "classes_file": classes_path.name,
        "types": "minimal" if settings.get("minimal_types") else "asserted",
        "count_semantics": sorted({p.count_semantics for p in grouping.raw_patterns}),
        "graph_scope": getattr(store, "graph_uris", None),
        "default_grouping": settings,
        "rows": terms.height,
        "classes": classes.height,
        "type_classes": len(type_classes),
        "typed_records": table["s"].n_unique(),
        "groupable_classes": len(groupable),
        "records_with_several_groupable_types": several,
        "groupable_pairs_sharing_records": pairs.height // 2,
        "condition": CONDITION,
        "condition_holds": several == 0,
        "hierarchy_edges": {
            "data": int((edges["source"] == "data").sum()),
            "file": int((edges["source"] == "file").sum()),
        },
    }
    terms_path.parent.mkdir(parents=True, exist_ok=True)
    _write(terms, terms_path, manifest)
    _write(classes, classes_path)
    manifest["files"] = {
        terms_path.name: terms_path.stat().st_size,
        classes_path.name: classes_path.stat().st_size,
    }
    return manifest
