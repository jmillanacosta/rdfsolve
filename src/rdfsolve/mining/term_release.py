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


def _coded_types(types: pl.LazyFrame) -> tuple[pl.DataFrame, list[str]]:
    """Return the type table by integer codes, and its IRI classes (bare, sorted).

    The table has one row per (record, class): r is the record (QLever's id sid when the
    table has it, else the term s), k the index of its class in the list. Only the classes
    written as <iri> are kept. The text of the records is never held: on allie (58 million
    membership rows) the table by text took 4.4 GB and the self-join of its records 10 GB more
    (article/corpus/agent-findings/term-release-memory.md).
    """
    import polars as pl

    record = "sid" if "sid" in types.collect_schema().names() else "s"
    found = (
        types.select("c")
        .filter(pl.col("c").str.starts_with("<"))
        .unique()
        .with_columns(bare=pl.col("c").str.strip_prefix("<").str.strip_suffix(">"))
        .collect(engine="streaming")
    )
    classes = sorted(set(found["bare"]))
    index = {c: k for k, c in enumerate(classes)}
    codes = pl.LazyFrame(
        {"c": found["c"], "k": [index[c] for c in found["bare"]]},
        schema={"c": pl.String, "k": pl.UInt32},
    )
    table = (
        types.select(record, "c")
        .join(codes, on="c")
        .select(r=record, k="k")
        .unique()
        .collect(engine="streaming")
    )
    return table, classes


def _named(frame: pl.DataFrame, classes: list[str], **columns: str) -> pl.DataFrame:
    """Replace the class codes of *frame* (column: new name) by the classes' IRIs."""
    import polars as pl

    names = pl.Series(classes, dtype=pl.String)
    return frame.with_columns([names.gather(frame[code]).alias(code) for code in columns]).rename(
        columns
    )


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

    if grouping.record_kinds is not None:
        return _write_record_release(store, grouping, stem, dataset=dataset)
    settings = dict(grouping.settings)
    table, type_classes = _coded_types(
        grouping.base_types if grouping.base_types is not None else type_table(store)
    )
    terms = (
        term_rows(grouping.raw_patterns)
        if grouping.raw_rows is None
        else pl.read_parquet(grouping.raw_rows).sort(
            "subject_class",
            "property",
            "object_class",
            "datatype",
            "graph",
            "subject_binding",
            nulls_last=True,
        )
    )
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
    typed = table.filter(
        pl.col("k").is_in([k for k, c in enumerate(type_classes) if c in groupable])
    )
    # Only the records with several groupable types make pairs: the others are not joined.
    multiple = typed.group_by("r").agg(n=pl.len()).filter(pl.col("n") > 1).select("r")
    several = multiple.height
    shared = typed.join(multiple, on="r", how="semi")
    del typed, multiple
    # One row per (record, class): the rows of a pair are its distinct records.
    pairs = _named(
        shared.join(shared.rename({"k": "other"}), on="r")
        .filter(pl.col("k") != pl.col("other"))
        .group_by("k", "other")
        .agg(records=pl.len()),
        type_classes,
        k="c",
        other="other",
    ).sort("c", "other")
    del shared
    instances = _named(table.group_by("k").agg(instances=pl.len()), type_classes, k="c")
    typed_records = table["r"].n_unique()
    del table
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
        "count_semantics": sorted(
            {p.count_semantics for p in grouping.raw_patterns or grouping.patterns}
        ),
        "graph_scope": getattr(store, "graph_uris", None),
        "default_grouping": settings,
        "rows": terms.height,
        "classes": classes.height,
        "type_classes": len(type_classes),
        "typed_records": typed_records,
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


def _bare_column(name: str) -> pl.Expr:
    """Return column *name* with the angle brackets of its IRIs removed."""
    import polars as pl

    return pl.col(name).str.strip_prefix("<").str.strip_suffix(">")


def _write_record_release(
    store: RowStore | StoreView,
    grouping: TermGrouping,
    stem: str | Path,
    *,
    dataset: str | None = None,
) -> dict[str, Any]:
    """write_term_release for records kept as classes (grouping.record_kinds), streamed.

    The same two files and columns, made from lazy tables and written by sinks: BioGateway has
    65.9 million record classes, too many to hold as Python lists or SchemaPattern objects.
    Each record class has its rdfs:subClassOf parents in the data as parents (source "data")
    and its kind's representative as representative; the other classes (the kinds and the
    classes without a parent, few) go through hierarchy_edges as in write_term_release. Rows
    are written in the order they were counted, not sorted (``row_order`` in the manifest).
    """
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq

    from rdfsolve.mining.scan_terms import _record_parents, hierarchy_edges

    if grouping.record_kinds is None or grouping.raw_rows is None or grouping.base_types is None:
        msg = "A release of records as classes needs the record kinds, rows and type table"
        raise ValueError(msg)
    settings = dict(grouping.settings)
    types = grouping.base_types
    record = "sid" if "sid" in types.collect_schema().names() else "s"
    table = (
        types.filter(pl.col("c").str.starts_with("<"))
        .select(r=record, c=_bare_column("c"))
        .unique()
    )
    kinds = grouping.record_kinds.select(c=_bare_column("c"), kind=_bare_column("kind"))
    records = kinds.select("c")
    # The parents in the data of each record class (all of them; its kind is the first).
    data_edges = (
        _record_parents(store, grouping.base_types)
        .select(child=_bare_column("c"), parent=_bare_column("kind"))
        .join(records.rename({"c": "child"}), on="child", how="semi")
        .with_columns(source=pl.lit("data"))
    )
    # The other classes of the table and the parents of the records: few, read as for any
    # source (followed to the roots, hierarchy files).
    others = (
        pl.concat(
            [
                table.select("c").join(records, on="c", how="anti"),
                data_edges.select(c="parent"),
            ]
        )
        .unique()
        .collect(engine="streaming")["c"]
        .to_list()
    )
    small = hierarchy_edges(
        store,
        others,
        ontology_graph_uris=settings.get("ontology_graph_uris"),
        hierarchy_files=settings.get("hierarchy_files") or (),
    )
    edges = pl.concat([data_edges, small.lazy()]).unique()
    representative = pl.DataFrame(
        {
            "class": list(grouping.representative),
            "representative": list(grouping.representative.values()),
        },
        schema={"class": pl.String, "representative": pl.String},
    ).lazy()
    # A record class takes the representative of its kind (its kind when it has none).
    record_representative = kinds.join(
        representative.rename({"class": "kind"}), on="kind", how="left"
    ).select(**{"class": pl.col("c"), "representative": pl.coalesce("representative", "kind")})
    representatives = pl.concat(
        [
            record_representative,
            representative.join(records.rename({"c": "class"}), on="class", how="anti"),
        ]
    )
    sentinels = sorted(_SENTINEL_OBJECTS)
    groupable = (
        pl.concat(
            [
                edges.select(c="child"),
                edges.select(c="parent"),
                representative.select(c="class"),
                records,
            ]
        )
        .unique()
        .filter(~pl.col("c").is_in(sentinels))
    )
    typed = table.join(groupable, on="c", how="semi")
    multiple = typed.group_by("r").agg(n=pl.len()).filter(pl.col("n") > 1).select("r")
    shared = typed.join(multiple, on="r", how="semi").collect(engine="streaming")
    several = shared["r"].n_unique()
    pairs = (
        shared.join(shared.rename({"c": "other"}), on="r")
        .filter(pl.col("c") != pl.col("other"))
        .group_by("c", "other")
        .agg(records=pl.len())
        .sort("c", "other")
    )
    del shared
    overlaps = pairs.group_by("c").agg(
        overlaps=pl.col("other"), overlap_records=pl.col("records").cast(pl.UInt64)
    )
    terms = pl.scan_parquet(grouping.raw_rows)
    names = (
        pl.concat(
            [
                table.select("c"),
                edges.select(c="child"),
                edges.select(c="parent"),
                representatives.select(c="representative"),
                # Untyped subjects have no class: rdfs:Resource is not listed for them.
                terms.filter(pl.col("subject_binding") != "untyped").select(c="subject_class"),
                terms.select(c="object_class"),
            ]
        )
        .unique()
        .filter(~pl.col("c").is_in(sentinels))
        .rename({"c": "class"})
    )
    parents = edges.group_by("child").agg(
        parents=pl.col("parent").sort_by("parent"),
        parent_sources=pl.col("source").sort_by("parent"),
    )
    classes = (
        names.join(
            table.group_by("c").agg(instances=pl.len()).rename({"c": "class"}),
            on="class",
            how="left",
        )
        .join(parents.rename({"child": "class"}), on="class", how="left")
        .join(representatives, on="class", how="left")
        .join(overlaps.lazy().rename({"c": "class"}), on="class", how="left")
        .join(
            groupable.select(**{"class": pl.col("c"), "groupable": pl.lit(True)}),
            on="class",
            how="left",
        )
        .select(
            "class",
            instances=pl.col("instances").fill_null(0).cast(pl.UInt64),
            parents="parents",
            parent_sources="parent_sources",
            representative="representative",
            groupable=pl.col("groupable").fill_null(False),
            overlaps="overlaps",
            overlap_records="overlap_records",
        )
    )
    stem = Path(stem)
    terms_path = stem.with_name(stem.name + TERMS_SUFFIX)
    classes_path = stem.with_name(stem.name + CLASSES_SUFFIX)
    terms_path.parent.mkdir(parents=True, exist_ok=True)
    classes.sink_parquet(classes_path, compression="zstd")

    def count(frame: pl.LazyFrame) -> int:
        """Return the rows of *frame*, counted by a stream."""
        return int(frame.select(pl.len()).collect(engine="streaming").item())

    source = pq.ParquetFile(grouping.raw_rows)
    manifest: dict[str, Any] = {
        "format": FORMAT,
        "version": VERSION,
        "dataset": dataset,
        "classes_file": classes_path.name,
        "types": "minimal",
        "count_semantics": sorted(
            {p.count_semantics for p in grouping.raw_patterns or grouping.patterns}
        ),
        "graph_scope": getattr(store, "graph_uris", None),
        "default_grouping": settings,
        "rows": source.metadata.num_rows,
        "row_order": "as counted",
        "records_as_classes": count(records),
        "classes": pq.ParquetFile(classes_path).metadata.num_rows,
        "type_classes": count(table.select("c").unique()),
        "typed_records": count(table.select("r").unique()),
        "groupable_classes": count(groupable),
        "records_with_several_groupable_types": several,
        "groupable_pairs_sharing_records": pairs.height // 2,
        "condition": CONDITION,
        "condition_holds": several == 0,
        "hierarchy_edges": {
            name: count(edges.filter(pl.col("source") == name)) for name in ("data", "file")
        },
    }
    schema = source.schema_arrow.with_metadata(
        {**(source.schema_arrow.metadata or {}), METADATA_KEY: json.dumps(manifest).encode()}
    )
    with pq.ParquetWriter(terms_path, schema, compression="zstd", use_dictionary=True) as out:
        for batch in source.iter_batches(batch_size=1 << 20):
            out.write_table(pa.Table.from_batches([batch], schema=schema))
    manifest["files"] = {
        terms_path.name: terms_path.stat().st_size,
        classes_path.name: classes_path.stat().st_size,
    }
    return manifest
