"""The files of a saved schema: a JSON summary, and Parquet files for the rows that grow with
the data.

``<stem>_schema.json``
    The schema without its per-row detail, written without indentation. ``details`` names the
    files that hold the rest, with their rows; navigation keeps the path the network keeps for
    each class pair (``paths_scope`` "network").
``<stem>_terms.parquet``
    The patterns before grouping (``section`` "raw") and the exact term bindings (``section``
    "term"): one row per pattern and graph in the columns of the term release
    (rdfsolve.mining.term_release.term_rows), so that a client regroups the raw rows as before;
    ``pattern`` numbers the patterns of a section, and ``fields`` (on the first row of a
    pattern) holds its other fields as JSON, without those at their default. The manifest of
    the term release, when there is one, is the file's metadata.
``<stem>_paths.parquet``
    The tested navigation paths, one row per path, as the numbers of its steps in the edge
    table of the file's metadata (as navigation.NavigationSummary packs them in JSON);
    ``detour`` marks a path through a membership or annotation property or back to a class it
    has passed, ``network`` the path the network keeps for its class pair
    (analysis.connectivity.network_paths).

write_schema writes the three from a schema in memory; repair_schema writes them from a schema
JSON of the earlier layout, streamed, through the same writer, so that both give the same files.
A full read (MinedSchema.from_json) puts the rows back into the model.

A census row whose subjects were selected as untyped keeps no covered types: they select the
rows of uncovered subjects only, and are the schema's own typed keys of the property.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.navigation import NavigationPath

SCHEMA_SUFFIX = "_schema.json"
TERMS_SUFFIX = "_terms.parquet"
PATHS_SUFFIX = "_paths.parquet"
PATHS_FORMAT = "rdfsolve-tested-paths"
PATHS_VERSION = 1
TERMS_VERSION = 2
METADATA_KEY = b"rdfsolve"
# The sections written beside the JSON, by their key in MinedSchema.details.
TERM_SECTIONS = {"raw_patterns": "raw", "term_patterns": "term"}
PATHS_SECTION = "navigation.paths"
# The key of a pattern in the term release's columns; the other fields go to ``fields``.
_KEY_FIELDS = ("subject_class", "property_uri", "object_class", "datatype", "subject_binding")
# Paths written to a Parquet file a batch at a time.
_PATH_BATCH = 1_000_000

__all__ = [
    "PATHS_SUFFIX",
    "TERMS_SUFFIX",
    "PackedPaths",
    "iter_detail_rows",
    "load_details",
    "packed_paths",
    "repair_schema",
    "write_schema",
]


def stem_of(path: str | Path) -> Path:
    """Return the stem of a schema JSON path (``<stem>_schema.json``)."""
    path = Path(path)
    if not path.name.endswith(SCHEMA_SUFFIX):
        msg = f"A schema file name ends with {SCHEMA_SUFFIX}: {path}"
        raise ValueError(msg)
    return path.with_name(path.name.removesuffix(SCHEMA_SUFFIX))


class PackedPaths:
    """Tested paths as navigation.NavigationSummary packs them: the edge table (written steps),
    the graphs shared by all paths, and the rows (edge numbers, matched, sources, at). *rows*
    gives a new iterator on each call, so that the paths can be read twice without being held.
    """

    def __init__(
        self,
        edges: list[dict[str, Any]],
        rows: Callable[[], Iterable[Mapping[str, Any]]],
        graph_uris: list[str],
        type_context_graph_uris: list[str],
    ) -> None:
        """Hold the edge table, the rows and the shared graphs of tested paths."""
        self.edges = edges
        self.rows = rows
        self.graph_uris = graph_uris
        self.type_context_graph_uris = type_context_graph_uris

    @classmethod
    def from_written(cls, packed: Mapping[str, Any]) -> PackedPaths:
        """Return the packed paths of a written navigation summary (its "paths" member)."""
        rows = list(packed.get("rows", []))
        return cls(
            list(packed.get("edges", [])),
            lambda: rows,
            list(packed.get("graph_uris", [])),
            list(packed.get("type_context_graph_uris", [])),
        )

    def path(self, row: Mapping[str, Any]) -> NavigationPath:
        """Return the path of one row (as NavigationSummary reads packed paths)."""
        from rdfsolve.schema_models.navigation import NavigationPath

        return NavigationPath.model_validate(
            {
                "evidence": "instance_tested",
                "instance_support": "matched",
                "graph_uris": self.graph_uris,
                "type_context_graph_uris": self.type_context_graph_uris,
                "steps": [self.edges[i] for i in row["edges"]],
                "matched_sources": row.get("matched"),
                "source_count": row.get("sources"),
                "observed_at": row.get("at"),
            }
        )


def _detours(edges: list[dict[str, Any]]) -> Callable[[list[int]], bool]:
    """Return a test of whether a path (edge numbers) is a detour (connectivity._is_detour)."""
    from rdfsolve.schema_models.navigation import NON_RELATION_PROPERTIES

    relation = [e.get("property_uri") not in NON_RELATION_PROPERTIES for e in edges]

    def detour(ids: list[int]) -> bool:
        """Return whether the path of edge numbers *ids* is a detour."""
        if not all(relation[i] for i in ids):
            return True
        classes = [edges[ids[0]]["subject_class"], *(edges[i]["object_class"] for i in ids)]
        return len(set(classes)) < len(classes)

    return detour


def _network(packed: PackedPaths) -> list[int]:
    """Return the numbers of the rows the network keeps, in the order of network_paths: per
    (start class, end class), the shortest path that is no detour, the one with the most
    matched instances among equals, the first found among those.
    """
    detour = _detours(packed.edges)
    best: dict[tuple[str, str], tuple[int, int, int]] = {}
    for number, row in enumerate(packed.rows()):
        ids = row["edges"]
        if detour(ids):
            continue
        key = (packed.edges[ids[0]]["subject_class"], packed.edges[ids[-1]]["object_class"])
        rank = (len(ids), -(row.get("matched") or 0))
        kept = best.get(key)
        if kept is None or rank < kept[:2]:
            best[key] = (*rank, number)
    return [best[key][2] for key in sorted(best)]


def _paths_table(
    packed: PackedPaths,
    rows: list[Mapping[str, Any]],
    numbers: set[int],
    start: int,
    detour: Callable[[list[int]], bool],
) -> pa.Table:
    import pyarrow as pa

    return pa.table(
        {
            "edges": pa.array([list(r["edges"]) for r in rows], pa.list_(pa.uint32())),
            "matched": pa.array([r.get("matched") for r in rows], pa.uint64()),
            "sources": pa.array([r.get("sources") for r in rows], pa.uint64()),
            "at": pa.array([r.get("at") for r in rows], pa.string()),
            "detour": pa.array([detour(list(r["edges"])) for r in rows], pa.bool_()),
            "network": pa.array([start + i in numbers for i in range(len(rows))], pa.bool_()),
        }
    )


def _write_paths(packed: PackedPaths, target: Path) -> tuple[int, list[NavigationPath]]:
    """Write *packed* to *target*; return its rows and the network's paths."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from rdfsolve.schema_models.pattern import SchemaPattern

    # The steps as this version writes them, whichever version wrote the paths.
    packed = PackedPaths(
        [SchemaPattern.model_validate(e).model_dump(mode="json") for e in packed.edges],
        packed.rows,
        packed.graph_uris,
        packed.type_context_graph_uris,
    )
    network = _network(packed)
    numbers = set(network)
    detour = _detours(packed.edges)
    metadata = {
        "format": PATHS_FORMAT,
        "version": PATHS_VERSION,
        "edges": packed.edges,
        "graph_uris": packed.graph_uris,
        "type_context_graph_uris": packed.type_context_graph_uris,
    }
    kept: dict[int, NavigationPath] = {}
    schema = _paths_table(packed, [], numbers, 0, detour).schema.with_metadata(
        {METADATA_KEY: json.dumps(metadata).encode()}
    )
    written = 0
    batch: list[Mapping[str, Any]] = []
    with pq.ParquetWriter(target, schema, compression="zstd", use_dictionary=True) as writer:

        def flush() -> None:
            """Write the batch of rows."""
            nonlocal written
            if batch:
                writer.write_table(
                    _paths_table(packed, batch, numbers, written, detour).cast(schema)
                )
                written += len(batch)
                batch.clear()

        for number, row in enumerate(packed.rows()):
            if number in numbers:
                kept[number] = packed.path(row)
            batch.append(row)
            if len(batch) >= _PATH_BATCH:
                flush()
        flush()
    return written, [kept[number] for number in network]


def _defaults() -> dict[str, Any]:
    """Return the written default of each field of a pattern that has one."""
    from pydantic_core import PydanticUndefined

    from rdfsolve.schema_models.pattern import SchemaPattern

    found = {}
    for name, field in SchemaPattern.model_fields.items():
        default = field.get_default(call_default_factory=True)
        if default is not PydanticUndefined:
            found[name] = getattr(default, "value", default)  # an enum is written as its value
    return found


def _fields(row: Mapping[str, Any], defaults: Mapping[str, Any]) -> str:
    """Return the fields of a written pattern other than its key, without those at default."""
    out = {
        name: value
        for name, value in row.items()
        if name not in _KEY_FIELDS and (name not in defaults or value != defaults[name])
    }
    return json.dumps(out, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def _term_frame(sections: Mapping[str, Iterable[Mapping[str, Any]]]) -> tuple[Any, dict[str, int]]:
    """Return the rows of the terms file for the written patterns of each section, and the
    patterns of each section (term_release.term_rows, plus section, pattern and fields).
    """
    import polars as pl

    from rdfsolve.schema_models.pattern import SchemaPattern

    columns: dict[str, list[Any]] = {
        name: []
        for name in (
            "subject_class",
            "property",
            "object_class",
            "datatype",
            "graph",
            "triples",
            "distinct_subjects",
            "distinct_objects",
            "subject_binding",
            "section",
            "pattern",
            "fields",
        )
    }
    counts: dict[str, int] = {}
    defaults = _defaults()

    def add(*values: Any) -> None:
        """Append one row."""
        for column, value in zip(columns.values(), values, strict=True):
            column.append(value)

    for name, rows in sections.items():
        section = TERM_SECTIONS[name]
        number = -1
        for number, given in enumerate(rows):
            row = SchemaPattern.model_validate(given).model_dump(mode="json")
            key = (row["subject_class"], row["property_uri"], row["object_class"], row["datatype"])
            fields: str | None = _fields(row, defaults)
            graphs = row.get("graphs")
            if graphs:
                subjects = row.get("graph_distinct_subjects") or {}
                objects = row.get("graph_distinct_objects") or {}
                for graph, n in sorted(graphs.items()):
                    add(
                        *key,
                        graph,
                        n,
                        subjects.get(graph),
                        objects.get(graph),
                        row["subject_binding"],
                        section,
                        number,
                        fields,
                    )
                    fields = None
            else:
                add(
                    *key,
                    None,
                    row.get("count"),
                    row.get("distinct_subjects"),
                    row.get("distinct_objects"),
                    row["subject_binding"],
                    section,
                    number,
                    fields,
                )
        counts[name] = number + 1
    frame = pl.DataFrame(
        columns,
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
            "section": pl.String,
            "pattern": pl.UInt32,
            "fields": pl.String,
        },
    )
    return frame, counts


def _manifest(path: Path) -> dict[str, Any] | None:
    """Return the term release manifest kept in a terms file's metadata, if any."""
    import pyarrow.parquet as pq

    if not path.is_file():
        return None
    raw = (pq.read_schema(path).metadata or {}).get(METADATA_KEY)
    return json.loads(raw) if raw else None


def _write_terms(frame: Any, target: Path, manifest: dict[str, Any] | None) -> None:
    import pyarrow.parquet as pq

    table = frame.to_arrow()
    metadata = {**(manifest or {}), "version": TERMS_VERSION, "layout": "schema-sections"}
    metadata.setdefault("format", "rdfsolve-term-release")
    table = table.replace_schema_metadata({METADATA_KEY: json.dumps(metadata).encode()})
    pq.write_table(table, target, compression="zstd", use_dictionary=True)


def _census_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return a written census row without covered types when its subjects are untyped."""
    if row.get("subject_selection", "all") != "uncovered" and row.get("covered_types"):
        return {**row, "covered_types": []}
    return row


def _write_parts(
    summary: MinedSchema,
    target: Path,
    *,
    terms: Mapping[str, Iterable[Mapping[str, Any]]],
    paths: PackedPaths | None,
    manifest: dict[str, Any] | None = None,
) -> MinedSchema:
    """Write the summary JSON and the detail files; return the summary written.

    *summary* has no raw or term patterns and no tested paths; *terms* gives the written rows of
    each term section that the schema has, *paths* the tested paths. The terms file keeps
    *manifest* (a term release's), else the manifest of the terms file it replaces.
    """
    from rdfsolve.schema_models.core import DetailFile
    from rdfsolve.schema_models.structural import StructuralPattern

    stem = stem_of(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    details: dict[str, DetailFile] = {}
    update: dict[str, Any] = {"raw_patterns": None, "term_patterns": None}
    if terms:
        terms_path = stem.with_name(stem.name + TERMS_SUFFIX)
        frame, counts = _term_frame(terms)
        _write_terms(frame, terms_path, manifest or _manifest(terms_path))
        for name, rows in counts.items():
            details[name] = DetailFile(file=terms_path.name, rows=rows)
    if paths is not None and summary.navigation is not None:
        paths_path = stem.with_name(stem.name + PATHS_SUFFIX)
        rows, network = _write_paths(paths, paths_path)
        details[PATHS_SECTION] = DetailFile(file=paths_path.name, rows=rows)
        update["navigation"] = summary.navigation.model_copy(
            update={"paths": network, "paths_scope": "network"}
        )
    if summary.structural_patterns:
        update["structural_patterns"] = [
            StructuralPattern.model_validate(_census_row(p.model_dump(mode="json")))
            for p in summary.structural_patterns
        ]
    update["details"] = details or None
    written = summary.model_copy(update=update)
    target.write_text(
        json.dumps(written.to_dict(), separators=(",", ":"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return written


def write_schema(
    schema: MinedSchema, target: str | Path, *, trim_descriptions: int | None = None
) -> MinedSchema:
    """Write *schema* as ``<stem>_schema.json`` and its detail files; return the summary written
    (the schema the exports of these files are made from).
    """
    from rdfsolve.schema_models.exporters.text import trim_descriptions as trim

    schema = trim(schema, trim_descriptions)
    terms = {
        name: [p.model_dump(mode="json") for p in rows]
        for name in TERM_SECTIONS
        if (rows := getattr(schema, name)) is not None
    }
    paths = None
    navigation = schema.navigation
    if navigation is not None and navigation.strategy == "tested" and navigation.paths:
        paths = PackedPaths.from_written(navigation.model_dump(mode="json")["paths"])
        navigation = navigation.model_copy(update={"paths": []})
    summary = schema.model_copy(
        update={"raw_patterns": None, "term_patterns": None, "navigation": navigation}
    )
    return _write_parts(summary, Path(target), terms=terms, paths=paths)


# Reading


def _schema_member(path: Path, name: str) -> Any:
    """Return one member of the schema object of a schema JSON file (None when absent)."""
    import ijson

    with path.open("rb") as stream:
        return next(iter(ijson.items(stream, f"schema.{name}", use_float=True)), None)


def _term_patterns(path: Path, section: str) -> Iterator[dict[str, Any]]:
    """Yield the written patterns of one section of a terms file, in their order."""
    import pyarrow.parquet as pq

    from rdfsolve.schema_models.pattern import SchemaPattern

    columns = [
        "subject_class",
        "property",
        "object_class",
        "datatype",
        "subject_binding",
        "section",
        "fields",
    ]
    file = pq.ParquetFile(path)
    for batch in file.iter_batches(columns=columns, batch_size=200_000):
        for row in batch.to_pylist():
            if row["section"] != section or row["fields"] is None:
                continue
            yield SchemaPattern.model_validate(
                {
                    "subject_class": row["subject_class"],
                    "property_uri": row["property"],
                    "object_class": row["object_class"],
                    "datatype": row["datatype"],
                    "subject_binding": row["subject_binding"],
                    **json.loads(row["fields"]),
                }
            ).model_dump(mode="json")


def iter_detail_rows(
    path: str | Path, details: Mapping[str, Any], names: Iterable[str]
) -> Iterator[tuple[str, dict[str, Any] | None]]:
    """Yield the rows of the named term sections that *details* places in files, as
    core.iter_section_rows yields them from the JSON: (name, None) first, then each row.
    """
    folder = Path(path).parent
    for name in names:
        detail = details.get(name)
        if name not in TERM_SECTIONS or detail is None:
            continue
        file = detail["file"] if isinstance(detail, Mapping) else detail.file
        yield name, None
        for row in _term_patterns(folder / file, TERM_SECTIONS[name]):
            yield name, row


def _paths_file(path: Path) -> PackedPaths:
    import pyarrow.parquet as pq

    file = pq.ParquetFile(path)
    metadata = json.loads(file.schema_arrow.metadata[METADATA_KEY])

    def rows() -> Iterator[dict[str, Any]]:
        """Yield the rows of the file."""
        for batch in file.iter_batches(columns=["edges", "matched", "sources", "at"]):
            yield from batch.to_pylist()

    return PackedPaths(
        metadata["edges"], rows, metadata["graph_uris"], metadata["type_context_graph_uris"]
    )


def packed_paths(path: str | Path) -> PackedPaths | None:
    """Return all tested paths of a saved schema, packed: from its paths file, or from the JSON
    of the earlier layout. None when its paths are not tested paths.
    """
    path = Path(path)
    details = _schema_member(path, "details") or {}
    detail = details.get(PATHS_SECTION)
    if detail is not None:
        return _paths_file(path.parent / detail["file"])
    if _schema_member(path, "navigation.paths.format") is None:
        return None
    edges = _schema_member(path, "navigation.paths.edges") or []
    graphs = list(_schema_member(path, "navigation.paths.graph_uris") or [])
    context = list(_schema_member(path, "navigation.paths.type_context_graph_uris") or [])

    def rows() -> Iterator[dict[str, Any]]:
        """Yield the packed rows of the JSON, streamed."""
        import ijson

        with path.open("rb") as stream:
            yield from ijson.items(stream, "schema.navigation.paths.rows.item", use_float=True)

    return PackedPaths(edges, rows, graphs, context)


def load_details(
    schema: MinedSchema, path: str | Path, leave_out: Iterable[str] = ()
) -> MinedSchema:
    """Return *schema* (read from *path*) with the rows of its detail files put back, except
    those of the sections in *leave_out* (navigation for the paths).
    """
    from rdfsolve.schema_models.pattern import SchemaPattern

    if not schema.details:
        return schema
    path = Path(path)
    skipped = set(leave_out)
    update: dict[str, Any] = {}
    for name, section in TERM_SECTIONS.items():
        detail = schema.details.get(name)
        if detail is not None and name not in skipped:
            update[name] = [
                SchemaPattern.model_validate(row)
                for row in _term_patterns(path.parent / detail.file, section)
            ]
    detail = schema.details.get(PATHS_SECTION)
    if detail is not None and "navigation" not in skipped and schema.navigation is not None:
        packed = _paths_file(path.parent / detail.file)
        update["navigation"] = schema.navigation.model_copy(
            update={"paths": [packed.path(row) for row in packed.rows()], "paths_scope": "all"}
        )
    if not update:
        return schema
    return schema.model_copy(update={**update, "details": None})


# Repair of a schema JSON of the earlier layout


def repair_schema(
    source: str | Path, target: str | Path, *, terms_file: str | Path | None = None
) -> MinedSchema:
    """Write the schema JSON *source* (all sections in the JSON) as *target* and its detail
    files, reading it as a stream: the summary is held, the rows of the term sections and the
    tested paths are read from *source* as they are written. The files are those write_schema
    gives for the schema read whole. The terms file keeps the term release manifest of
    *terms_file* (by default the terms file beside *source*). Return the summary written.
    """
    import ijson
    from ijson.common import ObjectBuilder

    from rdfsolve.schema_models.core import MinedSchema, _schema_sections, iter_section_rows

    source = Path(source)
    with source.open("rb") as stream:
        sections = _schema_sections(
            stream, frozenset({*TERM_SECTIONS, "structural_patterns", "navigation"})
        )
    if sections.get("details"):
        msg = f"{source} already has detail files"
        raise ValueError(msg)
    census: list[dict[str, Any]] | None = None
    for _, row in iter_section_rows(source, ["structural_patterns"]):
        if row is None:
            census = []
        elif census is not None:
            census.append(_census_row(row))
    sections["structural_patterns"] = census
    present = {name for name, row in iter_section_rows(source, list(TERM_SECTIONS)) if row is None}

    def term_rows(name: str) -> Iterator[dict[str, Any]]:
        """Yield the written rows of one term section of *source*."""
        for found, row in iter_section_rows(source, [name]):
            if row is not None and found == name:
                yield row

    # The navigation summary without its paths, built from the stream; the paths stay packed.
    navigation: dict[str, Any] | None = None
    packed: Any = None
    with source.open("rb") as stream:
        events = ijson.parse(stream, use_float=True)
        for prefix, event, value in events:
            if prefix != "schema" or event != "map_key" or value != "navigation":
                continue
            _, first, _ = next(events)
            if first != "start_map":
                break
            navigation = {}
            for inner_prefix, inner, key in events:
                if inner == "end_map" and inner_prefix == "schema.navigation":
                    break
                builder = ObjectBuilder()
                _, start, start_value = next(events)
                builder.event(start, start_value)
                depth = 1 if start in ("start_map", "start_array") else 0
                skip = key == "paths" and start == "start_map"
                while depth:
                    _, nested, nested_value = next(events)
                    if not skip:
                        builder.event(nested, nested_value)
                    depth += nested in ("start_map", "start_array")
                    depth -= nested in ("end_map", "end_array")
                if skip:
                    packed = True
                else:
                    navigation[key] = builder.value
            break
    paths = None
    if navigation is not None:
        if packed:
            paths = packed_paths(source)
            navigation["paths"] = []
        sections["navigation"] = navigation
    summary = MinedSchema.from_dict(
        {"format": "rdfsolve.mined-schema", "version": 1, "schema": sections}
    )
    if (
        paths is None
        and summary.navigation is not None
        and summary.navigation.paths
        and (summary.navigation.strategy == "tested")
    ):
        # Tested paths written in full by an earlier version: packed as write_schema packs them.
        paths = PackedPaths.from_written(summary.navigation.model_dump(mode="json")["paths"])
        summary = summary.model_copy(
            update={"navigation": summary.navigation.model_copy(update={"paths": []})}
        )
    terms = {name: term_rows(name) for name in TERM_SECTIONS if name in present}
    if terms_file is None:
        terms_file = stem_of(source).with_name(stem_of(source).name + TERMS_SUFFIX)
    manifest = _manifest(Path(terms_file))
    return _write_parts(summary, Path(target), terms=terms, paths=paths, manifest=manifest)
