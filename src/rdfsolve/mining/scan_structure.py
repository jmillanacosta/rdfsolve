"""The outputs of mining beyond the patterns, counted by Polars from a row store (scan mining).

Each function returns the object that the SPARQL miner produces, with the definitions of its
queries:

- structural_census: the census of each property, the coverage record and the structural
  patterns (structural_strategy: _census_queries, _property_discovery, _mine_graph);
- class_entity_counts: distinct members of each class (pattern_enrichment
  .query_class_entity_counts);
- class_extensions: the members each class shares with every class, related by
  schema_models.class_extensions.relate (mining/class_extensions.py);
- dataset_statistics: triples, distinct subjects and objects, overall and by property
  (mining/dataset_statistics.py);
- property_usage_evidence: class/property support, node kinds, literal profiles and value-count
  histograms (evidence/observed.collect_property_usage_evidence);
- iri_findings: schema_models.iri_quality.findings on the schema, and data_iri_findings, the
  terms of the store that its listing queries would return.

Distinct objects are counted by the value's id (oid), as QLever counts them; distinct subjects
by the term, which TSV writes in full for IRIs and blank nodes. Each function takes the store,
or a view of it in the graph scope of the run (scan.StoreView), and follows the scope as the
miner's query does. The work goes one predicate at a time; the property sets of nodes and the distinct subjects of the whole store are grouped in
buckets of nodes, so that memory is bound by the largest predicate or bucket, not by the store.
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from rdfsolve.mining.query_builders import MEMBERSHIP
from rdfsolve.mining.scan import (
    QLEVER_DEFAULT_GRAPH,
    RDF_LANG,
    XSD_STRING,
    RowStore,
    StoreView,
    _pattern_term,
)
from rdfsolve.schema_models.class_extensions import ClassExtensions, relate
from rdfsolve.schema_models.structural import StructuralPattern

if TYPE_CHECKING:
    import polars as pl

    from rdfsolve._outcomes import QueryState
    from rdfsolve.evidence.observed import PropertyUsageCollection
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern

__all__ = [
    "class_entity_counts",
    "class_extensions",
    "data_iri_findings",
    "dataset_statistics",
    "iri_findings",
    "property_usage_evidence",
    "structural_census",
    "structural_coverage",
    "typed_keys",
]

BUCKETS = 8
# Shapes (distinct subject and object property sets) of one property above which its uncovered
# edges are described by their kind, datatype and language alone (shape_semantics
# "property_profile"), with exact counts: Oregano (job 115896) has no class, and its nodes'
# property sets gave 1,714,912 structural patterns, which the outputs could not hold in 64 GB.
STRUCTURAL_SHAPES_PER_PROPERTY = 200
_KIND = {"iri": "IRI", "bnode": "BlankNode", "literal": "Literal"}


def _bare(expr: pl.Expr) -> pl.Expr:
    return expr.str.strip_prefix("<").str.strip_suffix(">")


def _types(store: RowStore | StoreView) -> pl.LazyFrame:
    """Return the members of each class (s, c) with bare class IRIs (a blank-node class keeps _:),
    read from the data and type context graphs of the scope.
    """
    import polars as pl

    return store.types().select("s", c=_bare(pl.col("c"))).unique()


def _members(store: RowStore | StoreView) -> pl.LazyFrame:
    """Return the members of each class as _subject_type_pattern counts them (bare IRIs)."""
    import polars as pl

    return store.members().select("s", c=_bare(pl.col("c"))).unique()


def _language() -> pl.Expr:
    """LANG(?o): the tag of a tagged literal, "" for another literal, unbound otherwise."""
    import polars as pl

    return (
        pl.when(pl.col("kind") != "literal")
        .then(pl.lit(None, pl.String))
        .when(pl.col("d") == RDF_LANG)
        .then(pl.col("o").str.extract(r"@([A-Za-z0-9-]+)$", 1))
        .otherwise(pl.lit(""))
    )


def _distinct(frames: Iterable[pl.LazyFrame], column: str, buckets: int) -> int:
    """Count the distinct values of *column* over *frames*, one bucket of values at a time."""
    import polars as pl

    frames = [f.select(column) for f in frames]
    if not frames:
        return 0
    total = 0
    for bucket in range(buckets):
        total += (
            pl.concat(frames)
            .filter(pl.col(column).hash() % buckets == bucket)
            .unique()
            .select(pl.len())
            .collect(engine="streaming")
            .item()
        )
    return int(total)


# Dataset statistics, class counts, class extensions


def dataset_statistics(
    store: RowStore, *, buckets: int = BUCKETS, source: str = "QLever index"
) -> dict[str, Any]:
    """Return the record of mining/dataset_statistics.count_dataset (report config
    dataset_statistics, and about.property_partitions).

    The triples of each property, and the distinct subjects and objects of each property and of
    the whole store.
    """
    import polars as pl

    graphs = getattr(store, "graph_uris", None)
    if graphs:
        # count_dataset counts the default graph, the whole index, and only when the graph
        # scope holds every triple of the named graphs (GRAPH ?_g: not QLever's own default
        # graph, which holds the input without a graph).
        outside = sum(
            n
            for graph, held in (store.graphs or {}).items()
            if graph not in graphs and graph != QLEVER_DEFAULT_GRAPH
            for n in held.values()
        )
        if outside:
            return {
                "state": "not_counted",
                "reason": "the graph scope does not hold the whole index",
                "triples_outside_scope": outside,
            }
        store = store.base
    partitions: dict[str, dict[str, int]] = {}
    for predicate in store.predicates:
        row = (
            store.rows(predicate)
            .select(
                triples=pl.len(),
                distinct_subjects=pl.col("s").n_unique(),
                distinct_objects=pl.col("oid").n_unique(),
            )
            .collect(engine="streaming")
            .row(0, named=True)
        )
        partitions[predicate] = {k: int(v) for k, v in row.items()}
    frames = [store.rows(p) for p in store.predicates]
    return {
        "state": "counted",
        "source": source,
        "triples": sum(p["triples"] for p in partitions.values()),
        "distinct_subjects": _distinct(frames, "s", buckets),
        "distinct_objects": _distinct(frames, "oid", buckets),
        "distinct_properties": len(partitions),
        "property_partitions": dict(sorted(partitions.items())),
        "refused_partitions": {},
    }


def class_entity_counts(
    store: RowStore, classes: Sequence[str]
) -> tuple[dict[str, int], dict[str, QueryState]]:
    """Return the distinct members of each class and the state of each count (about
    .class_entity_counts and .class_entity_count_states); a class without members counts 0.
    In a graph scope, the members of query_class_entity_counts (StoreView.members).
    """
    import polars as pl

    wanted = sorted(set(map(str, classes)))
    found = dict(
        _members(store)
        .filter(pl.col("c").is_in(wanted))
        .group_by("c")
        .agg(n=pl.col("s").n_unique())
        .collect(engine="streaming")
        .iter_rows()
    )
    counts = {c: int(found.get(c, 0)) for c in wanted}
    states: dict[str, QueryState] = dict.fromkeys(counts, "complete")
    return counts, states


def class_extensions(store: RowStore, classes: Sequence[Any]) -> ClassExtensions:
    """Return the relations between the member sets of *classes* (schema.class_extensions).

    A class that stands for a group of ontology terms (it has members) is not measured, as in
    mining/class_extensions.measure_class_extensions.
    """
    import polars as pl

    not_checked = {
        str(c): "a group of ontology terms" for c in classes if getattr(c, "members", None)
    }
    measured = sorted({str(c) for c in classes} - set(not_checked))
    members = _members(store).filter(pl.col("c").is_in(measured))
    pairs = (
        members.join(members.rename({"c": "other"}), on="s")
        .group_by("c", "other")
        .agg(n=pl.len())
        .collect(engine="streaming")
    )
    overlaps: dict[str, dict[str, int]] = {c: {} for c in measured}
    for cls, other, n in pairs.iter_rows():
        overlaps[cls][other] = int(n)
    found = relate(overlaps)
    found.not_checked = dict(sorted(not_checked.items()))
    return found


# Structural census and patterns


def typed_keys(patterns: Iterable[SchemaPattern]) -> list[tuple[str, str, str, str | None]]:
    """Return the typed profiles that cover edges, as StructuralStrategy.mine selects them."""
    return sorted(
        {
            (p.subject_class, p.property_uri, p.object_class, p.datatype)
            for p in patterns
            if p.subject_binding == p.object_binding == "type" and p.evidence_source == "mined"
        },
        key=str,
    )


def _membership_keys(
    store: RowStore | StoreView, keys: list[tuple[str, str, str, str | None]], types: pl.LazyFrame
) -> list[tuple[str, str, str, str | None]]:
    """Add the (C, rdf:type, Resource) profiles that the miner leaves out of its patterns.

    The two-phase strategy finds them, and the census counts their edges as covered; the
    patterns drop them afterwards (miner.py, membership rows). A class has the row when one of
    its members has a membership edge to an IRI without a type. The classes are every IRI type
    value of the store: the two-phase strategy mines each discovered class, also one whose only
    rows are these (owl:Class, for a class that has no other edge).
    """
    import polars as pl

    classes = types.filter(~pl.col("c").str.starts_with("_:")).select("c").unique()
    added = set(keys)
    typed_objects = types.select(o="s").unique()
    for prop in MEMBERSHIP.get():
        if prop not in store.predicates:
            continue
        found = (
            store.rows(prop)
            .filter(pl.col("kind") == "iri")
            .join(typed_objects, on="o", how="anti")
            .join(types.join(classes, on="c", how="semi"), on="s")
            .select("c")
            .unique()
            .collect(engine="streaming")
        )
        added |= {(c, prop, "Resource", None) for (c,) in found.iter_rows()}
    return sorted(added, key=str)


def _covered(
    rows: pl.LazyFrame, types: pl.LazyFrame, keys: list[tuple[str, str, str, str | None]]
) -> pl.LazyFrame:
    """Return the edges (s, o) of one property that a typed profile covers (typed_coverage.typed_match).

    An edge is covered when a type T of its subject has a profile (T, p, X) that the object
    matches: X a type of the object, X = Literal with the object's datatype (or any datatype),
    X = Resource for an IRI without a type, X = BlankNode for a blank node.
    """
    import polars as pl

    typed = [(s, o) for s, _, o, _ in keys if o not in ("Literal", "Resource", "BlankNode")]
    exact = [(s, dt) for s, _, o, dt in keys if o == "Literal" and dt is not None]
    any_literal = sorted({s for s, _, o, dt in keys if o == "Literal" and dt is None})
    resource = sorted({s for s, _, o, _ in keys if o == "Resource"})
    blank = sorted({s for s, _, o, _ in keys if o == "BlankNode"})
    subjects = sorted({k[0] for k in keys})
    edges = rows.join(types.filter(pl.col("c").is_in(subjects)).rename({"c": "t"}), on="s")
    parts = []
    if typed:
        pairs = pl.LazyFrame(typed, schema={"t": pl.String, "x": pl.String}, orient="row")
        parts.append(
            edges.filter(pl.col("kind") != "literal")
            .join(types.rename({"s": "o", "c": "x"}), on="o")
            .join(pairs, on=["t", "x"], how="semi")
        )
    if exact:
        pairs = pl.LazyFrame(exact, schema={"t": pl.String, "d": pl.String}, orient="row")
        literal = edges.filter(pl.col("kind") == "literal")
        parts.append(literal.join(pairs, on=["t", "d"], how="semi"))
    if any_literal:
        parts.append(edges.filter((pl.col("kind") == "literal") & pl.col("t").is_in(any_literal)))
    if resource:
        parts.append(
            edges.filter((pl.col("kind") == "iri") & pl.col("t").is_in(resource)).join(
                types.select(o="s").unique(), on="o", how="anti"
            )
        )
    if blank:
        parts.append(edges.filter((pl.col("kind") == "bnode") & pl.col("t").is_in(blank)))
    if not parts:
        return rows.select("s", "o").head(0)
    return pl.concat([p.select("s", "o") for p in parts]).unique()


def _property_sets(
    store: RowStore | StoreView, nodes: pl.LazyFrame, work: Path, buckets: int
) -> pl.LazyFrame:
    """Return the exact property set of each of *nodes* (column s): its outgoing predicates, sorted
    and joined by ">" (no IRI holds it), grouped one bucket of nodes at a time.
    """
    import polars as pl

    pairs, sets = work / "node-properties", work / "property-sets"
    pairs.mkdir()
    sets.mkdir()
    for number, predicate in enumerate(store.predicates):
        (
            store.rows(predicate)
            .select("s")
            .unique()
            .join(nodes, on="s", how="semi")
            .with_columns(p=pl.lit(predicate), b=pl.col("s").hash() % buckets)
            .sink_parquet(pairs / f"{number:05d}.parquet")
        )
    for bucket in range(buckets):
        (
            pl.scan_parquet(pairs / "*.parquet")
            .filter(pl.col("b") == bucket)
            .group_by("s")
            .agg(ps=pl.col("p").unique().sort().str.join(">"))
            .sink_parquet(sets / f"{bucket:03d}.parquet")
        )
    return pl.scan_parquet(sets / "*.parquet")


def _lexical(term: str) -> tuple[str, str | None]:
    """Return the lexical form and language of a literal written as in TSV or N-Triples."""
    if not term.startswith('"'):
        return term, None  # QLever writes numbers, dates and booleans bare
    quote = '"""' if term.startswith('"""') and len(term) >= 6 else '"'
    end = term.rindex(quote)
    body = term[len(quote) : end]
    suffix = term[end + len(quote) :]
    escapes = {'\\"': '"', "\\\\": "\\", "\\n": "\n", "\\t": "\t", "\\r": "\r"}
    body = re.sub(r'\\["\\ntr]', lambda m: escapes[m.group(0)], body)
    return body, suffix[1:] if suffix.startswith("@") else None


def _binding(term: str, datatype: str | None) -> dict[str, str]:
    """Write a term as a SPARQL JSON binding (the witness of a structural pattern)."""
    if term.startswith("<"):
        return {"type": "uri", "value": term[1:-1]}
    if term.startswith("_:"):
        return {"type": "bnode", "value": term[2:]}
    value, language = _lexical(term)
    binding = {"type": "literal", "value": value}
    if language:
        binding["xml:lang"] = language
    elif datatype and datatype != XSD_STRING:
        binding["datatype"] = datatype
    return binding


def structural_census(
    store: RowStore | StoreView,
    patterns: Iterable[SchemaPattern],
    *,
    buckets: int = BUCKETS,
    work_dir: Path | None = None,
) -> tuple[dict[str, Any], list[StructuralPattern]]:
    """Return the coverage record (report config structural_coverage[0]) and the structural
    patterns of the edges that no typed profile of *patterns* covers, in the default graph.

    The census counts, for each property, its triples, the triples of subjects without a type
    (no membership edge) and the uncovered triples. The uncovered edges are grouped by the exact
    property sets of subject and object (every outgoing predicate, membership included; none for
    a literal), the node kinds, the datatype (as the index gives it) and the language: triples,
    distinct subjects and distinct objects of each group, as the recount query of each pattern
    (structural_strategy.structural_queries) gives them. The census is recorded as "scan".
    A graph scope has one record for each data graph: structural_coverage.
    """
    if getattr(store, "graph_uris", None):
        raise ValueError("A graph scope has a census for each data graph: structural_coverage")
    types = _types(store)
    keys = _membership_keys(store, typed_keys(patterns), types)
    return _census_graph(
        store, keys, types, graph=None, named=[], context=[], buckets=buckets, work_dir=work_dir
    )


def structural_coverage(
    store: RowStore | StoreView,
    patterns: Iterable[SchemaPattern],
    *,
    buckets: int = BUCKETS,
    work_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], list[StructuralPattern]]:
    """Return the coverage records (report config structural_coverage) and the structural
    patterns, as StructuralStrategy.mine gives them in the scope of the run.

    In the default graph, one record (structural_census). In a graph scope, one record for each
    data graph, in the order of their IRIs: its edges (``FROM <graph>``), the property sets of
    its nodes read in that graph alone, the types read from the data and type context graphs
    (the covered profiles, typed_coverage.typed_match, and the untyped subjects); the patterns
    name the graph, the type graphs and the type context graphs.
    """
    graphs = getattr(store, "graph_uris", None)
    if not graphs or not isinstance(store, StoreView):
        entry, found = structural_census(store, patterns, buckets=buckets, work_dir=work_dir)
        return [entry], found
    types = _types(store)
    keys = _membership_keys(store, typed_keys(patterns), types)
    entries: list[dict[str, Any]] = []
    structural: list[StructuralPattern] = []
    for number, graph in enumerate(sorted(set(graphs))):
        entry, found = _census_graph(
            store.for_graph(graph),
            keys,
            types,
            graph=graph,
            named=list(store.type_graph_uris),
            context=list(store.context),
            buckets=buckets,
            work_dir=None if work_dir is None else Path(work_dir) / f"graph-{number:04d}",
        )
        entries.append(entry)
        structural.extend(found)
    return entries, structural


def _census_graph(
    store: RowStore | StoreView,
    keys: list[tuple[str, str, str, str | None]],
    types: pl.LazyFrame,
    *,
    graph: str | None,
    named: list[str],
    context: list[str],
    buckets: int,
    work_dir: Path | None,
) -> tuple[dict[str, Any], list[StructuralPattern]]:
    """Census and structural patterns of the edges of *store* (the default graph or one data
    graph), with *types* read from the type graphs of the scope.
    """
    import polars as pl

    from rdfsolve.mining.structural_strategy import structural_queries

    members = types.select("s").unique()
    owned = tempfile.mkdtemp(prefix="scan-structure-") if work_dir is None else None
    work = Path(owned or work_dir)  # type: ignore[arg-type]
    try:
        uncovered_dir = work / "uncovered"
        uncovered_dir.mkdir(parents=True)
        census: dict[str, dict[str, Any]] = {}
        files: dict[str, Path] = {}
        for number, predicate in enumerate(sorted(store.predicates)):
            rows = store.rows(predicate)
            own = [k for k in keys if k[1] == predicate]
            covered = _covered(rows, types, own)
            path = uncovered_dir / f"{number:05d}.parquet"
            rows.join(covered, on=["s", "o"], how="anti").sink_parquet(path)
            # Two plans, not one collect_all over a shared scan: collect_all panicked in Polars'
            # plan formatting on rdfportal.chembl (job 115861, "index out of bounds: the len
            # is 0 but the index is 0" in polars-plan ir/format.rs) and ended the job.
            triples = rows.select(pl.len()).collect()
            untyped = rows.join(members, on="s", how="anti").select(pl.len()).collect()
            missing = pl.scan_parquet(path).select(pl.len()).collect().item()
            census[predicate] = {
                "triples": int(triples.item()),
                "untypedTriples": int(untyped.item()),
                "uncoveredTriples": int(missing),
            }
            if missing:
                files[predicate] = path
        total = sum(n["triples"] for n in census.values())
        untyped = sum(n["untypedTriples"] for n in census.values())
        missing_total = sum(n["uncoveredTriples"] for n in census.values())
        entry: dict[str, Any] = {
            "graph_uri": graph,
            "state": "needed" if missing_total else "not_needed",
            "census": "scan",
            "census_properties": census,
            "triple_count": total,
            "untyped_subject_triples": untyped,
            "excluded_subject_triples": 0,
            "covered_triples": total - missing_total,
            "uncovered_triples": missing_total,
            "unchecked_triples": 0,
            "type_graph_uris": named,
            "subject_selection": "uncovered",
        }
        if not missing_total:
            return entry, []
        uncovered = [pl.scan_parquet(p) for p in files.values()]
        nodes = pl.concat(
            [f.select("s") for f in uncovered]
            + [f.filter(pl.col("kind") != "literal").select(s="o") for f in uncovered]
        ).unique()
        sets = _property_sets(store, nodes, work, buckets)
        candidates: dict[str, StructuralPattern] = {}
        profiled: dict[str, int] = {}
        # Uncovered edges whose kind or property set holds a term that cannot be one.
        left_out: dict[str, int] = {}
        for predicate, path in files.items():
            edges = (
                pl.scan_parquet(path)
                .join(sets.rename({"ps": "ss"}), on="s", how="left")
                .join(sets.rename({"s": "o", "ps": "os"}), on="o", how="left")
                .with_columns(
                    os=pl.when(pl.col("kind") == "literal")
                    .then(pl.lit(""))
                    .otherwise(pl.col("os").fill_null("")),
                    sk=pl.when(pl.col("s").str.starts_with("_:"))
                    .then(pl.lit("BlankNode"))
                    .otherwise(pl.lit("IRI")),
                    dt=pl.when(pl.col("kind") == "literal").then(pl.col("d")),
                    lang=_language(),
                )
            )
            measures = {
                "n": pl.len(),
                "subjects": pl.col("s").n_unique(),
                "objects": pl.col("oid").n_unique(),
                "ws": pl.col("s").first(),
                "wo": pl.col("o").first(),
            }
            group = (
                edges.group_by("ss", "os", "sk", "kind", "dt", "lang")
                .agg(**measures)
                .collect(engine="streaming")
            )
            semantics = "exact_property_sets"
            if group.height > STRUCTURAL_SHAPES_PER_PROPERTY:
                # Too many shapes: one row per kind, datatype and language, with exact counts,
                # and the properties that every node of the row has (_shared_properties).
                profiled[predicate] = group.height
                semantics = "property_profile"
                shared = _shared_properties(group)
                group = (
                    edges.group_by("sk", "kind", "dt", "lang")
                    .agg(**measures)
                    .collect(engine="streaming")
                )
                keys_of = [_profile_key(row) for row in group.iter_rows(named=True)]
                group = group.with_columns(
                    ss=pl.Series([shared[k][0] for k in keys_of], dtype=pl.String),
                    os=pl.Series([shared[k][1] for k in keys_of], dtype=pl.String),
                )
            n = census[predicate]
            selection = "untyped" if n["untypedTriples"] == n["uncoveredTriples"] else "uncovered"
            for row in group.iter_rows(named=True):
                terms = [p for p in f"{row['ss'] or ''}>{row['os'] or ''}".split(">") if p]
                if row["kind"] not in _KIND or not all(_pattern_term(t) for t in terms):
                    # A row whose kind or property is not a term (an empty or NUL-filled
                    # string): left out and reported, not a KeyError that fails the source
                    # (rdfportal.oma, job 115902: "KeyError: ''").
                    shown = repr(f"{predicate} {row['kind']} {row['ss']} {row['os']}"[:200])
                    left_out[shown] = left_out.get(shown, 0) + int(row["n"])
                    continue
                pattern = StructuralPattern(
                    subject_properties=[p for p in (row["ss"] or "").split(">") if p],
                    object_properties=[p for p in row["os"].split(">") if p],
                    subject_kind=row["sk"],
                    object_kind=_KIND[row["kind"]],
                    property_uri=predicate,
                    datatype=row["dt"],
                    language=row["lang"],
                    graph_uri=graph,
                    type_graph_uris=named,
                    object_type_graph_uris=context,
                    covered_types=[(s, o, dt) for s, p, o, dt in keys if p == predicate],
                    subject_selection=selection,
                    shape_semantics=semantics,
                    count=0,
                    distinct_subjects=0,
                    distinct_objects=0,
                    witness_query="",
                    recount_query="",
                )
                key = json.dumps(pattern.model_dump(), sort_keys=True)
                pattern.witness_query, pattern.recount_query = structural_queries(pattern)
                pattern.count = int(row["n"])
                pattern.distinct_subjects = int(row["subjects"])
                pattern.distinct_objects = int(row["objects"])
                pattern.examples = [
                    {"s": _binding(row["ws"], None), "o": _binding(row["wo"], row["dt"])}
                ]
                candidates[key] = pattern
        structural = [candidates[k] for k in sorted(candidates)]
        if sum(p.count for p in structural) + sum(left_out.values()) != missing_total:
            raise ValueError("Structural patterns do not account for the uncovered edges")
        if left_out:
            entry["invalid_terms"] = {
                "triples_left_out": sum(left_out.values()),
                "rows": dict(sorted(left_out.items())),
            }
        entry.update(
            undiscovered_triples=0,
            state="complete",
            pattern_count=len(structural),
            representation="property_profile" if profiled else "exact_property_sets",
        )
        if profiled:
            # The properties described by kind, datatype and language, with their shapes.
            entry["profiled_properties"] = dict(sorted(profiled.items()))
        return entry, structural
    finally:
        if owned:
            shutil.rmtree(owned, ignore_errors=True)


def _profile_key(key: dict[str, Any]) -> tuple[Any, ...]:
    """Return the key of a profile row (subject kind, object kind, datatype, language)."""
    return (key["sk"], key["kind"], key["dt"], key["lang"])


def _shared_properties(group: Any) -> dict[tuple[Any, ...], tuple[str, str]]:
    """Return, for each profile row, the subject and object properties of all its shapes.

    *group* has one row per shape (ss, os: property sets joined by ">"); a profile row
    (_profile_key) keeps the properties that each of its shapes has, so that they hold for
    every subject and object it counts.
    """
    shared: dict[tuple[Any, ...], list[set[str]]] = {}
    for row in group.iter_rows(named=True):
        key = _profile_key(row)
        subject = {p for p in (row["ss"] or "").split(">") if p}
        objects = {p for p in (row["os"] or "").split(">") if p}
        if key in shared:
            shared[key][0] &= subject
            shared[key][1] &= objects
        else:
            shared[key] = [subject, objects]
    return {key: (">".join(sorted(s)), ">".join(sorted(o))) for key, (s, o) in shared.items()}


# Property usage evidence


def property_usage_evidence(
    store: RowStore | StoreView,
    *,
    dataset_id: str,
    classes: list[str],
    class_entity_counts: dict[str, int] | None,
    class_entity_count_states: dict[str, str] | None = None,
    graph_uris: list[str] | None = None,
    batch_size: int = 10,
    collect_node_kinds: bool = True,
    collect_datatypes: bool = True,
    collect_histograms: bool = False,
    shared_extensions: dict[str, str] | None = None,
) -> PropertyUsageCollection:
    """Return what evidence.observed.collect_property_usage_evidence returns, from the rows.

    For each class C of *classes* and property p other than membership: the triples of members
    of C, their distinct subjects and objects, the node kinds of the objects, the datatypes and
    languages of the literals, and the subjects by number of distinct values (buckets of
    observed._histogram_bucket; "0" is the class's members without the property).
    """
    import polars as pl

    from rdfsolve.evidence.observed import (
        ClassPopulationEvidence,
        MeasurementState,
        PropertyUsageCollection,
        PropertyUsageEvidence,
        _histogram_bucket,
    )

    # collect_property_usage_evidence reads the RDF merge of the graphs (FROM each), types
    # included, without type context graphs.
    store = store.base.view(graph_uris) if graph_uris else store.base
    scope = list(graph_uris or [])
    semantics: Literal["endpoint_default_graph", "rdf_merge_selected_graphs"] = (
        "rdf_merge_selected_graphs" if scope else "endpoint_default_graph"
    )
    denominator = class_entity_counts or {}
    denominator_states = class_entity_count_states or {}
    populations = [
        ClassPopulationEvidence(
            class_iri=c,
            graph_scope=scope,
            scope_semantics=semantics,
            subject_count=denominator.get(c),
            count_status=denominator_states.get(c, "complete" if c in denominator else "not_run"),
        )
        for c in sorted(set(classes))
    ]
    shared = {k: v for k, v in (shared_extensions or {}).items() if v in classes}
    measured = [c for c in classes if c not in shared]
    types = _types(store)
    data = [p for p in store.predicates if p not in MEMBERSHIP.get()]
    records: list[PropertyUsageEvidence] = []
    states: list[MeasurementState] = []
    size = max(1, batch_size)
    # One pass over each predicate for all measured classes; the rows are then split into the
    # batches of classes whose states the miner records.
    members = types.filter(pl.col("c").is_in(measured))
    found: list[list[tuple[Any, ...]]] = [[], [], [], []]
    for predicate in data:
        edges = (
            store.rows(predicate)
            .join(members, on="s")
            .with_columns(p=pl.lit(predicate), lang=_language())
        )
        frames = pl.collect_all(
            [
                edges.group_by("c", "p").agg(
                    triples=pl.len(),
                    subjects=pl.col("s").n_unique(),
                    objects=pl.col("oid").n_unique(),
                ),
                edges.group_by("c", "p", "kind").agg(n=pl.len()),
                edges.filter(pl.col("kind") == "literal")
                .group_by("c", "p", "d", "lang")
                .agg(n=pl.len()),
                edges.group_by("c", "p", "s")
                .agg(v=pl.col("oid").n_unique())
                .group_by("c", "p", "v")
                .agg(n=pl.len()),
            ],
            engine="streaming",
        )
        for out, frame in zip(found, frames, strict=True):
            out.extend(frame.iter_rows())
    for offset in range(0, len(measured), size):
        batch = set(measured[offset : offset + size])
        summary, kinds, literals, histogram = ([r for r in rows if r[0] in batch] for rows in found)
        state = MeasurementState(status="complete", purpose="evidence/property-usage")
        states.append(state)
        batch_records: dict[tuple[str, str], PropertyUsageEvidence] = {}
        for cls, prop, triples, subjects, objects in summary:
            eligible = denominator.get(cls)
            batch_records[cls, prop] = PropertyUsageEvidence(
                subject_class=cls,
                property_uri=prop,
                graph_scope=scope,
                scope_semantics=semantics,
                eligible_subjects=eligible,
                denominator_state=denominator_states.get(
                    cls, "complete" if eligible is not None else "not_run"
                ),
                subjects_with_property=subjects,
                triple_count=triples,
                distinct_objects=objects,
                summary_state=state,
            )
        if collect_node_kinds and batch_records:
            detail = MeasurementState(status="complete", purpose="evidence/property-node-kind")
            states.append(detail)
            for record in batch_records.values():
                record.node_kind_state = detail.model_copy(deep=True)
            for cls, prop, kind, n in kinds:
                batch_records[cls, prop].node_kind_counts[_KIND[kind]] = n
        if collect_datatypes and batch_records:
            detail = MeasurementState(
                status="complete", purpose="evidence/property-literal-profile"
            )
            states.append(detail)
            for record in batch_records.values():
                record.datatype_state = detail.model_copy(deep=True)
            for cls, prop, datatype, language, n in literals:
                record = batch_records[cls, prop]
                key = datatype or "untyped-literal"
                record.datatype_counts[key] = record.datatype_counts.get(key, 0) + n
                if language:
                    record.language_counts[language] = record.language_counts.get(language, 0) + n
        if collect_histograms and batch_records:
            detail = MeasurementState(
                status="complete", purpose="evidence/property-value-count-histogram"
            )
            states.append(detail)
            exact: dict[tuple[str, str], dict[str, int]] = defaultdict(dict)
            nonzero: dict[tuple[str, str], int] = defaultdict(int)
            for cls, prop, values, n in histogram:
                bucket = _histogram_bucket(values)
                exact[cls, prop][bucket] = exact[cls, prop].get(bucket, 0) + n
                nonzero[cls, prop] += n
            for key, record in batch_records.items():
                # A subject with one triple for a property has exactly one value (observed.py).
                if record.triple_count == record.subjects_with_property:
                    exact[key] = {"1": record.subjects_with_property}  # type: ignore[dict-item]
                    nonzero[key] = record.subjects_with_property  # type: ignore[assignment]
                record.histogram_state = detail.model_copy(deep=True)
                values = dict(exact.get(key, {}))
                if record.eligible_subjects is not None:
                    zero = record.eligible_subjects - nonzero.get(key, 0)
                    if zero >= 0:
                        values["0"] = zero
                    else:
                        record.histogram_state.status = "partial"
                        record.histogram_state.failures.append(
                            "invalid_response: histogram subjects exceed class denominator"
                        )
                record.value_count_histogram = values or None
        records.extend(batch_records.values())
    for copy, source in shared.items():
        records.extend(
            r.model_copy(update={"subject_class": copy})
            for r in records
            if r.subject_class == source
        )
    records.sort(key=lambda row: (row.subject_class, row.property_uri))
    return PropertyUsageCollection(
        dataset_id=dataset_id,
        graph_scope=scope,
        scope_semantics=semantics,
        class_populations=populations,
        records=records,
        batch_states=states,
    )


# IRI findings


def iri_findings(schema: MinedSchema, statistics: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the report's iri_findings: schema_models.iri_quality.findings on the schema terms
    (the scan changes nothing in it; it is here so that a scan run records it the same way).
    """
    from rdfsolve.schema_models.iri_quality import findings

    return findings(schema, statistics)


# The test of iri_quality.QUERIES: RE2's \s (QLever) is ASCII whitespace. U+00A0, which
# Python's \s matches, is allowed in IRIs (RFC 3987 ucschar).
_QUERY_TEST = r"(?-u:[ \t\n\r\f\v])|[<>\"{}|^`\\]"


def data_iri_findings(store: RowStore) -> dict[str, dict[str, int]]:
    """Return the terms of the store that iri_quality.QUERIES list, by role: each IRI used as
    property, subject or object that has a character RDF IRIs exclude, with its triples.
    """
    import polars as pl

    found: dict[str, dict[str, int]] = {"property": {}, "subject": {}, "object": {}}
    for predicate in store.predicates:
        rows = store.rows(predicate)
        if re.search(r"[ \t\n\r\f\v<>\"{}|^`\\]", predicate):
            found["property"][predicate] = int(rows.select(pl.len()).collect().item())
        for role, column in (("subject", "s"), ("object", "o")):
            frame = (
                rows.filter(pl.col(column).str.starts_with("<"))
                .select(t=_bare(pl.col(column)))
                .filter(pl.col("t").str.contains(_QUERY_TEST))
                .group_by("t")
                .agg(n=pl.len())
                .collect()
            )
            for term, n in frame.iter_rows():
                found[role][term] = found[role].get(term, 0) + int(n)
    return {role: dict(sorted(terms.items())) for role, terms in found.items()}
