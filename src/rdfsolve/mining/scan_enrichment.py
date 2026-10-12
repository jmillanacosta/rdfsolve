"""Labels, enrichment, restrictions, lists, ontology axioms and metadata from a row store.

Each function returns the object that the SPARQL miner produces, computed by Polars from the
rows of a store (rdfsolve.mining.scan) with the definitions of the miner's queries:

- pattern_labels: the labels phase (pattern_enrichment.enrich_patterns_with_labels);
- enrichment: definitions, labels, examples and class examples (enrichment.query_enrichment);
- restriction_patterns: OWL restrictions grouped by namespace
  (restrictions.mine_restriction_patterns);
- collections: RDF lists (collections.discover_collections, which the miner runs only on an
  rdflib graph, so that a QLever run had none);
- ontology_structure: the axioms around the schema's terms (ontology_extraction.OntologyMiner);
- metadata_document: the descriptions of datasets and services (metadata.query_metadata_document).

Where the SPARQL miner takes whatever the endpoint returns first (examples: LIMIT without ORDER
BY; restriction examples: SAMPLE), these functions take the smallest term, so that the result is
the same on every run. Where the miner chooses by a rule (labels), the rule is the same.

Literal text: a store exported from QLever holds the text as QLever's TSV writes it, and the TSV
writes a tab inside a literal as a space. Newlines and
other characters are kept. ``enrichment`` takes an optional *select* (a SPARQL SELECT function,
such as SparqlHelper.select on the index's endpoint): the definitions and labels, the only
long texts, are then read by the miner's own definition_query, in batches of 50 terms, and the
rows decide nothing about their text. Without it the TSV text is used.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Literal

from rdfsolve.mining.scan import RDF_LANG, RDF_TYPE, XSD_STRING, RowStore

if TYPE_CHECKING:
    from pathlib import Path

    import polars as pl

    from rdfsolve.ontology.structure import OntologyStructure
    from rdfsolve.schema_models.collections import CollectionProfile
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.enrichment import RdfTerm, SchemaEnrichment
    from rdfsolve.schema_models.metadata import MetadataDocument
    from rdfsolve.schema_models.pattern import SchemaPattern
    from rdfsolve.schema_models.restrictions import RestrictionPatterns

__all__ = [
    "collections",
    "enrichment",
    "label_map",
    "metadata_document",
    "ontology_structure",
    "pattern_labels",
    "restriction_patterns",
    "term_patterns",
]

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
XSD_BOOLEAN = "http://www.w3.org/2001/XMLSchema#boolean"
_SENTINELS = ("Literal", "Resource", "BlankNode")
_ESCAPES = {'\\"': '"', "\\\\": "\\", "\\n": "\n", "\\t": "\t", "\\r": "\r"}


# Terms


def _lexical(term: str) -> tuple[str, str | None]:
    """Return the lexical form and language of a literal written as in TSV or N-Triples."""
    if not term.startswith('"'):
        return term, None  # QLever writes numbers, dates and booleans bare
    quote = '"""' if term.startswith('"""') and len(term) >= 6 else '"'
    end = term.rindex(quote)
    body = re.sub(r'\\["\\ntr]', lambda m: _ESCAPES[m.group(0)], term[len(quote) : end])
    suffix = term[end + len(quote) :]
    return body, suffix[1:] if suffix.startswith("@") else None


def _rdf_term(term: str, datatype: str | None, scope: str = "scan") -> RdfTerm:
    """Return a store term as the miner's RdfTerm (as _term makes it from SPARQL JSON).

    A blank node label is local to the store, as it is to one response: it is written
    ``example_<scope>_<label>``, as the miner writes ``example_<query>_<label>``.
    """
    from rdfsolve.schema_models.enrichment import RdfTerm

    if term.startswith("<"):
        return RdfTerm(kind="uri", value=term[1:-1])
    if term.startswith("_:"):
        return RdfTerm(kind="bnode", value=f"example_{scope}_{term[2:]}")
    value, language = _lexical(term)
    if language:
        return RdfTerm(kind="literal", value=value, language=language)
    return RdfTerm(
        kind="literal", value=value, datatype=None if datatype in (None, XSD_STRING) else datatype
    )


def _rows(store: RowStore, predicate: str) -> pl.LazyFrame | None:
    """Return the rows of *predicate* (s, o, d, oid, kind), or None when the store has none."""
    return store.rows(predicate) if predicate in store.predicates else None


def _literals(
    store: RowStore, predicate: str, subjects: Iterable[str]
) -> list[tuple[str, str, str | None, str | None]]:
    """Return (subject IRI, text, language, datatype) of the literal values of *predicate* on
    *subjects* (IRIs); datatype is None for xsd:string and language-tagged strings.
    """
    import polars as pl

    rows = _rows(store, predicate)
    terms = [f"<{s}>" for s in subjects]
    if rows is None or not terms:
        return []
    found = (
        rows.filter((pl.col("kind") == "literal") & pl.col("s").is_in(terms))
        .select("s", "o", "d")
        .collect()
    )
    out: list[tuple[str, str, str | None, str | None]] = []
    for s, o, d in found.iter_rows():
        text, language = _lexical(o)
        out.append((s[1:-1], text, language, None if language or d == XSD_STRING else d))
    return out


def _pairs(store: RowStore, predicate: str) -> list[tuple[str, str]]:
    """Return the distinct (subject, object) IRIs of *predicate*, without brackets."""
    import polars as pl

    rows = _rows(store, predicate)
    if rows is None:
        return []
    frame = (
        rows.filter(pl.col("s").str.starts_with("<") & (pl.col("kind") == "iri"))
        .select(s=pl.col("s").str.slice(1, pl.col("s").str.len_chars() - 2), o="o")
        .with_columns(o=pl.col("o").str.slice(1, pl.col("o").str.len_chars() - 2))
        .unique()
        .sort("s", "o")
        .collect()
    )
    return [tuple(r) for r in frame.iter_rows()]


def _type_rows(store: RowStore) -> pl.DataFrame:
    """Return the rdf:type rows (s, c) as terms: the miner's ``a`` (rdf:type only)."""
    import polars as pl

    rows = _rows(store, RDF_TYPE)
    if rows is None:
        return pl.DataFrame(schema={"s": pl.String, "c": pl.String})
    return rows.select("s", c="o").unique().collect()


# Labels phase


LABEL_COLUMNS = {
    "rdfsLabel": (RDFS + "label",),
    "dcTitle": ("http://purl.org/dc/elements/1.1/title", "http://purl.org/dc/terms/title"),
    "iaoLabel": ("http://purl.obolibrary.org/obo/IAO_0000118",),
    "skosPrefLabel": ("http://www.w3.org/2004/02/skos/core#prefLabel",),
    "skosAltLabel": ("http://www.w3.org/2004/02/skos/core#altLabel",),
}


def label_map(store: RowStore, uris: Iterable[str]) -> dict[str, str]:
    """Return the label of each IRI that has one, as the labels phase chooses it.

    The rule of pattern_enrichment._fetch_label_batch: for each IRI and label column of
    query_builders._build_label_query, the literal with the smallest (language other than none
    or "en", text); then pick_label chooses among the columns.
    """
    from rdfsolve._uri import pick_label

    uris = set(uris)
    best: dict[str, dict[str, tuple[bool, str]]] = defaultdict(dict)
    for column, predicates in LABEL_COLUMNS.items():
        for predicate in predicates:
            for s, text, language, _ in _literals(store, predicate, uris):
                rank = (language not in (None, "en"), text)
                if column not in best[s] or rank < best[s][column]:
                    best[s][column] = rank
    return {
        uri: pick_label(
            row.get("rdfsLabel", (False, None))[1],
            row.get("dcTitle", (False, None))[1],
            uri,
            iao_label=row.get("iaoLabel", (False, None))[1],
            skos_pref_label=row.get("skosPrefLabel", (False, None))[1],
            skos_alt_label=row.get("skosAltLabel", (False, None))[1],
        )
        for uri, row in best.items()
    }


def pattern_labels(store: RowStore, patterns: list[SchemaPattern]) -> list[SchemaPattern]:
    """Return *patterns* with subject, property and object labels (the miner's labels phase,
    pattern_enrichment.enrich_patterns_with_labels), the local name when there is no label.
    """
    from rdfsolve.mining.pattern_enrichment import _enrich_with_local

    uris: set[str] = set()
    for pattern in patterns:
        uris.update((pattern.subject_class, pattern.property_uri))
        if pattern.object_class not in _SENTINELS:
            uris.add(pattern.object_class)
    return _enrich_with_local(patterns, label_map(store, uris))


# Enrichment


def _annotations(
    store: RowStore, iris: list[str], select: Callable[..., dict[str, Any]] | None
) -> list[tuple[str, str, RdfTerm]]:
    """Return (term, predicate, text) of every definition or name literal of *iris*."""
    from rdfsolve.mining.enrichment import _term, definition_query
    from rdfsolve.schema_models.enrichment import (
        DEFINITION_PREDICATES,
        NAME_PREDICATES,
        RdfTerm,
    )

    found: list[tuple[str, str, RdfTerm]] = []
    if select is not None:
        for offset in range(0, len(iris), 50):
            answer = select(definition_query(iris[offset : offset + 50], None), "definitions")
            found.extend(
                (row["term"]["value"], row["predicate"]["value"], _term(row["text"], 0))
                for row in answer.get("results", {}).get("bindings", [])
            )
        return found
    for predicate in DEFINITION_PREDICATES + NAME_PREDICATES:
        for s, text, language, datatype in _literals(store, predicate, iris):
            term = RdfTerm(kind="literal", value=text, datatype=datatype, language=language)
            found.append((s, predicate, term))
    return found


# Classified rows (a row under each class of its subject and of its object) of a predicate read
# at a time for the examples: a larger predicate is read in parts by subject id, and the
# smallest examples of the parts are merged.
EXAMPLE_PART_ROWS = 10_000_000
# Rows of a predicate sampled to estimate its classified rows a row, and the fewest rows of a
# part however many classes its nodes have.
EXAMPLE_SAMPLE_ROWS = 1_000_000
EXAMPLE_MIN_PART_ROWS = 10_000
# Seconds between two progress lines of the examples.
EXAMPLE_PROGRESS_SECONDS = 60.0

# The kinds of subject and object an example row is classified under.
_TYPED, _UNTYPED, _TERM = "type", "untyped", "term"


def _smallest(frame: pl.DataFrame, keys: list[str], limit: int) -> pl.DataFrame:
    """Return the *limit* smallest distinct (s, o) of each group of *keys*, by s then o.

    The smallest pairs of a group have the group's *limit* smallest subjects, which a group-by
    finds without sorting the rows: only the rows of those subjects are sorted.
    """
    import polars as pl

    cut = frame.group_by(keys).agg(pl.col("s").unique().bottom_k(limit)).explode("s")
    kept = frame.join(cut, on=[*keys, "s"], how="semi", nulls_equal=True)
    return (
        kept.unique([*keys, "s", "o"], keep="first")
        .sort([*keys, "s", "o"])
        .group_by(keys, maintain_order=True)
        .head(limit)
    )


def _subject_key(pattern: SchemaPattern) -> tuple[str, str]:
    """Return how *pattern* selects its subjects: (kind, class or term IRI)."""
    if pattern.subject_binding == "term":
        return _TERM, pattern.subject_class
    if pattern.untyped_subject:
        return _UNTYPED, ""
    return _TYPED, pattern.subject_class


def _object_key(pattern: SchemaPattern) -> tuple[str, str]:
    """Return how *pattern* selects its values: (kind, class, sentinel or term IRI)."""
    if pattern.object_binding == "term":
        return _TERM, pattern.object_class
    if pattern.object_class in _SENTINELS:
        return pattern.object_class, ""
    return _TYPED, pattern.object_class


class _Progress:
    """Log the progress of the examples at most once every EXAMPLE_PROGRESS_SECONDS."""

    def __init__(self, properties: int, patterns: int) -> None:
        """Keep the totals and the time of the last line."""
        import time

        self.properties, self.patterns = properties, patterns
        self.last = time.monotonic()

    def step(self, properties: int, patterns: int) -> None:
        """Log the properties and patterns done, when the last line is old enough."""
        import logging
        import time

        now = time.monotonic()
        if now - self.last >= EXAMPLE_PROGRESS_SECONDS:
            self.last = now
            logging.getLogger(__name__).info(
                "Examples: %d of %d properties, %d of %d patterns",
                properties,
                self.properties,
                patterns,
                self.patterns,
            )


def _subject_rows(
    rows: pl.DataFrame,
    patterns: list[SchemaPattern],
    types: pl.DataFrame,
    typed: pl.DataFrame,
) -> pl.DataFrame:
    """Classify rows under every subject a pattern asks for: (sk, sc) is a class of the subject
    (_TYPED), no type of an IRI subject (_UNTYPED) or the subject term itself (_TERM). *types*
    and *typed* hold the types of these rows' subjects (sid, c: the classes the patterns name)
    and the typed ids (sid).
    """
    import polars as pl

    classes = sorted({f"<{p.subject_class}>" for p in patterns if _subject_key(p)[0] == _TYPED})
    terms = sorted({f"<{p.subject_class}>" for p in patterns if _subject_key(p)[0] == _TERM})
    frames = []
    if classes:
        frames.append(
            rows.join(
                types.rename({"c": "sc"}),
                on="sid",
            ).with_columns(sk=pl.lit(_TYPED))
        )
    if any(_subject_key(p)[0] == _UNTYPED for p in patterns):
        frames.append(
            rows.filter(pl.col("s").str.starts_with("<"))
            .join(typed, on="sid", how="anti")
            .with_columns(sk=pl.lit(_UNTYPED), sc=pl.lit(""))
        )
    if terms:
        frames.append(
            rows.filter(pl.col("s").is_in(terms)).with_columns(sk=pl.lit(_TERM), sc=pl.col("s"))
        )
    columns = ["s", "o", "d", "oid", "kind", "sk", "sc"]
    if not frames:
        return rows.select(columns[:5]).with_columns(sk=pl.lit(""), sc=pl.lit("")).head(0)
    return pl.concat([f.select(*columns[:-1], pl.col("sc").cast(pl.String)) for f in frames])


def _value_rows(
    rows: pl.DataFrame,
    patterns: list[SchemaPattern],
    types: pl.DataFrame,
    typed: pl.DataFrame,
) -> pl.DataFrame:
    """Classify subject-classified rows under every value a pattern asks for: (ok, oc, dt) is a
    class of the object (_TYPED), Literal with its datatype, Resource (an IRI without a type),
    BlankNode, or the object term itself (_TERM). *types* and *typed* hold the types of these
    rows' objects (sid, c: the classes the patterns name) and the typed ids (sid).
    """
    import polars as pl

    objects = [_object_key(p) for p in patterns]
    classes = sorted({f"<{c}>" for kind, c in objects if kind == _TYPED})
    terms = sorted({f"<{c}>" for kind, c in objects if kind == _TERM})
    sentinels = {kind for kind, _ in objects if kind in _SENTINELS}
    literal = pl.col("kind") == "literal"
    none = pl.lit(None, pl.String)
    frames = []
    if "Literal" in sentinels:
        frames.append(
            rows.filter(literal).with_columns(ok=pl.lit("Literal"), oc=pl.lit(""), dt=pl.col("d"))
        )
    if "BlankNode" in sentinels:
        frames.append(
            rows.filter(pl.col("kind") == "bnode").with_columns(
                ok=pl.lit("BlankNode"), oc=pl.lit(""), dt=none
            )
        )
    if "Resource" in sentinels:
        frames.append(
            rows.filter(pl.col("kind") == "iri")
            .join(typed.rename({"sid": "oid"}), on="oid", how="anti")
            .with_columns(ok=pl.lit("Resource"), oc=pl.lit(""), dt=none)
        )
    if classes:
        frames.append(
            rows.filter(~literal)
            .join(
                types.rename({"sid": "oid", "c": "oc"}),
                on="oid",
            )
            .with_columns(ok=pl.lit(_TYPED), dt=none)
        )
    if terms:
        frames.append(
            rows.filter(pl.col("o").is_in(terms)).with_columns(
                ok=pl.lit(_TERM), oc=pl.col("o"), dt=none
            )
        )
    keys = ["sk", "sc", "ok", "oc", "dt"]
    if not frames:
        return pl.DataFrame(
            schema={
                **dict.fromkeys(keys, pl.String),
                "s": pl.String,
                "o": pl.String,
                "d": pl.String,
            }
        )
    return pl.concat(
        [
            f.select(
                *(pl.col(k).cast(pl.String) for k in keys),
                pl.col("s").cast(pl.String),
                pl.col("o").cast(pl.String),
                pl.col("d").cast(pl.String),
            )
            for f in frames
        ]
    )


def _fanout(sample: pl.DataFrame, types: pl.DataFrame, column: str) -> float:
    """Return the classes of a row's *column* node on average (at least 1), from a sample."""
    if not sample.height:
        return 1.0
    joined = sample.join(types, left_on=column, right_on="sid").height
    return max(1.0, joined / sample.height)


def _predicate_examples(
    store: RowStore,
    predicate: str,
    patterns: list[SchemaPattern],
    limit: int,
    types: pl.DataFrame,
    typed: pl.DataFrame,
) -> dict[tuple[str, str, str, str, str | None], pl.DataFrame]:
    """Return the smallest examples of the patterns of one predicate, by pattern key.

    Each row of the predicate is classified under every subject and every value that a pattern
    of the predicate asks for, with the conditions of enrichment.example_query; the classes are
    joined by id with *types* (sid, c: the rdf:type rows of the classes the patterns name) and
    *typed* (sid: every node with an rdf:type row). A row whose nodes have k and m classes
    becomes k * m classified rows, so the rows are read in parts by subject id sized by those
    classes (EXAMPLE_PART_ROWS classified rows a part, estimated on a sample). Each part keeps
    the *limit* smallest (s, o) of each group, and the parts are merged the same way, which
    gives the smallest of the whole predicate.
    """
    import polars as pl

    rows = store.rows(predicate).select("s", "o", "d", "sid", "oid", "kind")
    size = int(rows.select(pl.len()).collect(engine="streaming").item())
    if not size:
        return {}
    subject_classes = [f"<{p.subject_class}>" for p in patterns if _subject_key(p)[0] == _TYPED]
    object_classes = [f"<{c}>" for kind, c in map(_object_key, patterns) if kind == _TYPED]
    subject_types = types.filter(pl.col("c").cast(pl.String).is_in(subject_classes))
    object_types = types.filter(pl.col("c").cast(pl.String).is_in(object_classes))
    step = max(1, size // EXAMPLE_SAMPLE_ROWS)
    sample = rows.select("sid", "oid").gather_every(step).collect(engine="streaming")
    expansion = _fanout(sample, subject_types, "sid") * _fanout(sample, object_types, "oid")
    part_rows = max(EXAMPLE_MIN_PART_ROWS, int(EXAMPLE_PART_ROWS / expansion))
    parts = max(1, -(-size // part_rows))
    keys = ["sk", "sc", "ok", "oc", "dt"]
    found: list[pl.DataFrame] = []
    for part in range(parts):
        selected = rows.filter(pl.col("sid") % parts == part) if parts > 1 else rows
        subjects = _subject_rows(
            selected.collect(engine="streaming"), patterns, subject_types, typed
        )
        values = _value_rows(subjects, patterns, object_types, typed)
        if values.height:
            # A copy: a slice of the part's strings would keep the whole part's text alive.
            small = _smallest(values, keys, limit)
            found.append(pl.DataFrame(small.to_dict(as_series=False), schema=small.schema))
    if not found:
        return {}
    merged = _smallest(pl.concat(found), keys, limit)
    out: dict[tuple[str, str, str, str, str | None], pl.DataFrame] = {}
    for key, group in merged.group_by(keys, maintain_order=True):
        out[tuple(key)] = group.select("s", "o", "d")
    return out


def _pattern_examples(
    store: RowStore, patterns: list[SchemaPattern], limit: int, progress: _Progress
) -> list[list[tuple[str, str, str | None]]]:
    """Return the first *limit* (subject, value) pairs of each pattern, ordered by subject then
    value, with the conditions of enrichment.example_query.

    The rdf:type rows (the miner's ``a``) of the classes the patterns name are read once, by id
    with the classes as categories, and the typed nodes by id; each predicate is then read once
    for all of its patterns (_predicate_examples).
    """
    import polars as pl

    by_predicate: dict[str, list[int]] = defaultdict(list)
    for number, pattern in enumerate(patterns):
        if pattern.property_uri in store.predicates:
            by_predicate[pattern.property_uri].append(number)
    out: list[list[tuple[str, str, str | None]]] = [[] for _ in patterns]
    if not by_predicate:
        return out
    type_rows = _rows(store, RDF_TYPE)
    classes = sorted(
        {f"<{p.subject_class}>" for p in patterns if _subject_key(p)[0] == _TYPED}
        | {f"<{c}>" for kind, c in map(_object_key, patterns) if kind == _TYPED}
    )
    if type_rows is None:
        types = pl.DataFrame(schema={"sid": pl.UInt64, "c": pl.Categorical})
        typed = pl.DataFrame(schema={"sid": pl.UInt64})
    else:
        types = (
            type_rows.filter(pl.col("o").is_in(classes))
            .select("sid", c=pl.col("o").cast(pl.Categorical))
            .unique()
            .collect(engine="streaming")
        )
        typed = type_rows.select("sid").unique().collect(engine="streaming")
    done = 0
    for count, (predicate, numbers) in enumerate(sorted(by_predicate.items()), start=1):
        own = [patterns[n] for n in numbers]
        found = _predicate_examples(store, predicate, own, limit, types, typed)
        for number, pattern in zip(numbers, own, strict=True):
            out[number] = _examples_of(found, pattern, limit)
        done += len(numbers)
        progress.step(count, done)
    return out


def _examples_of(
    found: dict[tuple[str, str, str, str, str | None], pl.DataFrame],
    pattern: SchemaPattern,
    limit: int,
) -> list[tuple[str, str, str | None]]:
    """Return the examples of one pattern from the smallest rows of its predicate's groups."""
    import polars as pl

    sk, sc = _subject_key(pattern)
    ok, oc = _object_key(pattern)
    subject = "" if sk == _UNTYPED else f"<{sc}>"
    value = "" if ok in _SENTINELS else f"<{oc}>"
    if ok == "Literal" and not pattern.datatype:
        groups = [frame for key, frame in found.items() if key[:4] == (sk, subject, ok, value)]
    else:
        dt = pattern.datatype if ok == "Literal" else None
        groups = [found[key] for key in [(sk, subject, ok, value, dt)] if key in found]
    if not groups:
        return []
    frame = _smallest(pl.concat(groups).with_columns(k=pl.lit(0)), ["k"], limit)
    return [(s, o, d) for s, o, d in frame.select("s", "o", "d").iter_rows()]


def _class_examples(store: RowStore, classes: list[str], limit: int) -> dict[str, list[str]]:
    """Return the *limit* smallest members (rdf:type subjects) of each class.

    One streaming pass a member: the smallest subject of each class not chosen yet, so that
    no pass holds more than the members already chosen.
    """
    import polars as pl

    type_rows = _rows(store, RDF_TYPE)
    wanted = [f"<{c}>" for c in classes]
    members: dict[str, list[str]] = {c: [] for c in classes}
    if type_rows is None or not wanted:
        return members
    rows = type_rows.filter(pl.col("o").is_in(wanted)).select("s", c="o")
    chosen = pl.DataFrame(schema={"s": pl.String, "c": pl.String})
    for _ in range(limit):
        found = (
            rows.join(chosen.lazy(), on=["s", "c"], how="anti")
            .group_by("c")
            .agg(pl.col("s").min())
            .collect(engine="streaming")
        )
        if not found.height:
            break
        chosen = pl.concat([chosen, found.select("s", "c")])
    for s, c in chosen.sort("c", "s").iter_rows():
        members[c[1:-1]].append(s)
    return members


def enrichment(
    store: RowStore,
    schema: MinedSchema,
    *,
    examples_per_pattern: int = 2,
    annotation_iris: Sequence[str] | None = None,
    select: Callable[..., dict[str, Any]] | None = None,
) -> SchemaEnrichment:
    """Return the definitions, labels, examples and class examples of *schema*.

    Replaces enrichment.query_enrichment on the same schema. The terms are the classes and
    properties of the schema and *annotation_iris*; every literal of a definition or name
    predicate on them is kept, as definition_query returns them. The examples follow the
    conditions of example_query and class_example_query, but are the smallest subjects, then
    the smallest values (the order of terms as written in the store), not the endpoint's first
    rows: the same schema gives the same examples on every run. *select* reads the texts of
    the definitions and labels from an endpoint (see the module docstring).
    """
    from rdfsolve.schema_models.enrichment import (
        NAME_PREDICATES,
        PatternExample,
        SchemaEnrichment,
        TermAnnotation,
    )

    if not 0 <= examples_per_pattern <= 20:
        raise ValueError("examples_per_pattern must be between 0 and 20")
    result = SchemaEnrichment(
        examples_per_pattern=examples_per_pattern, endpoint=store.manifest.get("endpoint")
    )
    classes = sorted(schema.get_classes())
    iris = sorted(set(classes) | set(schema.get_properties()) | set(annotation_iris or []))
    for term, predicate, text in sorted(
        _annotations(store, iris, select), key=lambda a: (a[0], a[1], a[2].model_dump_json())
    ):
        annotation = TermAnnotation(term_iri=term, predicate=predicate, text=text)
        destination = result.labels if predicate in NAME_PREDICATES else result.definitions
        if annotation not in destination:
            destination.append(annotation)
    if examples_per_pattern:
        members = _class_examples(store, classes, examples_per_pattern)
        result.class_examples = {iri: [_rdf_term(s, None) for s in members[iri]] for iri in classes}
        unique = {}
        for pattern in schema.patterns:
            key = (pattern.subject_class, pattern.property_uri)
            unique[(*key, pattern.subject_binding, pattern.object_class, pattern.datatype)] = (
                pattern
            )
        chosen = list(unique.values())
        progress = _Progress(len({p.property_uri for p in chosen}), len(chosen))
        rows_of = _pattern_examples(store, chosen, examples_per_pattern, progress)
        for pattern, rows in zip(chosen, rows_of, strict=True):
            for s, o, d in rows:
                result.examples.append(
                    PatternExample(
                        subject_class=pattern.subject_class,
                        subject_binding="untyped" if pattern.untyped_subject else "type",
                        property_uri=pattern.property_uri,
                        subject=_rdf_term(s, None),
                        value=_rdf_term(o, d),
                    )
                )
    result.state = "complete"
    return result


# Restriction patterns


def _rest_closure(store: RowStore) -> pl.DataFrame:
    """Return (l, m) for l rdf:rest* m: zero or more steps, distinct pairs (SPARQL paths)."""
    import polars as pl

    rest_rows = _rows(store, RDF + "rest")
    first = _rows(store, RDF + "first")
    if rest_rows is None or first is None:
        return pl.DataFrame(schema={"l": pl.String, "m": pl.String})
    rest = rest_rows.select(a="s", b="o").unique().collect()
    nodes = pl.concat([rest["a"], rest["b"], first.select("s").collect()["s"]]).unique()
    pairs = pl.DataFrame({"l": nodes, "m": nodes})
    frontier = pairs
    while frontier.height:
        step = frontier.join(rest, left_on="m", right_on="a").select("l", m="b")
        frontier = step.join(pairs, on=["l", "m"], how="anti").unique()
        pairs = pl.concat([pairs, frontier])
    return pairs


def restriction_patterns(store: RowStore) -> RestrictionPatterns:
    """Return the restriction patterns of the store (restrictions.mine_restriction_patterns).

    Each axiom and form joins the rows as the miner's grouped query does: a class that is a
    subclass of a restriction (rdfs:subClassOf), or equivalent to an intersection with one
    (owl:equivalentClass/owl:intersectionOf/rdf:rest*/rdf:first), the restriction's
    owl:onProperty and its filler, grouped by property and the namespaces of class and filler.
    The examples are the smallest class and filler of each group (the miner: SAMPLE).
    """
    import polars as pl

    from rdfsolve.mining.restrictions import FORMS, manchester
    from rdfsolve.ontology.terms import namespace
    from rdfsolve.schema_models.restrictions import (
        CLASS_EXPRESSION,
        RestrictionPattern,
        RestrictionPatterns,
    )

    result = RestrictionPatterns(query_count=0)
    on_property_rows = _rows(store, OWL + "onProperty")
    if on_property_rows is None:
        return result
    on_property = on_property_rows.select(r="s", p="o").unique().collect()
    axioms: dict[str, pl.DataFrame] = {}
    subclass = _rows(store, RDFS + "subClassOf")
    if subclass is not None:
        axioms["SubClassOf"] = subclass.select(c="s", r="o").unique().collect()
    equivalent_rows = _rows(store, OWL + "equivalentClass")
    intersection_rows = _rows(store, OWL + "intersectionOf")
    first = _rows(store, RDF + "first")
    if equivalent_rows is not None and intersection_rows is not None and first is not None:
        equivalent = equivalent_rows.select("s", "o").unique().collect()
        intersection = intersection_rows.select("s", "o").unique().collect()
        axioms["EquivalentTo"] = (
            equivalent.rename({"s": "c", "o": "x"})
            .join(intersection.rename({"s": "x", "o": "l"}), on="x")
            .join(_rest_closure(store), on="l")
            .join(first.select(m="s", r="o").unique().collect(), on="m")
            .select("c", "r")
        )
    rows: list[dict[str, Any]] = []
    for axiom, links in axioms.items():
        for form, predicate in FORMS.items():
            fillers = _rows(store, predicate)
            if fillers is None:
                continue
            joined = (
                links.join(on_property, on="r")
                .join(fillers.select(r="s", v="o").unique().collect(), on="r")
                .filter(pl.col("c").str.starts_with("<") & pl.col("p").str.starts_with("<"))
            )
            groups: dict[tuple[str, str, str], list[tuple[str, str]]] = defaultdict(list)
            for c, p, v in joined.select("c", "p", "v").iter_rows():
                filler = namespace(v[1:-1]) if v.startswith("<") else CLASS_EXPRESSION
                groups[(p[1:-1], namespace(c[1:-1]), filler)].append((c, v))
            for (p, sns, fns), members in groups.items():
                example_filler = min(v for _, v in members)
                rows.append(
                    {
                        "subject_namespace": sns,
                        "axiom": axiom,
                        "property_uri": p,
                        "form": form,
                        "filler_namespace": fns,
                        "count": len(members),
                        "classes": len({c for c, _ in members}),
                        "example_subject": min(c for c, _ in members)[1:-1],
                        "example_filler": example_filler[1:-1]
                        if example_filler.startswith("<")
                        else None,
                    }
                )
    labels: dict[str, str] = {}
    for s, text, language, _ in _literals(store, RDFS + "label", {r["property_uri"] for r in rows}):
        english = language is None or language.lower() == "en" or language.lower().startswith("en-")
        if english and (s not in labels or text < labels[s]):
            labels[s] = text
    result.patterns = sorted(
        (
            RestrictionPattern(**row, label=manchester(row, labels.get(row["property_uri"])))
            for row in rows
        ),
        key=lambda p: (-p.count, p.label),
    )
    return result


# Collections


def collections(store: RowStore) -> list[CollectionProfile]:
    """Return the RDF lists of the store by owner class and property, as
    collections.discover_collections does on an rdflib graph of the default graph.

    The miner runs discover_collections only when it reads an rdflib graph; with a store the
    lists of a QLever index are described too.
    """
    import polars as pl

    from rdfsolve.schema_models.collections import CollectionProfile

    nil = f"<{RDF}nil>"
    firsts: dict[str, list[tuple[str, str | None]]] = defaultdict(list)
    rests: dict[str, list[str]] = defaultdict(list)
    first_rows = _rows(store, RDF + "first")
    if first_rows is not None:
        for s, o, d in first_rows.select("s", "o", "d").unique().collect().iter_rows():
            firsts[s].append((o, d))
    rest_rows = _rows(store, RDF + "rest")
    if rest_rows is not None:
        for s, o, _ in rest_rows.select("s", "o", "d").unique().collect().iter_rows():
            rests[s].append(o)
    heads = set(firsts) | set(rests) | {nil}
    head_frame = pl.LazyFrame({"o": sorted(heads)}, schema={"o": pl.String})
    owners: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for predicate in store.predicates:
        if predicate in (RDF + "first", RDF + "rest", RDF_TYPE):
            continue
        found = store.rows(predicate).select("s", "o").join(head_frame, on="o", how="semi")
        for s, o in found.collect(engine="streaming").iter_rows():
            owners[o].append((s, predicate))
    # The types of the owners and of the list members only: the type rows are read for these
    # nodes, never collected whole (a store can have lists on a handful of nodes and types on
    # all of them).
    nodes = {owner for found in owners.values() for owner, _ in found}
    nodes |= {term for cells in firsts.values() for term, _ in cells}
    types: dict[str, set[str]] = defaultdict(set)
    type_rows = _rows(store, RDF_TYPE)
    if type_rows is not None and nodes:
        wanted = pl.LazyFrame({"s": sorted(nodes)}, schema={"s": pl.String})
        found_types = (
            type_rows.select("s", c="o")
            .join(wanted, on="s", how="semi")
            .unique()
            .collect(engine="streaming")
        )
        for s, c in found_types.iter_rows():
            if c.startswith("<"):
                types[s].add(c[1:-1])

    def members(head: str) -> list[tuple[str, str | None]] | None:
        """Return the (term, datatype) members of the list at *head*; None for a malformed one."""
        out: list[tuple[str, str | None]] = []
        seen: set[str] = set()
        cell = head
        while cell != nil:
            if cell in seen or not cell.startswith(("<", "_:")):
                return None
            seen.add(cell)
            first, rest = firsts.get(cell, []), rests.get(cell, [])
            if len(first) != 1 or len(rest) != 1:
                return None
            out.append(first[0])
            cell = rest[0]
        return out

    profiles: dict[tuple[str, str], CollectionProfile] = {}
    for head in sorted(heads):
        if head not in owners:
            continue
        listed = members(head)
        for owner, predicate in owners[head]:
            for cls in types.get(owner, ()):
                profile = profiles.setdefault(
                    (cls, predicate), CollectionProfile(subject_class=cls, property_uri=predicate)
                )
                if listed is None:
                    profile.invalid_count += 1
                    continue
                profile.list_count += 1
                n = len(listed)
                profile.min_length = n if profile.min_length is None else min(profile.min_length, n)
                profile.max_length = n if profile.max_length is None else max(profile.max_length, n)
                for term, datatype in listed:
                    kind: Literal["IRI", "BlankNode", "Literal"]
                    if term.startswith(("<", "_:")):
                        kind = "IRI" if term.startswith("<") else "BlankNode"
                        profile.member_types = sorted(
                            set(profile.member_types) | types.get(term, set())
                        )
                    else:
                        kind = "Literal"
                        _, language = _lexical(term)
                        datatype = RDF_LANG if language else datatype or XSD_STRING
                        profile.member_datatypes = sorted({*profile.member_datatypes, datatype})
                        if language:
                            profile.member_languages = sorted({*profile.member_languages, language})
                    profile.member_kinds = sorted({*profile.member_kinds, kind})
    return [profiles[key] for key in sorted(profiles)]


# Ontology axioms


def ontology_structure(
    store: RowStore, class_iris: list[str] | None, property_iris: list[str] | None
) -> OntologyStructure:
    """Return the axioms around the schema's terms (ontology_extraction.OntologyMiner.mine).

    None selects all terms (the "full" scope). The named classes, domains, ranges, ancestors
    (followed upwards), superproperties, deprecations, inverses and characteristics are read as
    the miner's queries define them. One difference, on purpose: the miner's queries for
    owl:equivalentClass, owl:disjointWith, owl:equivalentProperty and owl:inverseOf bind the
    selected term inside a UNION branch, where SPARQL evaluates it before the outer VALUES, so
    they return every such pair of the dataset, once per batch. Here these pairs are
    those that touch a selected term, as the queries intend.
    """
    from rdfsolve.ontology.structure import (
        DisjointClassRelation,
        DomainAssertion,
        EquivalentClassRelation,
        EquivalentPropertyRelation,
        InverseRelation,
        OntologyStructure,
        PropertyCharacteristic,
        RangeAssertion,
        SubClassRelation,
        SubPropertyRelation,
    )

    selected_classes = None if class_iris is None else set(class_iris)
    selected_properties = None if property_iris is None else set(property_iris)

    def touching(pairs: list[tuple[str, str]], selected: set[str] | None) -> list[tuple[str, str]]:
        """Keep the pairs with a *selected* term on either side (all without a selection)."""
        return [p for p in pairs if selected is None or p[0] in selected or p[1] in selected]

    def scoped(pairs: list[tuple[str, str]], selected: set[str] | None) -> list[tuple[str, str]]:
        """Keep the pairs whose subject is *selected* (all without a selection)."""
        return [p for p in pairs if selected is None or p[0] in selected]

    subclass_pairs = _pairs(store, RDFS + "subClassOf")
    domain = scoped(_pairs(store, RDFS + "domain"), selected_properties)
    ranges = scoped(_pairs(store, RDFS + "range"), selected_properties)
    types = _type_rows(store)
    declared = {
        s[1:-1]
        for s, c in types.iter_rows()
        if s.startswith("<") and c in (f"<{OWL}Class>", f"<{RDFS}Class>")
    }
    if selected_classes is None:
        classes = set(declared)
    else:
        related = {c for pair in subclass_pairs for c in pair}
        classes = {c for c in selected_classes if c in declared or c in related}
    equivalent = touching(_pairs(store, OWL + "equivalentClass"), selected_classes)
    disjoint = touching(_pairs(store, OWL + "disjointWith"), selected_classes)
    parents: dict[str, set[str]] = defaultdict(set)
    for child, parent in subclass_pairs:
        parents[child].add(parent)
    if selected_classes is None:
        subclass = subclass_pairs
    else:
        pending = selected_classes | {d for _, d in domain} | {r for _, r in ranges}
        for a, b in equivalent:
            if a in pending or b in pending:
                pending |= {a, b}
        reached: set[tuple[str, str]] = set()
        visited: set[str] = set()
        while pending - visited:
            current = pending - visited
            visited |= current
            for child in current:
                for parent in parents.get(child, ()):
                    reached.add((child, parent))
                    pending.add(parent)
        subclass = sorted(reached)
    for pair in [*subclass, *equivalent]:
        classes.update(pair)
    for a, b in disjoint:
        if a in classes or b in classes:
            classes |= {a, b}
    classes |= {d for _, d in domain} | {r for _, r in ranges}
    equivalent_p = touching(_pairs(store, OWL + "equivalentProperty"), selected_properties)
    superproperties: dict[str, set[str]] = defaultdict(set)
    subproperty_pairs = _pairs(store, RDFS + "subPropertyOf")
    for child, parent in subproperty_pairs:
        superproperties[child].add(parent)
    if selected_properties is None:
        subproperty = subproperty_pairs
    else:
        pending = set(selected_properties)
        for a, b in equivalent_p:
            if a in pending or b in pending:
                pending |= {a, b}
        found, visited = set(), set()
        while pending - visited:
            current = pending - visited
            visited |= current
            for child in current:
                for parent in superproperties.get(child, ()):
                    found.add((child, parent))
                    pending.add(parent)
        subproperty = sorted(found)
    retained = classes | (selected_properties or set()) | {t for pair in subproperty for t in pair}
    deprecated = set()
    rows = _rows(store, OWL + "deprecated")
    if rows is not None:
        for s, o, d in rows.select("s", "o", "d").collect().iter_rows():
            if s.startswith("<") and d == XSD_BOOLEAN and _lexical(o)[0] == "true":
                deprecated.add(s[1:-1])
    deprecated &= retained  # the miner asks for the retained terms only
    kinds = {
        f"<{OWL}{k}>"
        for k in (
            "FunctionalProperty",
            "InverseFunctionalProperty",
            "TransitiveProperty",
            "SymmetricProperty",
            "AsymmetricProperty",
            "ReflexiveProperty",
            "IrreflexiveProperty",
        )
    }
    characteristics = sorted(
        {
            (s[1:-1], c[1:-1])
            for s, c in types.iter_rows()
            if c in kinds
            and s.startswith("<")
            and (selected_properties is None or s[1:-1] in selected_properties)
        }
    )
    return OntologyStructure(
        classes=sorted(classes),
        subclass_relations=[SubClassRelation(child=a, parent=b) for a, b in subclass],
        subproperty_relations=[SubPropertyRelation(child=a, parent=b) for a, b in subproperty],
        equivalent_classes=[EquivalentClassRelation(class1=a, class2=b) for a, b in equivalent],
        equivalent_properties=[
            EquivalentPropertyRelation(property1=a, property2=b) for a, b in equivalent_p
        ],
        disjoint_classes=[DisjointClassRelation(class1=a, class2=b) for a, b in disjoint],
        deprecated_terms=sorted(deprecated),
        domain_assertions=[DomainAssertion(property_uri=a, domain=b) for a, b in domain],
        range_assertions=[RangeAssertion(property_uri=a, range=b) for a, b in ranges],
        inverse_properties=[
            InverseRelation(property1=a, property2=b)
            for a, b in touching(_pairs(store, OWL + "inverseOf"), selected_properties)
        ],
        property_characteristics=[
            PropertyCharacteristic(property_uri=a, characteristic=b) for a, b in characteristics
        ],
    )


# Metadata


METADATA_TYPES = (
    "http://rdfs.org/ns/void#Dataset",
    "http://www.w3.org/ns/dcat#Dataset",
    "http://www.w3.org/ns/sparql-service-description#Service",
)
PARTITIONS = (
    "http://rdfs.org/ns/void#classPartition",
    "http://rdfs.org/ns/void#propertyPartition",
    "http://ldf.fi/void-ext#datatypePartition",
)


def metadata_document(
    store: RowStore, census: dict[str, dict[str, int]] | None = None
) -> MetadataDocument:
    """Return the descriptions of datasets and services (metadata.query_metadata_document).

    The roots are the subjects typed void:Dataset, dcat:Dataset or sd:Service; their triples
    are kept with two levels of blank-node values (not through VoID partitions), as the
    miner's CONSTRUCT does. QLever reports every integer as xsd:int and a decimal as xsd:double;
    with the *census* of the index's source files (qlever.datatypes.read_census) a property
    with one source datatype of that group gets it back. (The miner's CONSTRUCT returns Turtle
    with bare numbers, which rdflib reads as xsd:integer or xsd:decimal whatever the source.)
    """
    import polars as pl
    from rdflib import BNode, Dataset, Graph, Literal, URIRef

    from rdfsolve.qlever.datatypes import REPORTED
    from rdfsolve.schema_models.metadata import MetadataDocument

    types = _type_rows(store)
    roots = set(types.filter(pl.col("c").is_in([f"<{t}>" for t in METADATA_TYPES]))["s"])

    def outgoing(subjects: set[str]) -> list[tuple[str, str, str, str | None]]:
        """Return the (subject, predicate, object, datatype) rows of *subjects*."""
        if not subjects:
            return []
        found: list[tuple[str, str, str, str | None]] = []
        for predicate in store.predicates:
            rows = store.rows(predicate).filter(pl.col("s").is_in(list(subjects)))
            found.extend(
                (s, predicate, o, d) for s, o, d in rows.select("s", "o", "d").collect().iter_rows()
            )
        return found

    def node(term: str, predicate: str, datatype: str | None) -> Any:
        """Return the rdflib node of *term*, the object of *predicate*."""
        if term.startswith("<"):
            return URIRef(term[1:-1])
        if term.startswith("_:"):
            return BNode(term[2:])
        text, language = _lexical(term)
        if language:
            return Literal(text, lang=language)
        group = REPORTED.get(datatype or "")
        if group and census:
            source = [dt for dt in census.get(predicate, {}) if dt in group]
            if len(source) == 1:
                datatype = source[0]
        return Literal(text, datatype=None if datatype == XSD_STRING else datatype)

    level0 = outgoing(roots)
    blank1 = {o for _, p, o, _ in level0 if o.startswith("_:") and p not in PARTITIONS}
    level1 = outgoing(blank1)
    level2 = outgoing({o for _, _, o, _ in level1 if o.startswith("_:")})
    graph = Graph()
    for s, p, o, d in [*level0, *level1, *level2]:
        graph.add((node(s, p, None), URIRef(p), node(o, p, d)))
    dataset = Dataset()
    dataset.default_context += graph
    return MetadataDocument(
        graph=graph, rdf_dataset=dataset, endpoint=store.manifest.get("endpoint")
    )


# Ontology terms as data


# (subject class, property, object class, datatype, subject binding, object binding)
_TermPatternKey = tuple[str, str, str, str | None, Literal["type", "term"], Literal["type", "term"]]


# Rows of a predicate counted at a time for the terms that are its values: a larger predicate is
# counted in parts by subject id, each joined with its share of the type table.
TERM_PART_ROWS = 50_000_000


def term_patterns(
    store: RowStore,
    typed: list[SchemaPattern] | None,
    *,
    ontology_graph_uris: list[str] | None = None,
) -> list[SchemaPattern]:
    """Return the patterns that use an ontology term as value or subject, with their triple
    counts: what ontology_as_data.probe_term_patterns returns from the endpoint.

    An ontology term is an IRI typed owl:Class or rdfs:Class (in the data scope, and in the
    *ontology_graph_uris* when given). With *typed* and no ontology graphs, the terms that are
    values are counted only for the (class, property) pairs whose typed pattern has object class
    owl:Class or rdfs:Class (build_term_object_pair_query); otherwise for every class and data
    property (build_term_object_query). Then every term as a subject: its data properties, by
    value kind, object class (the value itself when it is a term, else each type of the value),
    datatype and binding (build_term_subject_query); blank-node values are left out. A data
    property is outside the namespaces of ontology structure (NON_DATA_NAMESPACES) and not
    declared owl:AnnotationProperty. The rows are read once per predicate and joined by id.

    The type of a value is joined only when the value is not a term, as the left join of the
    subject query defines it (SPARQL: the FILTER of an OPTIONAL is its join condition). On a
    store with a graph scope each graph is counted apart (graphs) and the count is their sum,
    as add() merges the graph rows of the query.
    """
    import polars as pl

    from rdfsolve.mining.types import ONTOLOGY_METACLASSES
    from rdfsolve.ontology.vocabulary import NON_DATA_NAMESPACES, OWL_CLASS, RDFS_CLASS
    from rdfsolve.schema_models.pattern import SchemaPattern

    graph_uris = getattr(store, "graph_uris", None) or None
    semantics = (
        "quad_occurrences"
        if len(graph_uris or []) > 1
        else "triples_in_graph"
        if graph_uris
        else "endpoint_default"
    )
    class_terms = [f"<{OWL_CLASS}>", f"<{RDFS_CLASS}>"]
    annotation_class = f"<{OWL}AnnotationProperty>"
    # The type table is read by id (sid, c as a category) and never collected whole: the terms,
    # declared classes and annotation properties are the rows of a few classes, read first.
    rows_of_types = store.graph_types()
    types = rows_of_types.select("sid", c=pl.col("c").cast(pl.Categorical))
    if not getattr(store, "distinct_types", False):
        types = types.unique()
    wanted = pl.col("c").is_in([*class_terms, annotation_class])
    own = rows_of_types.filter(wanted).select("s", "sid", "c").unique().collect(engine="streaming")
    context = own
    if ontology_graph_uris:
        base = store.base
        extra = base.graph_types().filter(
            pl.col("g").is_in([f"<{g}>" for g in ontology_graph_uris]) & wanted
        )
        context = pl.concat([own, extra.select("s", "sid", "c").collect()]).unique()
    term_ids = (
        context.filter(pl.col("c").is_in(class_terms) & pl.col("s").str.starts_with("<"))
        .select("sid")
        .unique()
    )
    declared_classes = own.filter(pl.col("c").is_in(class_terms)).select("sid").unique().lazy()
    annotation = set(context.filter(pl.col("c") == annotation_class)["s"].str.slice(1).str.head(-1))
    data_predicates = [
        p for p in store.predicates if not p.startswith(NON_DATA_NAMESPACES) and p not in annotation
    ]
    counts: dict[_TermPatternKey, dict[str | None, int]] = {}

    def add(key: _TermPatternKey, frame: pl.DataFrame) -> None:
        """Add the (graph, count) rows of *frame* to the counts of *key*."""
        for graph, n in frame.iter_rows():
            per_graph = counts.setdefault(key, {})
            per_graph[graph] = per_graph.get(graph, 0) + n

    def edges(predicate: str) -> pl.LazyFrame:
        """Return the rows of *predicate* (s, o, d, kind, g, sid, oid); g is null outside a
        graph scope.
        """
        rows = store.graph_rows(predicate) if graph_uris else store.rows(predicate)
        if not graph_uris:
            rows = rows.with_columns(g=pl.lit(None, pl.String))
        return rows.select("s", "o", "d", "kind", "g", "sid", "oid")

    term_list = term_ids.lazy()
    term_objects = term_list.rename({"sid": "oid"})
    iri_s = pl.col("s").str.starts_with("<")

    def term_values(predicate: str, members: pl.LazyFrame) -> None:
        """Count the terms that are values of *predicate* by subject class (*members*: sid, c),
        in parts by subject id (TERM_PART_ROWS), each part joined with its share of the types.
        """
        rows = (
            edges(predicate)
            .filter(iri_s & (pl.col("kind") == "iri"))
            .join(term_objects, on="oid", how="semi")
            .join(declared_classes, on="sid", how="anti")
        )
        size = int(store.rows(predicate).select(pl.len()).collect(engine="streaming").item())
        parts = max(1, -(-size // TERM_PART_ROWS))
        found = []
        for part in range(parts):
            selected = pl.col("sid") % parts == part
            found.append(
                rows.filter(selected)
                .join(members.filter(selected), on="sid", how="inner")
                .group_by("c", "o", "g")
                .agg(n=pl.len())
                .collect(engine="streaming")
                .with_columns(pl.col("c").cast(pl.String))
            )
        total = pl.concat(found).group_by("c", "o", "g").agg(pl.col("n").sum())
        for (c, o), part_rows in total.group_by("c", "o"):
            add((c[1:-1], predicate, o[1:-1], None, "type", "term"), part_rows.select("g", "n"))

    if typed is not None and not ontology_graph_uris:
        pairs: dict[str, set[str]] = defaultdict(set)
        for pattern in typed:
            if pattern.object_class in (
                OWL_CLASS,
                RDFS_CLASS,
            ) and not pattern.property_uri.startswith(NON_DATA_NAMESPACES):
                pairs[pattern.property_uri].add(pattern.subject_class)
        for predicate in sorted(pairs):
            if predicate not in data_predicates:
                continue
            classes = [f"<{c}>" for c in pairs[predicate]]
            term_values(predicate, types.filter(pl.col("c").cast(pl.String).is_in(classes)))
    else:
        metaclasses = [f"<{m}>" for m in ONTOLOGY_METACLASSES]
        name = pl.col("c").cast(pl.String)
        members = types.filter(name.str.starts_with("<") & ~name.is_in(metaclasses))
        for predicate in data_predicates:
            term_values(predicate, members)
    value_types = types.filter(pl.col("c").cast(pl.String).str.starts_with("<")).rename(
        {"sid": "oid"}
    )
    for predicate in data_predicates:
        rows = (
            edges(predicate)
            .join(term_list, on="sid", how="semi")
            .filter(pl.col("kind") != "bnode")
            .collect(engine="streaming")
            .lazy()
        )
        is_term = rows.join(term_objects, on="oid", how="semi").with_columns(
            oc=pl.col("o"), binding=pl.lit("term")
        )
        rest = rows.join(term_objects, on="oid", how="anti")
        iri_rest = rest.filter(pl.col("kind") == "iri")
        # The types of these values only: the type table is read for them, not held whole.
        own_types = (
            value_types.join(iri_rest.select("oid").unique(), on="oid", how="semi")
            .with_columns(pl.col("c").cast(pl.String))
            .collect(engine="streaming")
            .lazy()
        )
        iris = (
            iri_rest.join(own_types, on="oid", how="left")
            .with_columns(oc=pl.col("c").fill_null("Resource"), binding=pl.lit("type"))
            .drop("c")
        )
        literals = rest.filter(pl.col("kind") == "literal").with_columns(
            oc=pl.lit("Literal"), binding=pl.lit("type")
        )
        found = (
            pl.concat([is_term, iris, literals], how="diagonal_relaxed")
            .with_columns(dt=pl.when(pl.col("kind") == "literal").then(pl.col("d")))
            .group_by("s", "oc", "dt", "binding", "g")
            .agg(n=pl.len())
            .collect()
        )
        for (s, oc, dt, binding), part in found.group_by("s", "oc", "dt", "binding"):
            oc = oc[1:-1] if oc.startswith("<") else oc
            add((s[1:-1], predicate, oc, dt, "term", binding), part.select("g", "n"))
    patterns = []
    for (sc, predicate, oc, dt, subject_binding, object_binding), per_graph in sorted(
        counts.items(), key=lambda item: tuple(x or "" for x in item[0])
    ):
        named = {g[1:-1]: n for g, n in per_graph.items() if g is not None}
        patterns.append(
            SchemaPattern(
                subject_class=sc,
                property_uri=predicate,
                object_class=oc,
                datatype=dt,
                subject_binding=subject_binding,
                object_binding=object_binding,
                count=sum(per_graph.values()),
                count_semantics=semantics,
                graphs=named or None,
            )
        )
    return patterns
