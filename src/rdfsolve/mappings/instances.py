"""Mappings between datasets derived from the instances that external mapping sets relate.

An external mapping set (SSSOM) relates identifiers. Either end of a row can be a class of a
mined schema (the class mappings of rdfsolve.mappings.enrichment) or an instance in a dataset:

- subject form: a record named by the identifier, typed with class X;
- value form: a value of property p of records of class X: an IRI in a registered URI form, or
  a CURIE or local identifier literal where the property's identifier signature
  (rdfsolve.mappings.signatures) says that it carries that identifier type.

Membership is decided per dataset. A dataset with a local scan store
(rdfsolve.mining.scan.RowStore) is read there, by id joins over its type and row tables, and
its instances are counted (validated). Any other dataset is matched by namespace only: the
identifier types of its schema's class examples and signatures, without counts (asserted).

Three outputs follow, each naming the mapping sets it comes from:

- instance mappings (SSSOM): the external rows that join records of two datasets, with the
  datasets as subject and object source and the effective confidence of the policy;
- VoID linksets per (dataset A, dataset B, mapping predicate, mapping set), with the number of
  record pairs for local datasets. Where both datasets write the same IRI for an entity, the
  entity is shared, not bridged: such pairs are counted apart and make no linkset, since the
  direct links (rdfsolve.mappings.void.links_to_void) already state them;
- instance-derived class mappings (SSSOM): one row per (dataset A, class X, via property p) and
  (dataset B, class Y), with the record pairs, the coverage of each class, a predicate set by
  the coverage, the Jaccard index of the mapped members and the mean confidence of the rows.

Record pairs are counted per pair of identifiers: a pair of records joined by two identifier
pairs counts twice. The members taking part are counted exactly.
"""

from __future__ import annotations

import csv
import json
import logging
import time
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from math import isnan, sqrt
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from rdfsolve.mappings.confidence import (
    ConfidencePolicy,
    agent_type,
    edge_type,
    knowledge_level,
)
from rdfsolve.mappings.keys import IdentifierReader
from rdfsolve.schema_models.class_parts import ClassParts

if TYPE_CHECKING:
    import polars as pl

    from rdfsolve.schema_models.core import MinedSchema

log = logging.getLogger(__name__)

__all__ = [
    "DerivedClassMapping",
    "InstanceSettings",
    "derive_instance_mappings",
    "derived_predicate",
    "instance_inputs",
    "lower_share",
    "read_derived_mappings",
    "read_instance_links",
    "support",
]

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
JUSTIFICATION = "semapv:InstanceBasedMatching"
SIMILARITY = (
    "Jaccard index of the mapped members: m / (|X| + |Y| - m), m the smaller number of members "
    "taking part"
)
# The predicate of the reverse mapping, by local name (the others are their own reverse).
_REVERSE = {
    "narrowMatch": "broadMatch",
    "broadMatch": "narrowMatch",
    "crossSpeciesNarrowMatch": "crossSpeciesBroadMatch",
    "crossSpeciesBroadMatch": "crossSpeciesNarrowMatch",
}
# A group of instances: the dataset, the form, the class and the property (value form).
GROUP = ("ds", "form", "cls", "prop")
# Properties whose value identifies the record that holds it: through them the record itself
# takes part, as in subject form.
IDENTITY_PROPERTIES = frozenset(
    {
        "http://www.w3.org/2002/07/owl#sameAs",
        "http://www.w3.org/2004/02/skos/core#exactMatch",
        "http://purl.org/dc/elements/1.1/identifier",
        "http://purl.org/dc/terms/identifier",
        "http://schema.org/identifier",
        "https://schema.org/identifier",
        "http://schema.org/sameAs",
        "https://schema.org/sameAs",
    }
)
# Classes of the modelling languages (an OWL class, axiom or restriction, an RDF property, a
# shape): what a vocabulary is written with, not what a dataset describes.
META_NAMESPACES = (
    "http://www.w3.org/2002/07/owl#",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/ns/shacl#",
)
# The rows that join an identifier written by two datasets with itself.
SAME_IDENTIFIER = "same_identifier"
# The form of an end where the record itself takes part (subject form or an identity property).
RECORD = "record"


@dataclass(frozen=True)
class InstanceSettings:
    """Settings of the instance-derived mappings.

    *exact* is the coverage at which a class counts as covered: both classes covered give
    skos:exactMatch, one gives skos:broadMatch or skos:narrowMatch (the covered class is the
    narrower), none skos:relatedMatch. *validated* is the support (the coverage the predicate
    claims, see support, at the lower bound of its interval) from which a counted mapping is
    validated; below it, a candidate.
    *predicates* keeps the rows of these mapping predicates (local names; every mapping
    predicate when None). *policy* gives each row its effective confidence.
    *identity_properties* are the properties whose value identifies its record. With
    *same_identifier*, an identifier that two local datasets both hold relates their records
    as a mapping row would (confidence 1).
    """

    exact: float = 0.95
    validated: float = 0.5
    predicates: frozenset[str] | None = None
    policy: ConfidencePolicy = field(default_factory=ConfidencePolicy)
    identity_properties: frozenset[str] = IDENTITY_PROPERTIES
    same_identifier: bool = True

    def __post_init__(self) -> None:
        """Refuse a coverage threshold outside 0..1."""
        for value in (self.exact, self.validated):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"A coverage threshold is between 0 and 1, not {value}")


@dataclass(frozen=True)
class DerivedClassMapping:
    """A relation between classes of two datasets derived from their instances.

    A class mapping (*kind* class_mapping) relates the records themselves: the identifiers
    name them (subject form, or the value of an identity property). A record link (*kind*
    record_link) relates records of one class to records of the other through a property of
    either (*source_property*, *target_property*): the records cite what the others are, or
    both cite the same thing. It has no mapping predicate.
    """

    source_dataset: str
    source_class: str
    target_dataset: str
    target_class: str
    predicate: str | None
    # validated (counted, support at least the threshold), candidate (counted, below it) or
    # asserted (an end matched by namespace, without counts)
    validation: str
    confidence: float | None
    source_property: str | None = None
    target_property: str | None = None
    pairs: int | None = None
    source_coverage: float | None = None
    target_coverage: float | None = None
    similarity: float | None = None
    mapping_sets: tuple[str, ...] = ()
    kind: str = "class_mapping"
    # The part of each class (rdfsolve.schema_models.class_parts, ClassPart.key) a part-level
    # mapping relates; None for a mapping of the whole classes.
    source_part: str | None = None
    target_part: str | None = None
    # What the SSSOM row records besides (in other): members, members taking part, ...
    details: dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    def edge(self) -> dict[str, Any]:
        """Return the attributes of the network edge of the mapping (rdfsolve.analysis)."""
        return {
            "confidence": self.confidence,
            "justification": JUSTIFICATION,
            "mapping_sets": list(self.mapping_sets),
            "derivation": "instance_based",
            "predicate_edge_type": edge_type(self.predicate) if self.predicate else None,
            "edge_type": "INSTANCE_DERIVED" if self.kind == "class_mapping" else "INSTANCE_LINK",
            "knowledge_level": knowledge_level(JUSTIFICATION),
            "agent_type": agent_type(JUSTIFICATION),
            "validation": self.validation,
            "evidence": {"validated": "confirmed", "candidate": "tested"}.get(
                self.validation, "plausible"
            ),
            "pairs": self.pairs,
            "source_coverage": self.source_coverage,
            "target_coverage": self.target_coverage,
            "source_property": self.source_property,
            "target_property": self.target_property,
        }


def derived_predicate(
    source_coverage: float | None, target_coverage: float | None, exact: float = 0.95
) -> str:
    """Return the predicate of a derived class mapping by the coverage of its classes.

    Both covered: skos:exactMatch; only the source: skos:broadMatch (the source is the
    narrower); only the target: skos:narrowMatch; none, or no counts: skos:relatedMatch.
    """
    left = source_coverage is not None and source_coverage >= exact
    right = target_coverage is not None and target_coverage >= exact
    if left and right:
        return "skos:exactMatch"
    if left:
        return "skos:broadMatch"
    if right:
        return "skos:narrowMatch"
    return "skos:relatedMatch"


def lower_share(taking: int | None, members: int | None, z: float = 1.96) -> float | None:
    """Return the lower bound of the Wilson interval of a share (*taking* of *members*), so
    that the share of a small class weighs as little as it shows (one of one: 0.21).
    """
    if taking is None or not members:
        return None
    share = taking / members
    centre = share + z * z / (2 * members)
    spread = z * sqrt(share * (1 - share) / members + z * z / (4 * members * members))
    return max(0.0, (centre - spread) / (1 + z * z / members))


def support(
    predicate: str | None, source_share: float | None, target_share: float | None
) -> float | None:
    """Return the share of members that bear out a derived relation.

    skos:exactMatch claims both classes: the smaller share; skos:broadMatch the source;
    skos:narrowMatch the target; a related match or a record link the larger share.
    """
    if source_share is None or target_share is None:
        return None
    if predicate == "skos:exactMatch":
        return min(source_share, target_share)
    if predicate == "skos:broadMatch":
        return source_share
    if predicate == "skos:narrowMatch":
        return target_share
    return max(source_share, target_share)


def _local_name(term: str) -> str:
    """Return the local name of a CURIE or IRI."""
    for separator in ("#", "/", ":"):
        term = term.rsplit(separator, 1)[-1]
    return term


def _reverse(predicate: str) -> str:
    """Return the predicate of the reverse mapping, written with the same prefix."""
    name = _local_name(predicate)
    return (
        predicate[: len(predicate) - len(name)] + _REVERSE[name] if name in _REVERSE else predicate
    )


# Mapping rows ---------------------------------------------------------------------------


@dataclass
class _Source:
    """A mapping set of the registry and the files it was read from."""

    name: str
    entry: dict[str, Any]
    files: list[Path]
    set_ids: list[str] = field(default_factory=list)

    @property
    def oxo(self) -> str | None:
        """Return how OxO identifies the set, when the registry says so."""
        value = self.entry.get("oxo_mapping_set_id")
        return str(value) if value else None


def _set_id(header: list[str], fallback: str) -> str:
    """Return the mapping_set_id of a mapping set's metadata, else *fallback*."""
    metadata = yaml.safe_load("".join(line[1:] for line in header)) or {}
    return str(metadata.get("mapping_set_id") or fallback)


def mapping_rows(
    path: Path,
    reader: IdentifierReader,
    *,
    fallback_id: str,
    settings: InstanceSettings,
    set_confidence: float | None = None,
    ignore_confidence: bool = False,
) -> tuple[pl.LazyFrame | None, str]:
    """Read a mapping set as rows with the identifier keys of their ends.

    Return the rows (subject_id, predicate_id, object_id, mapping_justification, confidence:
    the effective one, stated: the set's own, mapping_source, s_key, o_key) and the set's id.
    Rows without a justification, with a hierarchy predicate or a predicate the settings leave
    out are dropped; a row of the set may name its own set (mapping_set_id), which is kept.
    """
    import polars as pl

    from rdfsolve.mappings.enrichment import _filtered_rows, _prefix_map

    header, columns, _ = _filtered_rows(path)
    set_id = _set_id(header, fallback_id)
    if columns is None or not {"subject_id", "predicate_id", "object_id"} <= set(columns):
        return None, set_id
    prefix_map, _ = _prefix_map(header)
    frame = pl.scan_csv(
        path,
        separator="\t",
        comment_prefix="#",
        quote_char=None,
        infer_schema=False,
        truncate_ragged_lines=True,
        encoding="utf8-lossy",
    )
    wanted = ("mapping_justification", "confidence", "mapping_set_id")
    frame = frame.select(
        "subject_id",
        "predicate_id",
        "object_id",
        *[pl.col(c) if c in columns else pl.lit(None, dtype=pl.String).alias(c) for c in wanted],
    )
    predicates = frame.select(pl.col("predicate_id").drop_nulls().unique()).collect()
    kept = [
        p
        for p in predicates["predicate_id"].to_list()
        if edge_type(p) is not None
        and (settings.predicates is None or _local_name(p) in settings.predicates)
    ]
    justifications = (
        frame.select(pl.col("mapping_justification").drop_nulls().unique())
        .collect()["mapping_justification"]
        .to_list()
    )
    policy = settings.policy
    defaults = {j: policy.of(None, j, set_confidence, ignore_confidence) for j in justifications}
    stated = pl.col("confidence").cast(pl.Float64, strict=False)
    effective = (
        pl.when(stated.is_not_null() & pl.lit(not ignore_confidence))
        .then(stated)
        .otherwise(
            pl.col("mapping_justification").replace_strict(
                defaults, default=None, return_dtype=pl.Float64
            )
        )
    )
    frame = (
        frame.filter(
            pl.col("predicate_id").is_in(kept)
            & pl.col("mapping_justification").is_not_null()
            & (pl.col("mapping_justification") != "")
        )
        .with_columns(
            effective.alias("confidence"),
            stated.alias("stated"),
            pl.coalesce(pl.col("mapping_set_id"), pl.lit(set_id)).alias("mapping_source"),
        )
        .drop("mapping_set_id")
    )
    frame = reader.curie_keys(frame, "subject_id", "s_key", prefix_map)
    frame = reader.curie_keys(frame, "object_id", "o_key", prefix_map)
    return frame, set_id


# Occurrences of identifiers in the datasets ---------------------------------------------------


def _iri(term: pl.Expr) -> pl.Expr:
    """Return the IRI of a term written as in SPARQL TSV (<iri>)."""
    return term.str.strip_prefix("<").str.strip_suffix(">")


def store_occurrences(
    dataset: str,
    store_path: Path,
    schema: MinedSchema | None,
    reader: IdentifierReader,
    keys: pl.DataFrame,
    identity_properties: frozenset[str] = IDENTITY_PROPERTIES,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return the instances of a local store whose identifiers are in *keys*, the members of
    their classes, and the identity claims of the store's records.

    An instance is (key, ds, form, cls, prop, sid, h, part): the identifier, the dataset, its
    form (subject or value), the class of the record, the property (value form), QLever's id of
    the record, a hash of the IRI as written (None for a literal) and, for the subject form, the
    part of the class the record falls in (rdfsolve.mining.class_parts). The members table is
    (ds, cls, members). An identity claim is (ds, sid, cls, part, own_key, prop, value_key): a
    record that states an identifier (value_key) with one of *identity_properties*, and the
    record's own identifier, if any. The store is read one predicate at a time; only
    identifiers in *keys* are kept as instances, so that memory follows the matched instances.
    """
    import polars as pl

    from rdfsolve.mining.class_parts import classify
    from rdfsolve.mining.scan import RowStore

    store = RowStore(store_path)
    wanted = keys.lazy()
    class_ids = store.graph_types().select("sid", _iri(pl.col("c")).alias("cls")).unique()
    found: list[pl.DataFrame] = []
    stated: list[pl.DataFrame] = []
    parts = _store_parts(store, schema, reader)
    _, subjects = next(parts)
    found.append(
        subjects.join(wanted, on="key", how="semi")
        .select(
            "key",
            pl.lit(dataset).alias("ds"),
            pl.lit("subject").alias("form"),
            "cls",
            pl.lit(None, dtype=pl.String).alias("prop"),
            "sid",
            "h",
        )
        .collect()
    )
    # Identity claims are read with every registered prefix: a claimed identifier of a prefix
    # that no mapping set names is a claim that no set bears out.
    for predicate, held in _store_parts(store, schema, IdentifierReader(), identity_properties):
        stated.append(
            held.select("sid", pl.lit(predicate).alias("prop"), pl.col("key").alias("value_key"))
            .unique()
            .collect()
        )
    for predicate, held in parts:
        values = (
            held.join(wanted, on="key", how="semi")
            .join(class_ids, on="sid", how="inner")
            .select(
                "key",
                pl.lit(dataset).alias("ds"),
                pl.lit("value").alias("form"),
                "cls",
                pl.lit(predicate).alias("prop"),
                "sid",
                "h",
            )
            .unique()
            .collect()
        )
        if values.height:
            found.append(values)
    # A value that names its own record (the record's identifier, written again as a value) is
    # the record in subject form, not another instance.
    named = found[0].select("sid", "key").unique()
    occurrences = pl.concat(
        [found[0], *[part.join(named, on=["sid", "key"], how="anti") for part in found[1:]]]
    )
    claims = (
        pl.concat(stated)
        if stated
        else pl.DataFrame(schema={"sid": pl.UInt64, "prop": pl.String, "value_key": pl.String})
    )
    # The part and the own identifier of the records that take part or state identity, read
    # with every registered prefix, as the class parts are counted.
    sids = pl.concat([occurrences.select("sid"), claims.select("sid")]).unique()
    records = (
        classify(
            store.graph_types().select("sid", "s", "c").join(sids.lazy(), on="sid", how="semi"),
            "s",
            IdentifierReader(),
        )
        .select("sid", _iri(pl.col("c")).alias("cls"), "part", pl.col("node_key").alias("own_key"))
        .unique()
        .collect()
    )
    occurrences = occurrences.join(
        records.select("sid", "cls", "part"), on=["sid", "cls"], how="left"
    ).with_columns(pl.when(pl.col("form") == "subject").then(pl.col("part")).alias("part"))
    claims = claims.join(records, on="sid").select(
        pl.lit(dataset).alias("ds"), "sid", "cls", "part", "own_key", "prop", "value_key"
    )
    classes = occurrences.select("cls").unique()
    members = (
        class_ids.join(classes.lazy(), on="cls", how="semi")
        .group_by("cls")
        .agg(pl.col("sid").n_unique().alias("members"))
        .select(pl.lit(dataset).alias("ds"), "cls", "members")
        .collect()
    )
    return occurrences, members, claims


def _store_parts(
    store: Any,
    schema: MinedSchema | None,
    reader: IdentifierReader,
    predicates: Iterable[str] | None = None,
) -> Iterator[tuple[str | None, pl.LazyFrame]]:
    """Yield the identifiers a store holds, part by part: first the typed records named by an
    identifier (None, frame of key, cls, sid, h), then per predicate the values that are
    identifiers (predicate, frame of key, sid, h); h hashes the IRI as written (None for a
    literal). A literal is read where the property's identifier signature names its type.
    With *predicates*, only the values of those predicates are read (no records first).
    """
    import polars as pl

    from rdfsolve.mappings.signatures import UNTYPED, signatures

    membership = set(store.manifest.get("membership") or [RDF_TYPE]) | {RDF_TYPE}
    wanted = None if predicates is None else set(predicates)
    if wanted is None:
        subjects = (
            store.graph_types()
            .select("sid", "s", "c")
            .filter(pl.col("s").str.starts_with("<"))
            .select("sid", _iri(pl.col("s")).alias("iri"), _iri(pl.col("c")).alias("cls"))
            .unique()
        )
        yield (
            None,
            reader.iri_keys(subjects, "iri")
            .filter(pl.col("key").is_not_null())
            .select("key", "cls", "sid", pl.col("iri").hash().alias("h")),
        )
    literal_types: dict[str, set[str]] = {}
    if schema is not None:
        for kind, owners in signatures(schema).values.items():
            for owner, prop in owners:
                if owner != UNTYPED:
                    literal_types.setdefault(prop, set()).add(kind)
    for predicate in sorted(store.predicates):
        if predicate in membership or (wanted is not None and predicate not in wanted):
            continue
        rows = store.rows(predicate).select("sid", "o", "kind")
        iris = reader.iri_keys(
            rows.filter(pl.col("kind") == "iri").select("sid", _iri(pl.col("o")).alias("iri")),
            "iri",
        ).select("sid", "key", pl.col("iri").hash().alias("h"))
        parts = [iris]
        for kind in sorted(literal_types.get(predicate, ())):
            text = rows.filter(pl.col("kind") == "literal").select(
                "sid", pl.col("o").str.extract(r'^"(.*)"', 1).alias("text")
            )
            curies = reader.curie_keys(text, "text", "curie")
            locals_ = reader.local_keys(curies, "text", kind, "bare")
            parts.append(
                locals_.select(
                    "sid",
                    pl.coalesce("curie", "bare").alias("key"),
                    pl.lit(None, dtype=pl.UInt64).alias("h"),
                )
            )
        yield predicate, pl.concat(parts).filter(pl.col("key").is_not_null())


def store_keys(
    store_path: Path, schema: MinedSchema | None, reader: IdentifierReader
) -> pl.DataFrame:
    """Return the identifiers that the records of a local store hold (key), each once."""
    import polars as pl

    from rdfsolve.mining.scan import RowStore

    found = [
        part.select("key").unique().collect(engine="streaming")
        for _, part in _store_parts(RowStore(store_path), schema, reader)
    ]
    return pl.concat(found).unique() if found else pl.DataFrame(schema={"key": pl.String})


def shared_identifiers(
    stores: Mapping[str, Path], schemas: Mapping[str, MinedSchema], reader: IdentifierReader
) -> pl.DataFrame:
    """Return the identifiers held by records of at least two local stores (key)."""
    import polars as pl

    held = [
        store_keys(path, schemas.get(name), reader).with_columns(pl.lit(name).alias("ds"))
        for name, path in sorted(stores.items())
    ]
    if len(held) < 2:
        return pl.DataFrame(schema={"key": pl.String})
    return (
        pl.concat(held)
        .group_by("key")
        .agg(pl.col("ds").n_unique().alias("n"))
        .filter(pl.col("n") > 1)
        .select("key")
    )


def same_identifier_rows(keys: pl.DataFrame, set_id: str) -> pl.DataFrame:
    """Return the rows that join each identifier with itself, in the columns of mapping_rows."""
    import polars as pl

    return keys.select(
        pl.col("key").alias("subject_id"),
        pl.lit("skos:exactMatch").alias("predicate_id"),
        pl.col("key").alias("object_id"),
        pl.lit("semapv:UnspecifiedMatching").alias("mapping_justification"),
        pl.lit(1.0).alias("confidence"),
        pl.lit(None, dtype=pl.Float64).alias("stated"),
        pl.lit(set_id).alias("mapping_source"),
        pl.col("key").alias("s_key"),
        pl.col("key").alias("o_key"),
    )


def namespace_occurrences(
    dataset: str, schema: MinedSchema, reader: IdentifierReader
) -> pl.DataFrame:
    """Return the identifier types of a dataset known by its schema alone (no local store).

    A row (prefix, ds, form, cls, prop): the subjects of class cls are identifiers of the
    prefix (their example IRIs, as the namespaces of the class's node shape), or the values
    of prop on cls are (the identifier signature of the schema).
    """
    import polars as pl

    from rdfsolve.mappings.signatures import UNTYPED, signatures

    rows: list[tuple[str, str, str, str | None]] = []
    found = signatures(schema)
    for kind, pairs in found.values.items():
        rows += [(kind, "value", owner, prop) for owner, prop in pairs if owner != UNTYPED]
    for kind, classes in found.subjects.items():
        rows += [(kind, "subject", owner, None) for owner in classes if owner != UNTYPED]
    examples = [
        (cls, term.value)
        for cls, terms in (schema.enrichment.class_examples if schema.enrichment else {}).items()
        for term in terms
        if term.kind == "uri"
    ]
    if examples:
        frame = pl.LazyFrame(
            {"cls": [c for c, _ in examples], "iri": [i for _, i in examples]},
            schema={"cls": pl.String, "iri": pl.String},
        )
        keyed = reader.iri_keys(frame, "iri").filter(pl.col("key").is_not_null()).collect()
        rows += [
            (key.split(":", 1)[0], "subject", cls, None)
            for cls, key in keyed.select("cls", "key").iter_rows()
        ]
    wanted = reader.prefixes
    rows = [r for r in rows if wanted is None or r[0] in wanted]
    return pl.DataFrame(
        {
            "prefix": [r[0] for r in rows],
            "ds": [dataset] * len(rows),
            "form": [r[1] for r in rows],
            "cls": [r[2] for r in rows],
            "prop": [r[3] for r in rows],
        },
        schema={
            "prefix": pl.String,
            "ds": pl.String,
            "form": pl.String,
            "cls": pl.String,
            "prop": pl.String,
        },
    ).unique()


# Identity claims ------------------------------------------------------------------------------

# Local names of mapping predicates that state that two identifiers name the same entity.
SAME_ENTITY = frozenset({"exactMatch", "sameAs", "equivalentClass", "equivalentProperty"})
# Outcomes of the check under which a record counts as the identifier it claims.
SOUND = frozenset({"confirmed", "record_without_registered_identifier"})


def _empty_claims() -> pl.DataFrame:
    """Return an identity claim table without rows."""
    import polars as pl

    return pl.DataFrame(
        schema={
            "ds": pl.String,
            "sid": pl.UInt64,
            "cls": pl.String,
            "part": pl.String,
            "own_key": pl.String,
            "prop": pl.String,
            "value_key": pl.String,
        }
    )


def check_claims(claims: pl.DataFrame, files: Sequence[tuple[str, Path]]) -> pl.DataFrame:
    """Check each identity claim (a record's own identifier and the identifier claimed for it)
    against the mapping rows of *files* (set name, rows of mapping_rows), in either direction.

    Add the outcome of the check (outcome, one of CHECK_OUTCOMES) and the sets and predicates
    by which the two identifiers are related (sets, predicates).
    """
    import polars as pl

    prefix = r"^([^:]+):"
    pairs = [
        pl.scan_parquet(path).select(
            pl.col(a).alias("own_key"),
            pl.col(b).alias("value_key"),
            "predicate_id",
            pl.lit(name).alias("set"),
        )
        for name, path in files
        for a, b in (("s_key", "o_key"), ("o_key", "s_key"))
    ]
    wanted = (
        claims.filter(
            pl.col("own_key").is_not_null()
            & (pl.col("own_key") != pl.col("value_key"))
            & (
                pl.col("own_key").str.extract(prefix, 1)
                != pl.col("value_key").str.extract(prefix, 1)
            )
        )
        .select("own_key", "value_key")
        .unique()
    )
    if pairs and wanted.height:
        local = pl.col("predicate_id").str.extract(r"([^:/#]+)$", 1)
        related = (
            pl.concat(pairs)
            .join(wanted.lazy(), on=["own_key", "value_key"], how="semi")
            .group_by("own_key", "value_key")
            .agg(
                local.is_in(sorted(SAME_ENTITY)).any().alias("same"),
                pl.col("set").unique().sort().alias("sets"),
                pl.col("predicate_id").unique().sort().alias("predicates"),
            )
            .collect()
        )
    else:
        related = pl.DataFrame(
            schema={
                "own_key": pl.String,
                "value_key": pl.String,
                "same": pl.Boolean,
                "sets": pl.List(pl.String),
                "predicates": pl.List(pl.String),
            }
        )
    own_prefix = pl.col("own_key").str.extract(prefix, 1)
    outcome = (
        pl.when(pl.col("own_key").is_null())
        .then(pl.lit("record_without_registered_identifier"))
        .when(pl.col("own_key") == pl.col("value_key"))
        .then(pl.lit("self"))
        .when(own_prefix == pl.col("value_key").str.extract(prefix, 1))
        .then(pl.lit("conflicting"))
        .when(pl.col("same").fill_null(value=False))
        .then(pl.lit("confirmed"))
        .when(pl.col("sets").is_not_null())
        .then(pl.lit("weaker"))
        .otherwise(pl.lit("unconfirmed"))
    )
    return claims.join(related, on=["own_key", "value_key"], how="left").with_columns(
        outcome.alias("outcome")
    )


def promote_confirmed(occ: pl.DataFrame, claims: pl.DataFrame) -> pl.DataFrame:
    """Return the instances with the values of identity properties that a check bears out
    (SOUND) written as the record itself (subject form, the record's part); the others stay
    values of their property.
    """
    import polars as pl

    sound = (
        claims.filter(pl.col("outcome").is_in(sorted(SOUND)))
        .select(
            "ds",
            "sid",
            "cls",
            "prop",
            pl.col("value_key").alias("key"),
            pl.col("part").alias("_part"),
            pl.lit(value=True).alias("_sound"),
        )
        .unique()
    )
    marked = occ.join(sound, on=["ds", "sid", "cls", "prop", "key"], how="left")
    promoted = pl.col("_sound").fill_null(value=False)
    return (
        marked.with_columns(
            pl.when(promoted).then(pl.lit("subject")).otherwise(pl.col("form")).alias("form"),
            pl.when(promoted)
            .then(pl.lit(None, dtype=pl.String))
            .otherwise(pl.col("prop"))
            .alias("prop"),
            pl.when(promoted).then(pl.col("_part")).otherwise(pl.col("part")).alias("part"),
        )
        .drop("_sound", "_part")
        .unique(subset=["key", "ds", "form", "cls", "prop", "sid"])
    )


def _class_parts(store_path: Path, found: pl.DataFrame) -> ClassParts:
    """Return the parts of the classes that hold instances in *found* (counted in the store)."""
    from rdfsolve.mining.class_parts import class_parts
    from rdfsolve.mining.scan import RowStore

    classes = found["cls"].unique().to_list()
    return class_parts(RowStore(store_path).members(), classes)


# Joining rows with occurrences ------------------------------------------------------------------


def _side(frame: pl.LazyFrame, suffix: str) -> pl.LazyFrame:
    """Rename a group's columns for one end of a pair (_a or _b)."""
    return frame.rename({c: f"{c}{suffix}" for c in frame.collect_schema()})


def _oriented(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Write each pair from the dataset that sorts first, with the predicate reversed when the
    ends are swapped, so that a row and its inverse meet.
    """
    import polars as pl

    names = frame.collect_schema().names()
    ends = [c[:-2] for c in names if c.endswith("_a") and f"{c[:-2]}_b" in names]
    swap = pl.col("ds_a") > pl.col("ds_b")
    predicates = frame.select(pl.col("predicate_id").unique()).collect()["predicate_id"].to_list()
    reverse = {p: _reverse(p) for p in predicates}
    return frame.with_columns(
        *[
            pl.when(swap).then(pl.col(f"{c}_b")).otherwise(pl.col(f"{c}_a")).alias(f"{c}_a")
            for c in ends
        ],
        *[
            pl.when(swap).then(pl.col(f"{c}_a")).otherwise(pl.col(f"{c}_b")).alias(f"{c}_b")
            for c in ends
        ],
        pl.when(swap)
        .then(pl.col("predicate_id").replace_strict(reverse, return_dtype=pl.String))
        .otherwise(pl.col("predicate_id"))
        .alias("predicate_id"),
    )


@dataclass
class _Joined:
    """What one mapping set contributes, before the outputs are written."""

    instance_rows: pl.DataFrame  # one row per external row and dataset pair (local)
    local: pl.DataFrame  # oriented (key pair, group pair) combinations, local on both ends
    asserted: pl.DataFrame  # (group pair, predicate) with a remote end: rows and confidence


def join_rows(rows: pl.LazyFrame, groups: pl.DataFrame, namespaces: pl.DataFrame) -> _Joined:
    """Join mapping rows with the instance groups of the datasets.

    *groups* is (key, ds, form, cls, prop): the groups of instances that hold an identifier;
    *namespaces* the identifier types of the datasets without a store.
    """
    import polars as pl

    rows = rows.with_row_index("row")
    group_a = _side(groups.lazy(), "_a")
    group_b = _side(groups.lazy(), "_b")
    local = (
        rows.join(group_a, left_on="s_key", right_on="key_a")
        .join(group_b, left_on="o_key", right_on="key_b")
        .filter(pl.col("ds_a") != pl.col("ds_b"))
        .with_columns(pl.col("s_key").alias("key_a"), pl.col("o_key").alias("key_b"))
        .collect()
        .lazy()
    )
    instance_rows = (
        local.select(
            "row",
            "subject_id",
            "predicate_id",
            "object_id",
            "mapping_justification",
            "confidence",
            "stated",
            "mapping_source",
            "s_key",
            "o_key",
            pl.col("ds_a").alias("subject_dataset"),
            pl.col("ds_b").alias("object_dataset"),
        )
        .unique(subset=["row", "subject_dataset", "object_dataset"])
        .drop("row")
        .collect()
    )
    oriented = _oriented(
        local.select(
            "row",
            "predicate_id",
            "confidence",
            "key_a",
            "key_b",
            *[f"{c}_a" for c in GROUP],
            *[f"{c}_b" for c in GROUP],
        )
    ).collect()
    return _Joined(instance_rows, oriented, _asserted(rows, groups, namespaces))


def pair_counts(combos: pl.DataFrame, occ: pl.DataFrame, columns: Sequence[str]) -> pl.DataFrame:
    """Count the record pairs of each identifier pair, and those that share an entity.

    *combos* has key_a, key_b and the *columns* of each end (ds_a ..., ds_b ...); *occ* the
    instances (key, ds, form, cls, prop, sid, h). For each combination, n_a and n_b are the
    records of each end that hold the identifier, pairs = n_a * n_b, and shared the pairs whose
    records write one of the two identifiers as the same IRI (the record of A holds key_b as
    B writes it, or the record of B holds key_a as A writes it): a direct link, not a bridge.
    """
    import polars as pl

    ends = [*[f"{c}_a" for c in columns], *[f"{c}_b" for c in columns]]
    combos = combos.select("key_a", "key_b", *ends).unique().with_row_index("combo")
    records = occ.lazy().select("key", *columns, "sid").unique()
    written = occ.lazy().filter(pl.col("h").is_not_null()).select("key", "ds", "sid", "h")
    # (key, ds holding it in a record, sid of that record, ds writing it so as well)
    alike = (
        written.join(
            written.select("key", "h", pl.col("ds").alias("other")).unique(), on=["key", "h"]
        )
        .filter(pl.col("ds") != pl.col("other"))
        .select("key", "ds", "sid", "other")
        .unique()
    )
    counted = []
    for side, other in (("a", "b"), ("b", "a")):
        held = (
            combos.lazy()
            .join(
                records.rename({"key": f"key_{side}", **{c: f"{c}_{side}" for c in columns}}),
                on=[f"key_{side}", *[f"{c}_{side}" for c in columns]],
                nulls_equal=True,
            )
            .join(
                alike.rename(
                    {"key": f"key_{other}", "ds": f"ds_{side}", "other": f"ds_{other}"}
                ).with_columns(pl.lit(value=True).alias("_alike")),
                on=[f"key_{other}", f"ds_{side}", "sid", f"ds_{other}"],
                how="left",
            )
            .group_by("combo")
            .agg(
                pl.col("sid").n_unique().alias(f"n_{side}"),
                pl.col("sid")
                .filter(pl.col("_alike").fill_null(value=False))
                .n_unique()
                .alias(f"s_{side}"),
            )
        )
        counted.append(held)
    frame = (
        combos.lazy()
        .join(counted[0], on="combo")
        .join(counted[1], on="combo")
        .with_columns(
            (pl.col("n_a").cast(pl.Int64) * pl.col("n_b")).alias("pairs"),
            (
                pl.col("s_a").cast(pl.Int64) * pl.col("n_b")
                + pl.col("s_b").cast(pl.Int64) * pl.col("n_a")
                - pl.col("s_a").cast(pl.Int64) * pl.col("s_b")
            ).alias("shared"),
        )
        .drop("combo", "s_a", "s_b")
    )
    return frame.collect()


def _asserted(rows: pl.LazyFrame, groups: pl.DataFrame, namespaces: pl.DataFrame) -> pl.DataFrame:
    """Return the group pairs with an end known by namespace only: per (group A, group B,
    predicate), the rows and the sum of their confidences. A local end must hold the identifier.
    """
    import polars as pl

    columns = [*[f"{c}_a" for c in GROUP], *[f"{c}_b" for c in GROUP], "predicate_id"]
    empty = pl.DataFrame(
        schema={
            **dict.fromkeys(columns, pl.String),
            "rows": pl.UInt32,
            "confidence_sum": pl.Float64,
        }
    )
    if not namespaces.height:
        return empty
    rows = rows.with_columns(
        pl.col("s_key").str.extract(r"^([^:]+):", 1).alias("s_prefix"),
        pl.col("o_key").str.extract(r"^([^:]+):", 1).alias("o_prefix"),
    )
    remote = namespaces.lazy()
    local = groups.lazy().select("key", *GROUP).unique()
    parts = [
        rows.join(_side(local, "_a"), left_on="s_key", right_on="key_a").join(
            _side(remote, "_b"), left_on="o_prefix", right_on="prefix_b"
        ),
        rows.join(_side(remote, "_a"), left_on="s_prefix", right_on="prefix_a").join(
            _side(local, "_b"), left_on="o_key", right_on="key_b"
        ),
    ]
    by_prefix = rows.group_by("s_prefix", "o_prefix", "predicate_id").agg(
        pl.len().alias("rows"), pl.col("confidence").sum().alias("confidence_sum")
    )
    both = by_prefix.join(_side(remote, "_a"), left_on="s_prefix", right_on="prefix_a").join(
        _side(remote, "_b"), left_on="o_prefix", right_on="prefix_b"
    )
    counted = [
        part.group_by(*columns).agg(
            pl.len().alias("rows"), pl.col("confidence").sum().alias("confidence_sum")
        )
        for part in parts
    ]
    counted.append(
        both.group_by(*columns).agg(
            pl.col("rows").sum().cast(pl.UInt32), pl.col("confidence_sum").sum()
        )
    )
    frame = pl.concat(
        [c.select(*columns, pl.col("rows").cast(pl.UInt32), "confidence_sum") for c in counted]
    ).filter(pl.col("ds_a") != pl.col("ds_b"))
    frame = _oriented(frame.with_columns())
    return (
        frame.group_by(*columns).agg(pl.col("rows").sum(), pl.col("confidence_sum").sum()).collect()
    )


# The whole step -----------------------------------------------------------------------------------


def _sources(registry: Path | Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return the class-mapping sources of a registry (its file or its entries)."""
    entries = (
        yaml.safe_load(Path(registry).read_text())
        if isinstance(registry, (str, Path))
        else registry
    )
    return [
        dict(e) for e in entries or [] if e.get("type", "class_mappings") != "property_mappings"
    ]


def instance_inputs(
    extractions: Iterable[tuple[str, Any, MinedSchema | None]],
    release_dir: Path,
    data_dir: Path | None = None,
) -> tuple[dict[str, MinedSchema], dict[str, Path]]:
    """Return one schema per dataset (the local one when there are both) and the local stores.

    *extractions* are (dataset, extraction record, schema), as rdfsolve.analysis.io
    iter_extractions yields them. The store of a local extraction is the one its report names
    (config.scan_store.path), else data_dir/qlever_workdirs/<dataset>/scan-store.
    """
    schemas: dict[str, tuple[bool, MinedSchema]] = {}
    stores: dict[str, Path] = {}
    for dataset, extraction, schema in extractions:
        if schema is None:
            continue
        local = getattr(extraction, "mode", None) == "local"
        if dataset in schemas and (schemas[dataset][0] or not local):
            continue
        schemas[dataset] = (local, schema)
        if not local:
            continue
        candidates = []
        report = getattr(extraction, "report_path", None)
        if report and (release_dir / report).is_file():
            config = json.loads((release_dir / report).read_text()).get("config") or {}
            if (config.get("scan_store") or {}).get("path"):
                candidates.append(Path(config["scan_store"]["path"]))
        if data_dir is not None:
            candidates.append(data_dir / "qlever_workdirs" / dataset / "scan-store")
        found = next((c for c in candidates if (c / "store.json").is_file()), None)
        if found is not None:
            stores[dataset] = found
    return {name: schema for name, (_, schema) in sorted(schemas.items())}, stores


def source_files(entry: Mapping[str, Any], downloads: Path) -> list[Path]:
    """Return the mapping files of a source, downloading them when they are not there yet."""
    from rdfsolve.mappings.enrichment import download_sssom_source

    folder = downloads / entry["name"]
    found = (
        sorted(p for p in folder.rglob("*.sssom.tsv") if not p.name.endswith(".selected.sssom.tsv"))
        if folder.is_dir()
        else []
    )
    return found or download_sssom_source(entry["url"], folder, entry["name"])


def derive_instance_mappings(
    registry: Path | Sequence[Mapping[str, Any]],
    schemas: Mapping[str, MinedSchema],
    stores: Mapping[str, Path],
    output_dir: Path,
    *,
    downloads: Path | None = None,
    settings: InstanceSettings | None = None,
    creator_id: str | None = None,
    creator_label: str | None = None,
) -> dict[str, Any]:
    """Derive instance mappings, linksets and class mappings from the registry's mapping sets.

    *schemas* holds one schema per dataset; *stores* the local scan store of the datasets that
    have one (the others are matched by namespace). The mapping sets are read from *downloads*
    (output_dir/sssom_downloads by default), and downloaded when absent. Outputs go to
    output_dir/mappings: instance/ (one SSSOM set per mapping set), linksets/ (VoID, and the
    shared entities), derived/ (the instance-derived class mappings, and the record links as
    a table). Return a summary per
    mapping set and the totals, which is also written as mappings/instance/summary.json.
    """
    import polars as pl

    settings = settings or InstanceSettings()
    downloads = downloads or output_dir / "sssom_downloads"
    mappings = output_dir / "mappings"
    work = mappings / "instance" / "work"
    work.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    summary: dict[str, Any] = {"sources": {}, "settings": _settings_record(settings)}

    # 1. The rows of every mapping set, with the keys of their ends.
    reader = IdentifierReader()
    sources: list[_Source] = []
    for entry in _sources(registry):
        name = entry["name"]
        record: dict[str, Any] = {"files": 0, "rows": 0, "kept": 0, "keyed": 0, "set_ids": []}
        summary["sources"][name] = record
        try:
            files = source_files(entry, downloads)
        except Exception as error:  # a source that cannot be read leaves the others
            log.warning("%s: not read: %s", name, error)
            record["error"] = str(error)
            continue
        written: list[Path] = []
        for number, path in enumerate(files):
            frame, set_id = mapping_rows(
                path,
                reader,
                fallback_id=str(entry.get("mapping_set_id") or entry["url"]),
                settings=settings,
                set_confidence=entry.get("confidence"),
                ignore_confidence=bool(entry.get("ignore_confidence", False)),
            )
            record["set_ids"].append(set_id)
            if frame is None:
                continue
            target = work / f"{name}.{number:04d}.parquet"
            frame.filter(
                pl.col("s_key").is_not_null() & pl.col("o_key").is_not_null()
            ).sink_parquet(target)
            record["rows"] += _count_rows(path)
            written.append(target)
        record["files"] = len(files)
        record["keyed"] = sum(pl.scan_parquet(p).select(pl.len()).collect().item() for p in written)
        sources.append(_Source(name, entry, written, sorted(set(record["set_ids"]))))
    record_time(summary, "rows_seconds", started)

    # 2. The identifiers that any row names, and their prefixes.
    files = [p for s in sources for p in s.files]
    if files:
        keys = (
            pl.concat(
                [pl.scan_parquet(p).select(pl.col("s_key").alias("key")) for p in files]
                + [pl.scan_parquet(p).select(pl.col("o_key").alias("key")) for p in files]
            )
            .unique()
            .collect(engine="streaming")
        )
    else:
        keys = pl.DataFrame(schema={"key": pl.String})

    # 2b. The identifiers that two local datasets both hold: their records are the same entity
    # whether or not a mapping set says so.
    if settings.same_identifier and len(stores) > 1:
        shared = shared_identifiers(stores, schemas, IdentifierReader())
        record_time(summary, "shared_identifiers_seconds", started)
        summary["sources"][SAME_IDENTIFIER] = {"rows": shared.height, "keyed": shared.height}
        if shared.height:
            from rdfsolve.config import mint

            set_id = mint("mapping-set", "same-identifier")
            target = work / f"{SAME_IDENTIFIER}.parquet"
            same_identifier_rows(shared, set_id).write_parquet(target)
            sources.append(_Source(SAME_IDENTIFIER, {"name": SAME_IDENTIFIER}, [target], [set_id]))
            keys = pl.concat([keys, shared]).unique()
    prefixes = set(keys["key"].str.extract(r"^([^:]+):", 1).unique().drop_nulls().to_list())
    summary["keys"] = keys.height
    keyed_reader = IdentifierReader(prefixes)

    # 3. The instances of the datasets: counted in local stores, by namespace elsewhere.
    occurrences, members, remote, claim_tables = [], [], [], []
    parts_by_dataset: dict[str, ClassParts] = {}
    datasets: dict[str, Any] = {}
    for name, schema in sorted(schemas.items()):
        tic = time.monotonic()
        if name in stores:
            found, counted, stated = store_occurrences(
                name, stores[name], schema, keyed_reader, keys, settings.identity_properties
            )
            members.append(counted)
            claim_tables.append(stated)
            parts_by_dataset[name] = _class_parts(stores[name], found)
            # Nodes of namespaces that their class holds in one part of a host: that part's key.
            from rdfsolve.mining.class_parts import fold_part_keys

            found = fold_part_keys(found, parts_by_dataset[name])
            occurrences.append(found)
            datasets[name] = {
                "membership": "local",
                "instances": found.height,
                "identifiers": found["key"].n_unique(),
                "seconds": round(time.monotonic() - tic, 1),
            }
        else:
            spaces = namespace_occurrences(name, schema, keyed_reader)
            remote.append(spaces)
            datasets[name] = {"membership": "namespace", "identifier_types": spaces.height}
    summary["datasets"] = datasets
    del keys
    occ = pl.concat(occurrences) if occurrences else _empty_occurrences()
    member_table = (
        pl.concat(members)
        if members
        else pl.DataFrame(schema={"ds": pl.String, "cls": pl.String, "members": pl.UInt32})
    )
    namespaces = (
        pl.concat(remote)
        if remote
        else pl.DataFrame(schema=dict.fromkeys(("prefix", *GROUP), pl.String))
    )
    # 3b. The identity claims of the records, checked against the mapping sets: a record counts
    # as the identifier it claims only where a set states that the two are the same.
    claims = check_claims(
        pl.concat(claim_tables) if claim_tables else _empty_claims(),
        [(s.name, p) for s in sources for p in s.files],
    )
    occ = promote_confirmed(occ, claims)
    summary["identity_claims"] = dict(Counter(claims["outcome"].to_list()))
    groups = occ.select("key", *GROUP).unique()
    class_keys = _class_keys(schemas, keyed_reader)
    record_time(summary, "occurrences_seconds", started)

    # 4. Each mapping set joined with the instances, and its instance mappings and linksets.
    from rdfsolve.config import mint

    contributions: list[tuple[_Source, _Joined]] = []
    linksets: list[dict[str, Any]] = []
    for source in sources:
        record = summary["sources"][source.name]
        if not source.files:
            continue
        rows = pl.concat([pl.scan_parquet(p) for p in source.files])
        record["classified"] = _classified(rows, groups, namespaces, class_keys)
        # An identifier held by two local datasets says nothing of a dataset known by its
        # namespaces only.
        joined = join_rows(
            rows, groups, namespaces.clear() if source.name == SAME_IDENTIFIER else namespaces
        )
        record["instance_rows"] = joined.instance_rows.height
        set_id = mint("mapping-set", "instance", source.name)
        # An identifier joined with itself is no mapping to publish; it counts as shared.
        if joined.instance_rows.height and source.name != SAME_IDENTIFIER:
            _write_instance_sssom(
                mappings / "instance" / f"{source.name}.sssom.tsv",
                joined.instance_rows,
                set_id,
                source,
                creator_id,
                creator_label,
            )
        made = _linksets(source, joined, occ)
        record["pairs"] = sum(r["pairs"] or 0 for r in made)
        record["shared_pairs"] = sum(r["shared"] or 0 for r in made)
        record["asserted_rows"] = int(joined.asserted["rows"].sum() or 0)
        linksets += made
        contributions.append((source, joined))
    record_time(summary, "join_seconds", started)

    # 5. The outputs over all sets.
    _write_linksets(mappings / "linksets", linksets, sources)
    derived = _derive(contributions, occ, member_table, settings, parts_by_dataset, claims)
    _write_identity_claims(mappings / "identity-claims", claims, schemas)
    class_mappings = [d for d in derived if d.kind == "class_mapping"]
    links = [d for d in derived if d.kind == "record_link"]
    _write_derived(
        mappings / "derived" / "instance-derived.sssom.tsv",
        class_mappings,
        schemas,
        creator_id,
        creator_label,
    )
    _write_links(mappings / "derived" / "instance-links.tsv", links)
    summary["linksets"] = sum(1 for row in linksets if row["pairs"] is None or row["pairs"])
    summary["shared_entity_rows"] = sum(1 for row in linksets if row["shared"])
    summary["derived"] = len(class_mappings)
    summary["derived_by_predicate"] = dict(Counter(d.predicate for d in class_mappings))
    summary["derived_by_validation"] = dict(Counter(d.validation for d in class_mappings))
    summary["record_links"] = len(links)
    summary["record_links_by_validation"] = dict(Counter(d.validation for d in links))
    record_time(summary, "seconds", started)
    for path in work.glob("*.parquet"):
        path.unlink()
    work.rmdir()
    (mappings / "instance" / "summary.json").write_text(json.dumps(summary, indent=1))
    return summary


def record_time(summary: dict[str, Any], name: str, started: float) -> None:
    """Record the seconds since *started* under *name*, and log them."""
    summary[name] = round(time.monotonic() - started, 1)
    log.info("instance mappings: %s %s", name, summary[name])


def _settings_record(settings: InstanceSettings) -> dict[str, Any]:
    """Return the settings as a record of the summary."""
    return {
        "exact": settings.exact,
        "predicates": sorted(settings.predicates) if settings.predicates else None,
        "confidence_defaults": dict(settings.policy.by_justification),
        "confidence_unspecified": settings.policy.unspecified,
    }


def _count_rows(path: Path) -> int:
    """Count the data rows of a mapping file (lines that are not metadata, minus the header)."""
    count = 0
    with path.open("rb") as stream:
        for line in stream:
            if not line.startswith(b"#"):
                count += 1
    return max(count - 1, 0)


def _empty_occurrences() -> pl.DataFrame:
    """Return an instance table without rows."""
    import polars as pl

    return pl.DataFrame(
        schema={
            "key": pl.String,
            "ds": pl.String,
            "form": pl.String,
            "cls": pl.String,
            "prop": pl.String,
            "sid": pl.UInt64,
            "h": pl.UInt64,
            "part": pl.String,
        }
    )


def _class_keys(schemas: Mapping[str, MinedSchema], reader: IdentifierReader) -> pl.DataFrame:
    """Return the identifier keys of the classes of the schemas (key)."""
    import polars as pl

    classes = sorted({c for schema in schemas.values() for c in schema.get_classes()})
    frame = pl.LazyFrame({"iri": classes}, schema={"iri": pl.String})
    return (
        reader.iri_keys(frame, "iri").filter(pl.col("key").is_not_null()).select("key").unique()
    ).collect()


def _classified(
    rows: pl.LazyFrame, groups: pl.DataFrame, namespaces: pl.DataFrame, classes: pl.DataFrame
) -> dict[str, dict[str, int]]:
    """Count the ends of the rows by what they are in the corpus.

    class: a class of a mined schema; instance: an instance in a local store; namespace: an
    identifier type that a dataset without a store carries; none: neither.
    """
    import polars as pl

    held = groups.lazy().select("key").unique().with_columns(pl.lit("instance").alias("_i"))
    known = classes.lazy().with_columns(pl.lit("class").alias("_c"))
    spaces = (
        namespaces.lazy().select("prefix").unique().with_columns(pl.lit("namespace").alias("_n"))
    )
    counts: dict[str, dict[str, int]] = {}
    for end in ("s", "o"):
        frame = (
            rows.select(pl.col(f"{end}_key").alias("key"))
            .with_columns(pl.col("key").str.extract(r"^([^:]+):", 1).alias("prefix"))
            .join(known, on="key", how="left")
            .join(held, on="key", how="left")
            .join(spaces, on="prefix", how="left")
            .select(pl.coalesce("_c", "_i", "_n", pl.lit("none")).alias("kind"))
            .group_by("kind")
            .len()
            .collect()
        )
        counts["subject" if end == "s" else "object"] = dict(frame.iter_rows())
    return counts


# Outputs ------------------------------------------------------------------------------------------


def _prefix_map_for(curies: Iterable[str]) -> dict[str, str]:
    """Return the URI prefixes of the CURIEs' prefixes (Bioregistry's, else the SSSOM ones)."""
    import bioregistry

    from rdfsolve.config import get_base_uri

    builtin = {
        "skos": "http://www.w3.org/2004/02/skos/core#",
        "semapv": "https://w3id.org/semapv/vocab/",
        "owl": "http://www.w3.org/2002/07/owl#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "oboInOwl": "http://www.geneontology.org/formats/oboInOwl#",
        "rdfsolve": get_base_uri().rstrip("/") + "/",
    }
    found: dict[str, str] = {}
    for prefix in sorted({c.split(":", 1)[0] for c in curies if ":" in c}):
        namespace = builtin.get(prefix) or bioregistry.get_uri_prefix(prefix)
        found[prefix] = namespace or f"https://bioregistry.io/{prefix}:"
    return found


def _dataset_curie(name: str) -> str:
    """Return the CURIE of a dataset's VoID IRI (rdfsolve:dataset/<name>)."""
    from rdfsolve.config import get_base_uri, mint

    return "rdfsolve:" + mint("dataset", name)[len(get_base_uri().rstrip("/") + "/") :]


def _write_instance_sssom(
    path: Path,
    rows: pl.DataFrame,
    set_id: str,
    source: _Source,
    creator_id: str | None,
    creator_label: str | None,
) -> None:
    """Write the instance mappings of one mapping set as an SSSOM table (TSV, metadata first)."""
    from datetime import UTC, datetime

    from rdfsolve.version import VERSION

    path.parent.mkdir(parents=True, exist_ok=True)
    set_ids = sorted(set(rows["mapping_source"].unique().to_list()))
    curies = [
        *rows["s_key"].unique().to_list(),
        *rows["o_key"].unique().to_list(),
        *rows["predicate_id"].unique().to_list(),
        *rows["mapping_justification"].unique().to_list(),
        "rdfsolve:dataset",
    ]
    metadata: dict[str, Any] = {
        "curie_map": _prefix_map_for(curies),
        "mapping_set_id": set_id,
        "mapping_set_title": f"Instance mappings of {source.name} between corpus datasets",
        "mapping_set_source": set_ids,
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "mapping_tool": "rdfsolve",
        "mapping_tool_version": VERSION,
        "mapping_date": str(datetime.now(UTC).date()),
    }
    if source.oxo:
        metadata["see_also"] = [source.oxo]
    if creator_id:
        metadata["creator_id"] = [creator_id]
    if creator_label:
        metadata["creator_label"] = [creator_label]
    columns = [
        "subject_id",
        "predicate_id",
        "object_id",
        "mapping_justification",
        "confidence",
        "subject_source",
        "object_source",
        "mapping_source",
        "other",
    ]
    with path.open("w", encoding="utf-8", newline="") as out:
        for line in yaml.safe_dump(metadata, sort_keys=False, allow_unicode=True).splitlines():
            out.write(f"#{line}\n")
        writer = csv.writer(out, delimiter="\t", lineterminator="\n")
        writer.writerow(columns)
        for row in rows.sort("s_key", "o_key", "subject_dataset", "object_dataset").iter_rows(
            named=True
        ):
            other: dict[str, Any] = {"validation": "validated", "set": source.name}
            if row["stated"] is not None and row["stated"] != row["confidence"]:
                other["source_confidence"] = row["stated"]
            writer.writerow(
                [
                    row["s_key"],
                    row["predicate_id"],
                    row["o_key"],
                    row["mapping_justification"],
                    f"{row['confidence']:g}" if row["confidence"] is not None else "",
                    _dataset_curie(row["subject_dataset"]),
                    _dataset_curie(row["object_dataset"]),
                    row["mapping_source"],
                    json.dumps(other, sort_keys=True),
                ]
            )


def _linksets(source: _Source, joined: _Joined, occ: pl.DataFrame) -> list[dict[str, Any]]:
    """Return the linksets of one mapping set per (ds_a, ds_b, predicate): the record pairs that
    bridge the datasets (pairs) and those that share an entity (shared); those with an end
    known by namespace only without counts (asserted).
    """
    import polars as pl

    found: list[dict[str, Any]] = []
    local = joined.local
    if local.height:
        combos = local.select("key_a", "key_b", "ds_a", "ds_b", "predicate_id").unique()
        counted = pair_counts(combos.drop("predicate_id").unique(), occ, ("ds",))
        pairs = (
            combos.join(counted, on=["key_a", "key_b", "ds_a", "ds_b"])
            .group_by("ds_a", "ds_b", "predicate_id")
            .agg(
                (pl.col("pairs") - pl.col("shared")).sum().alias("pairs"),
                pl.col("shared").sum(),
                pl.len().alias("identifier_pairs"),
            )
        )
        found += [
            {**row, "set": source.name, "validation": "validated"}
            for row in pairs.iter_rows(named=True)
        ]
    if joined.asserted.height:
        asserted = joined.asserted.group_by("ds_a", "ds_b", "predicate_id").agg(
            pl.col("rows").sum()
        )
        found += [
            {
                **row,
                "set": source.name,
                "validation": "asserted",
                "shared": None,
                "pairs": None,
                "identifier_pairs": None,
            }
            for row in asserted.iter_rows(named=True)
        ]
    return found


def _write_linksets(folder: Path, linksets: list[dict[str, Any]], sources: list[_Source]) -> None:
    """Write the linksets as VoID (instance-linksets.ttl) and the shared entities as a table."""
    import hashlib

    from rdflib import RDF, Graph, Literal, Namespace, URIRef
    from rdflib.namespace import DCTERMS, RDFS, XSD

    from rdfsolve.config import mint

    folder.mkdir(parents=True, exist_ok=True)
    void = Namespace("http://rdfs.org/ns/void#")
    graph = Graph()
    graph.bind("void", void)
    graph.bind("dcterms", DCTERMS)
    by_name = {s.name: s for s in sources}
    expand = _prefix_map_for([row["predicate_id"] for row in linksets])
    shared_rows = [row for row in linksets if row["shared"]]
    for row in sorted(linksets, key=lambda r: (r["ds_a"], r["ds_b"], r["predicate_id"], r["set"])):
        if row["pairs"] == 0:  # every pair shares an entity: a direct link, not a linkset
            continue
        key = "|".join(("instance", row["predicate_id"], row["set"]))
        node = URIRef(
            mint("linkset", row["ds_a"], row["ds_b"], hashlib.sha256(key.encode()).hexdigest()[:16])
        )
        prefix, _, local = row["predicate_id"].partition(":")
        predicate = (
            expand[prefix] + local
            if prefix in expand and not local.startswith("//")
            else row["predicate_id"]
        )
        graph.add((node, RDF.type, void.Linkset))
        graph.add((node, void.subjectsTarget, URIRef(mint("dataset", row["ds_a"]))))
        graph.add((node, void.objectsTarget, URIRef(mint("dataset", row["ds_b"]))))
        graph.add((node, void.linkPredicate, URIRef(predicate)))
        source = by_name[row["set"]]
        graph.add((node, DCTERMS.source, URIRef(mint("mapping-set", "instance", source.name))))
        for set_id in source.set_ids:
            graph.add((node, DCTERMS.source, URIRef(set_id)))
        if source.oxo:
            graph.add((node, RDFS.seeAlso, URIRef(source.oxo)))
        graph.add((node, DCTERMS.description, Literal(f"{row['validation']}: {row['set']}")))
        if row.get("pairs") is not None:
            graph.add((node, void.triples, Literal(int(row["pairs"]), datatype=XSD.integer)))
    graph.serialize(folder / "instance-linksets.ttl", format="turtle")
    with (folder / "shared-entities.tsv").open("w", newline="") as out:
        writer = csv.writer(out, delimiter="\t", lineterminator="\n")
        writer.writerow(
            ["dataset_a", "dataset_b", "predicate_id", "set", "shared_pairs", "bridge_pairs"]
        )
        for row in sorted(shared_rows, key=lambda r: (r["ds_a"], r["ds_b"], r["set"])):
            writer.writerow(
                [
                    row["ds_a"],
                    row["ds_b"],
                    row["predicate_id"],
                    row["set"],
                    row["shared"],
                    row["pairs"],
                ]
            )


def _meta(column: str) -> pl.Expr:
    """Return whether a class column holds a class of the modelling languages."""
    import polars as pl

    found = pl.lit(value=False)
    for namespace in META_NAMESPACES:
        found = found | pl.col(column).str.starts_with(namespace)
    return found


def _as_records(frame: pl.LazyFrame, settings: InstanceSettings, suffix: str = "") -> pl.LazyFrame:
    """Write an end where the record itself takes part (subject form: named by the identifier,
    or by an identity claim the check bears out, see promote_confirmed) as the form RECORD.
    """
    import polars as pl

    form = f"form{suffix}"
    return frame.with_columns(
        pl.when(pl.col(form) == "subject").then(pl.lit(RECORD)).otherwise(pl.col(form)).alias(form)
    )


def _derived_row(
    row: Mapping[str, Any],
    settings: InstanceSettings,
    validation: str | None,
    details: dict[str, Any],
) -> DerivedClassMapping:
    """Return the derived relation of one group pair (*row*: its ends, counts and evidence)."""
    mapping = row["form_a"] == RECORD and row["form_b"] == RECORD
    coverage_a = row["taking_a"] / row["members_a"] if row.get("members_a") else None
    coverage_b = row["taking_b"] / row["members_b"] if row.get("members_b") else None
    predicate = derived_predicate(coverage_a, coverage_b, settings.exact) if mapping else None
    supported = support(
        predicate,
        lower_share(row.get("taking_a"), row.get("members_a")),
        lower_share(row.get("taking_b"), row.get("members_b")),
    )
    if validation is None:
        validation = (
            "validated"
            if supported is not None and supported >= settings.validated
            else "candidate"
        )
    evidence = row["confidence"]
    confidence = (
        evidence * supported if evidence is not None and supported is not None else evidence
    )
    similarity = None
    if row.get("taking_a") is not None and row.get("members_a") and row.get("members_b"):
        m = min(row["taking_a"], row["taking_b"])
        similarity = m / (row["members_a"] + row["members_b"] - m)
    return DerivedClassMapping(
        source_dataset=row["ds_a"],
        source_class=row["cls_a"],
        target_dataset=row["ds_b"],
        target_class=row["cls_b"],
        predicate=predicate if mapping else None,
        validation=validation,
        confidence=confidence,
        source_property=row["prop_a"],
        target_property=row["prop_b"],
        pairs=row.get("pairs"),
        source_coverage=coverage_a,
        target_coverage=coverage_b,
        similarity=similarity,
        mapping_sets=tuple(row["sets"]),
        kind="class_mapping" if mapping else "record_link",
        details={
            "forms": [row["form_a"], row["form_b"]],
            "evidence": evidence,
            "support": supported,
            **details,
        },
    )


def _parts_of(parts: Mapping[str, ClassParts], ds: str, cls: str) -> list[Any]:
    """Return the counted parts of a class of a dataset (none when not counted)."""
    found = parts.get(ds)
    return list(found.by_class.get(cls, [])) if found is not None else []


def _part_of(parts: Mapping[str, ClassParts], ds: str, cls: str, key: str | None) -> Any:
    """Return the counted part of a class with this key, if any."""
    return next((p for p in _parts_of(parts, ds, cls) if p.key() == key), None)


def _part_counts(counts: Any, column: str) -> dict[str, int]:
    """Return the members taking part per part key, from a value_counts list."""
    return {item[column]: int(item["count"]) for item in counts or [] if item[column] is not None}


def _parts_profile(parts: list[Any], taking: dict[str, int], members: int | None) -> dict[str, Any]:
    """Describe how a class's members are identified and where the members taking part are.

    effective_parts: exp of the Shannon entropy of the parts' shares (how many equal parts the
    class behaves like); eligible: members of the parts that take part; within_eligible: the
    share of those that take part.
    """
    from math import exp, log

    total = sum(p.entities for p in parts) or (members or 0)
    shares = [p.entities / total for p in parts if total and p.entities]
    eligible = sum(p.entities for p in parts if p.key() in taking)
    taking_total = sum(taking.values())
    return {
        "parts": len(parts),
        "effective_parts": round(exp(-sum(x * log(x) for x in shares)), 2) if shares else None,
        "largest_part_share": round(max(shares), 4) if shares else None,
        "taking_part_by_part": dict(sorted(taking.items(), key=lambda kv: -kv[1])[:10]),
        "eligible": eligible,
        "eligible_share": round(eligible / total, 4) if total else None,
        "within_eligible": round(taking_total / eligible, 4) if eligible else None,
    }


def _claims_profile(claims: pl.DataFrame | None, row: Mapping[str, Any]) -> dict[str, Any]:
    """Count the identity claims of the classes of a derived row by outcome (each end)."""
    import polars as pl

    if claims is None or not claims.height:
        return {}
    found: dict[str, Any] = {}
    for e, name in (("a", "source"), ("b", "target")):
        mine = claims.filter((pl.col("ds") == row[f"ds_{e}"]) & (pl.col("cls") == row[f"cls_{e}"]))
        if mine.height:
            found[name] = dict(Counter(mine["outcome"].to_list()))
    return found


def _derive(
    contributions: list[tuple[_Source, _Joined]],
    occ: pl.DataFrame,
    members: pl.DataFrame,
    settings: InstanceSettings,
    parts_by_dataset: Mapping[str, ClassParts] | None = None,
    claims: pl.DataFrame | None = None,
) -> list[DerivedClassMapping]:
    """Return the class mappings and record links that the instance pairs of all mapping sets
    support.

    An end where the record itself takes part is one group per class, whatever property or
    form named it; the classes of the modelling languages are left out. Record pairs and the
    members taking part are counted once per group pair, over all mapping sets. The confidence
    is the mean confidence of the supporting rows times the support of the relation (the lower
    bound of the shares it claims).
    """
    import polars as pl

    parts_by_dataset = parts_by_dataset or {}
    ends = [*[f"{c}_a" for c in GROUP], *[f"{c}_b" for c in GROUP]]
    local_parts = [
        joined.local.with_columns(pl.lit(source.name).alias("set"))
        for source, joined in contributions
        if joined.local.height
    ]
    derived: list[DerivedClassMapping] = []
    if local_parts:
        local = _as_records(
            _as_records(
                pl.concat(local_parts, how="vertical_relaxed")
                .lazy()
                .filter(~_meta("cls_a") & ~_meta("cls_b")),
                settings,
                "_a",
            ),
            settings,
            "_b",
        ).collect()
        held = _as_records(occ.lazy(), settings).select("key", *GROUP, "sid", "part").unique()
        side = {
            end: held.rename({c: f"{c}_{end}" for c in ("key", *GROUP, "sid", "part")})
            for end in ("a", "b")
        }
        # One row per record pair of a group pair, with the part of each record.
        record_pairs = (
            local.lazy()
            .select(*ends, "key_a", "key_b", "set")
            .unique()
            .join(side["a"], on=["key_a", *[f"{c}_a" for c in GROUP]], nulls_equal=True)
            .join(side["b"], on=["key_b", *[f"{c}_b" for c in GROUP]], nulls_equal=True)
            .group_by(*ends, "sid_a", "sid_b")
            .agg(
                (pl.col("set") == SAME_IDENTIFIER).any().alias("same"),
                pl.col("part_a").first(),
                pl.col("part_b").first(),
            )
            .collect()
        )
        evidence = (
            local.lazy()
            .unique(subset=["set", "row", *ends])
            .group_by(*ends)
            .agg(
                pl.col("confidence").mean().alias("confidence"),
                pl.col("set").unique().sort().alias("sets"),
                pl.col("predicate_id").unique().sort().alias("predicates"),
                pl.len().alias("rows"),
            )
            .collect()
        )
        member_a = members.rename({"ds": "ds_a", "cls": "cls_a", "members": "members_a"})
        member_b = members.rename({"ds": "ds_b", "cls": "cls_b", "members": "members_b"})
        for grouping in ("class", "part"):
            keys = ends if grouping == "class" else [*ends, "part_a", "part_b"]
            pairs = record_pairs
            if grouping == "part":
                pairs = pairs.filter(
                    (pl.col("form_a") == RECORD)
                    & (pl.col("form_b") == RECORD)
                    & pl.col("part_a").is_not_null()
                    & pl.col("part_b").is_not_null()
                )
            counted = (
                pairs.group_by(*keys)
                .agg(
                    pl.len().alias("pairs"),
                    pl.col("same").sum().alias("same_identifier_pairs"),
                    pl.col("sid_a").n_unique().alias("taking_a"),
                    pl.col("sid_b").n_unique().alias("taking_b"),
                    *[
                        pl.struct(pl.col(f"part_{e}"), pl.col(f"sid_{e}"))
                        .unique()
                        .struct.field(f"part_{e}")
                        .value_counts()
                        .alias(f"by_part_{e}")
                        for e in ("a", "b")
                    ],
                )
                .join(evidence, on=ends, nulls_equal=True)
                .join(member_a, on=["ds_a", "cls_a"], how="left")
                .join(member_b, on=["ds_b", "cls_b"], how="left")
            )
            for row in counted.iter_rows(named=True):
                taking = {e: _part_counts(row[f"by_part_{e}"], f"part_{e}") for e in ("a", "b")}
                if grouping == "part":
                    found = [
                        _part_of(
                            parts_by_dataset, row[f"ds_{e}"], row[f"cls_{e}"], row[f"part_{e}"]
                        )
                        for e in ("a", "b")
                    ]
                    # A class of one part is its part: the class row says it all.
                    if not all(found) or all(
                        len(_parts_of(parts_by_dataset, row[f"ds_{e}"], row[f"cls_{e}"])) < 2
                        for e in ("a", "b")
                    ):
                        continue
                    row = {**row, "members_a": found[0].entities, "members_b": found[1].entities}
                details = {
                    "source_members": row["members_a"],
                    "target_members": row["members_b"],
                    "source_taking_part": row["taking_a"],
                    "target_taking_part": row["taking_b"],
                    "same_identifier_pairs": int(row["same_identifier_pairs"] or 0),
                    "supporting_rows": row["rows"],
                    "mapping_predicates": row["predicates"],
                }
                if grouping == "class":
                    for e, name in (("a", "source"), ("b", "target")):
                        details[f"{name}_parts"] = _parts_profile(
                            _parts_of(parts_by_dataset, row[f"ds_{e}"], row[f"cls_{e}"]),
                            taking[e],
                            row[f"members_{e}"],
                        )
                    details["claims"] = _claims_profile(claims, row)
                mapping = _derived_row(row, settings, None, details)
                if grouping == "part":
                    # A class of one part is its own part: that end stays the class.
                    single = {
                        e: len(_parts_of(parts_by_dataset, row[f"ds_{e}"], row[f"cls_{e}"])) < 2
                        for e in ("a", "b")
                    }
                    mapping = replace(
                        mapping,
                        source_part=None if single["a"] else row["part_a"],
                        target_part=None if single["b"] else row["part_b"],
                    )
                derived.append(mapping)
    asserted_parts = [
        joined.asserted.with_columns(pl.lit(source.name).alias("set"))
        for source, joined in contributions
        if joined.asserted.height
    ]
    if asserted_parts:
        asserted = (
            _as_records(
                _as_records(
                    pl.concat(asserted_parts).lazy().filter(~_meta("cls_a") & ~_meta("cls_b")),
                    settings,
                    "_a",
                ),
                settings,
                "_b",
            )
            .group_by(*ends)
            .agg(
                pl.col("rows").sum(),
                pl.col("confidence_sum").sum(),
                pl.col("set").unique().sort().alias("sets"),
            )
            .collect()
        )
        for row in asserted.iter_rows(named=True):
            row["confidence"] = row["confidence_sum"] / row["rows"] if row["rows"] else None
            derived.append(
                _derived_row(row, settings, "asserted", {"supporting_rows": row["rows"]})
            )
    return sorted(
        derived,
        key=lambda d: (
            d.kind,
            d.source_dataset,
            d.target_dataset,
            d.source_class,
            d.target_class,
            d.source_property or "",
            d.target_property or "",
            d.validation,
        ),
    )


def _end_iri(d: DerivedClassMapping, end: str) -> str:
    """Return the IRI of one end of a derived mapping: its class, or the node shape of its part
    (rdfsolve.schema_models.class_parts.part_shape_iri), which the dataset's SHACL defines.
    """
    from rdfsolve.schema_models.class_parts import part_shape_iri_of_key
    from rdfsolve.schema_models.exporters.shacl import shape_iri

    cls = d.source_class if end == "source" else d.target_class
    part = d.source_part if end == "source" else d.target_part
    if part is None:
        return cls
    dataset = d.source_dataset if end == "source" else d.target_dataset
    return part_shape_iri_of_key(shape_iri(dataset, cls), part)


def explain(d: DerivedClassMapping) -> tuple[list[str], str | None]:
    """Return the flags of a derived mapping and a sentence that explains its numbers.

    concentrated_<end>: the members taking part sit in parts of the class that hold less than
    half of it (the class mixes ways of identifying its members); scope_<end>: even within
    those parts few members take part; unconfirmed_claims_<end>: records of the class state
    identities that no mapping set bears out, and those records do not count as themselves.
    """
    flags: list[str] = []
    said: list[str] = []
    for end in ("source", "target"):
        profile = d.details.get(f"{end}_parts") or {}
        cover = d.source_coverage if end == "source" else d.target_coverage
        eligible, within = profile.get("eligible_share"), profile.get("within_eligible")
        if eligible is not None and within is not None and cover is not None:
            if eligible < 0.5:
                flags.append(f"concentrated_{end}")
                said.append(
                    f"the members of the {end} class are identified in {profile['parts']} ways "
                    f"(about {profile['effective_parts']} parts of equal size); the matched members "
                    f"are found in parts that hold {eligible:.0%} of the class, and {within:.0%} "
                    "of the members of those parts are matched"
                )
            if within < 0.5:
                flags.append(f"scope_{end}")
        outcomes = (d.details.get("claims") or {}).get(end) or {}
        doubtful = {
            k: outcomes[k] for k in ("unconfirmed", "weaker", "conflicting") if outcomes.get(k)
        }
        if doubtful:
            flags.append(f"unconfirmed_claims_{end}")
            said.append(
                f"of the {sum(outcomes.values())} identity claims stated by {end} records, "
                + ", ".join(f"{v} are {k.replace('_', ' ')}" for k, v in sorted(outcomes.items()))
                + f"; the {sum(doubtful.values())} claims that are not borne out by a mapping set "
                "are not counted as matches of the records"
            )
    if d.source_coverage is not None and d.target_coverage is not None:
        said.insert(
            0,
            f"{d.source_coverage:.0%} of the source members and {d.target_coverage:.0%} of the "
            "target members are matched",
        )
    return flags, ("; ".join(said) + ".") if said else None


def _write_derived(
    path: Path,
    derived: list[DerivedClassMapping],
    schemas: Mapping[str, MinedSchema],
    creator_id: str | None,
    creator_label: str | None,
) -> None:
    """Write the derived class mappings as an SSSOM mapping set."""
    from curies import Converter
    from sssom import Mapping as SSSOMMapping

    from rdfsolve._uri import prefix_map
    from rdfsolve.config import get_base_uri, mint
    from rdfsolve.mappings.sssom import create_sssom_mappings, write_sssom_tsv

    path.parent.mkdir(parents=True, exist_ok=True)
    retained: dict[str, str] = {"rdfsolve": get_base_uri()}
    for schema in schemas.values():
        for prefix, namespace in schema.get_prefixes().items():
            if prefix not in retained and namespace not in retained.values():
                retained[prefix] = namespace
    iris = [mint("dataset", name) for name in schemas]
    for d in derived:
        iris += [
            _end_iri(d, "source"),
            _end_iri(d, "target"),
            d.source_class,
            d.target_class,
            d.source_property or "",
            d.target_property or "",
        ]
    converter = Converter.from_prefix_map(prefix_map([i for i in iris if i], retained))

    def curie(iri: str | None) -> str | None:
        """Return the CURIE of an IRI."""
        return converter.compress(iri, strict=True) if iri else None

    rows = []
    for d in derived:
        details = d.details
        flags, comment = explain(d)
        other = {
            "validation": d.validation,
            "mapping_sets": list(d.mapping_sets),
            "source_coverage": d.source_coverage,
            "target_coverage": d.target_coverage,
            "pairs": d.pairs,
            "flags": flags,
            **{k: v for k, v in details.items() if k != "forms"},
        }
        if d.source_part or d.target_part:
            other.update(
                source_class=curie(d.source_class),
                target_class=curie(d.target_class),
                source_part=d.source_part,
                target_part=d.target_part,
            )
        rows.append(
            SSSOMMapping(
                subject_id=curie(_end_iri(d, "source")),
                subject_type="rdfs resource" if d.source_part else "owl class",
                comment=comment,
                subject_source=curie(mint("dataset", d.source_dataset)),
                subject_match_field=[curie(d.source_property)] if d.source_property else None,
                predicate_id=d.predicate,
                object_id=curie(_end_iri(d, "target")),
                object_type="rdfs resource" if d.target_part else "owl class",
                object_source=curie(mint("dataset", d.target_dataset)),
                object_match_field=[curie(d.target_property)] if d.target_property else None,
                mapping_justification=JUSTIFICATION,
                confidence=round(d.confidence, 4) if d.confidence is not None else None,
                similarity_score=round(d.similarity, 6) if d.similarity is not None else None,
                similarity_measure=SIMILARITY if d.similarity is not None else None,
                see_also=list(d.mapping_sets),
                other=json.dumps(other, sort_keys=True),
            )
        )
    metadata: dict[str, str] = {}
    if creator_id:
        metadata["creator_id"] = creator_id
    if creator_label:
        metadata["creator_label"] = creator_label
    msdf = create_sssom_mappings(
        rows, mint("mapping-set", "instance-derived"), converter=converter, **metadata
    )
    write_sssom_tsv(msdf, path)


def read_derived_mappings(path: str | Path) -> list[DerivedClassMapping]:
    """Read instance-derived class mappings written by derive_instance_mappings."""
    from sssom.parsers import parse_sssom_table

    from rdfsolve.config import mint

    table = parse_sssom_table(Path(path))
    expand = table.converter.expand
    prefix = mint("dataset", "_")[:-1]

    def dataset(curie: str) -> str:
        """Return the dataset name of its VoID IRI."""
        from urllib.parse import unquote

        return unquote((expand(curie) or curie)[len(prefix) :])

    def first(value: Any) -> str | None:
        """Return the IRI of the first match field, if any."""
        if value is None or (isinstance(value, float)) or not len(value):
            return None
        item = value[0] if isinstance(value, list) else str(value).split("|")[0]
        return expand(item) or item

    found = []
    for row in table.df.to_dict("records"):
        other = json.loads(row.get("other") or "{}")
        confidence = row.get("confidence")
        similarity = row.get("similarity_score")
        found.append(
            DerivedClassMapping(
                source_dataset=dataset(row["subject_source"]),
                source_class=expand(row["subject_id"]) or row["subject_id"],
                target_dataset=dataset(row["object_source"]),
                target_class=expand(row["object_id"]) or row["object_id"],
                predicate=row["predicate_id"],
                validation=other.get("validation", "asserted"),
                confidence=None if confidence is None or isnan(confidence) else confidence,
                source_property=first(row.get("subject_match_field")),
                target_property=first(row.get("object_match_field")),
                pairs=other.get("pairs"),
                source_coverage=other.get("source_coverage"),
                target_coverage=other.get("target_coverage"),
                similarity=None if similarity is None or isnan(similarity) else similarity,
                mapping_sets=tuple(other.get("mapping_sets") or ()),
            )
        )
    return found


LINK_COLUMNS = (
    "source_dataset",
    "source_class",
    "source_property",
    "target_dataset",
    "target_class",
    "target_property",
    "validation",
    "confidence",
    "pairs",
    "source_coverage",
    "target_coverage",
    "similarity",
    "mapping_sets",
    "other",
)


def _write_links(path: Path, links: list[DerivedClassMapping]) -> None:
    """Write the record links as a table (TSV): one row per pair of (class, property) ends, an
    empty property where the record itself takes part.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out, delimiter="\t", lineterminator="\n")
        writer.writerow(LINK_COLUMNS)
        for d in links:
            other = {k: v for k, v in d.details.items() if k != "forms"}
            writer.writerow(
                [
                    d.source_dataset,
                    d.source_class,
                    d.source_property or "",
                    d.target_dataset,
                    d.target_class,
                    d.target_property or "",
                    d.validation,
                    "" if d.confidence is None else f"{d.confidence:.4g}",
                    "" if d.pairs is None else d.pairs,
                    "" if d.source_coverage is None else f"{d.source_coverage:.4g}",
                    "" if d.target_coverage is None else f"{d.target_coverage:.4g}",
                    "" if d.similarity is None else f"{d.similarity:.4g}",
                    "|".join(d.mapping_sets),
                    json.dumps(other, sort_keys=True),
                ]
            )


def read_instance_links(path: str | Path) -> list[DerivedClassMapping]:
    """Read the record links written by derive_instance_mappings."""

    def number(value: str, kind: type) -> Any:
        """Return a number of the table, None where it is empty."""
        return kind(value) if value else None

    found = []
    with Path(path).open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream, delimiter="\t"):
            found.append(
                DerivedClassMapping(
                    source_dataset=row["source_dataset"],
                    source_class=row["source_class"],
                    target_dataset=row["target_dataset"],
                    target_class=row["target_class"],
                    predicate=None,
                    validation=row["validation"],
                    confidence=number(row["confidence"], float),
                    source_property=row["source_property"] or None,
                    target_property=row["target_property"] or None,
                    pairs=number(row["pairs"], int),
                    source_coverage=number(row["source_coverage"], float),
                    target_coverage=number(row["target_coverage"], float),
                    similarity=number(row["similarity"], float),
                    mapping_sets=tuple(filter(None, row["mapping_sets"].split("|"))),
                    kind="record_link",
                    details=json.loads(row["other"] or "{}"),
                )
            )
    return found


# The outcomes of the identity check, as written in the outputs (rdfs:label, skos:definition).
CHECK_OUTCOMES = {
    "confirmed": "The two identifiers are stated by a mapping set to name the same entity.",
    "weaker": "The two identifiers are related by a mapping set, but not as the same entity.",
    "unconfirmed": "The two identifiers are not related by any mapping set.",
    "conflicting": "A second identifier of the record's own prefix is claimed.",
    "self": "The record's own identifier is claimed.",
    "record_without_registered_identifier": "The record is not named by a registered "
    "identifier, so it is identified by the claimed identifier only.",
}
_PROPERTY_PREFIXES = {
    "http://www.w3.org/2002/07/owl#": "owl",
    "http://www.w3.org/2004/02/skos/core#": "skos",
    "http://purl.org/dc/elements/1.1/": "dc",
    "http://purl.org/dc/terms/": "dcterms",
    "http://schema.org/": "schema",
    "https://schema.org/": "schemaorg",
}


def _property_curie(iri: str) -> str:
    """Return the CURIE of an identity property (the IRI when its namespace is not known)."""
    for namespace, prefix in _PROPERTY_PREFIXES.items():
        if iri.startswith(namespace):
            return f"{prefix}:{iri[len(namespace) :]}"
    return iri


def _write_identity_claims(
    folder: Path, claims: pl.DataFrame, schemas: Mapping[str, MinedSchema]
) -> None:
    """Write the identity claims of the local datasets and how the check found them.

    Per dataset, an SSSOM set (<dataset>.sssom.tsv) with one row per claim between two
    registered identifiers (the record's own and the claimed one; claims of a record's own
    identifier are left out), the outcome in curation_rule_text and other; and one VoID file
    (identity-claims.ttl) with a linkset per (class part, identity property, prefix of the
    claimed identifiers), its triples, and a subset per outcome typed with the outcome.
    """
    import hashlib
    from datetime import UTC, datetime

    import polars as pl
    from rdflib import RDF, RDFS, Graph, Literal, Namespace, URIRef
    from rdflib.namespace import DCTERMS, SKOS, XSD

    from rdfsolve.config import mint
    from rdfsolve.version import VERSION

    folder.mkdir(parents=True, exist_ok=True)
    if not claims.height:
        return
    rule = mint("rule", "identity-check")
    prefix = r"^([^:]+):"
    for dataset in sorted(claims["ds"].unique().to_list()):
        rows = (
            claims.filter(
                (pl.col("ds") == dataset)
                & pl.col("own_key").is_not_null()
                & (pl.col("outcome") != "self")
            )
            .group_by("own_key", "prop", "value_key", "outcome")
            .agg(
                pl.col("cls").unique().sort().alias("classes"),
                pl.col("sets").first(),
                pl.col("predicates").first(),
            )
            .sort("own_key", "prop", "value_key")
        )
        curies = [*rows["own_key"].to_list(), *rows["value_key"].to_list(), "rdfsolve:dataset"]
        metadata = {
            "curie_map": {
                **_prefix_map_for(curies),
                **{p: n for n, p in _PROPERTY_PREFIXES.items()},
            },
            "mapping_set_id": mint("mapping-set", "identity-claims", dataset),
            "mapping_set_title": f"Identity claims of {dataset}, checked against mapping sets",
            "license": "https://creativecommons.org/publicdomain/zero/1.0/",
            "mapping_tool": "rdfsolve",
            "mapping_tool_version": VERSION,
            "mapping_date": str(datetime.now(UTC).date()),
        }
        columns = [
            "subject_id",
            "predicate_id",
            "object_id",
            "mapping_justification",
            "mapping_source",
            "curation_rule",
            "curation_rule_text",
            "see_also",
            "other",
        ]
        with (folder / f"{dataset}.sssom.tsv").open("w", encoding="utf-8", newline="") as out:
            for line in yaml.safe_dump(metadata, sort_keys=False).splitlines():
                out.write(f"#{line}\n")
            writer = csv.writer(out, delimiter="\t", lineterminator="\n")
            writer.writerow(columns)
            for row in rows.iter_rows(named=True):
                sets = row["sets"] or []
                text = CHECK_OUTCOMES[row["outcome"]]
                if row["predicates"]:
                    text += f" ({', '.join(row['predicates'])} in {', '.join(sets)})"
                writer.writerow(
                    [
                        row["own_key"],
                        _property_curie(row["prop"]),
                        row["value_key"],
                        "semapv:UnspecifiedMatching",
                        _dataset_curie(dataset),
                        rule,
                        text,
                        "|".join(sets),
                        json.dumps(
                            {"check": row["outcome"], "classes": row["classes"]}, sort_keys=True
                        ),
                    ]
                )

    void = Namespace("http://rdfs.org/ns/void#")
    graph = Graph()
    graph.bind("void", void)
    graph.bind("dcterms", DCTERMS)
    graph.bind("skos", SKOS)
    scheme = URIRef(mint("identity-check"))
    graph.add((scheme, RDF.type, SKOS.ConceptScheme))
    graph.add((scheme, RDFS.label, Literal("Outcomes of checking identity claims")))
    for outcome, definition in CHECK_OUTCOMES.items():
        concept = URIRef(mint("identity-check", outcome))
        graph.add((concept, RDF.type, SKOS.Concept))
        graph.add((concept, SKOS.inScheme, scheme))
        graph.add((concept, SKOS.prefLabel, Literal(outcome.replace("_", " "))))
        graph.add((concept, SKOS.definition, Literal(definition)))
    counted = (
        claims.with_columns(pl.col("value_key").str.extract(prefix, 1).alias("value_prefix"))
        .group_by("ds", "cls", "part", "prop", "value_prefix", "outcome")
        .agg(pl.struct("sid", "value_key").n_unique().alias("triples"))
    )
    linksets = counted.group_by("ds", "cls", "part", "prop", "value_prefix").agg(
        pl.col("triples").sum(), pl.struct("outcome", "triples").alias("outcomes")
    )
    for row in linksets.sort("ds", "cls", "part", "prop", "value_prefix").iter_rows(named=True):
        key = "|".join(str(row[k]) for k in ("cls", "part", "prop", "value_prefix"))
        node = URIRef(
            mint(
                "linkset",
                row["ds"],
                "identity-claims",
                hashlib.sha256(key.encode()).hexdigest()[:16],
            )
        )
        target = URIRef(mint("namespace", row["value_prefix"] or "unknown"))
        graph.add((target, RDF.type, void.Dataset))
        graph.add((target, RDFS.label, Literal(f"identifiers of {row['value_prefix']}")))
        if row["value_prefix"]:
            graph.add(
                (target, RDFS.seeAlso, URIRef(f"https://bioregistry.io/{row['value_prefix']}"))
            )
        graph.add((node, RDF.type, void.Linkset))
        graph.add(
            (
                node,
                void.subjectsTarget,
                URIRef(_part_partition_iri(row["ds"], row["cls"], row["part"])),
            )
        )
        graph.add((node, void.objectsTarget, target))
        graph.add((node, void.linkPredicate, URIRef(row["prop"])))
        graph.add((node, void.triples, Literal(int(row["triples"]), datatype=XSD.integer)))
        for item in row["outcomes"]:
            subset = URIRef(f"{node}-{item['outcome']}")
            graph.add((node, void.subset, subset))
            graph.add((subset, RDF.type, void.Linkset))
            graph.add((subset, DCTERMS.type, URIRef(mint("identity-check", item["outcome"]))))
            graph.add((subset, void.triples, Literal(int(item["triples"]), datatype=XSD.integer)))
    graph.serialize(folder / "identity-claims.ttl", format="turtle")


def _part_partition_iri(dataset: str, cls: str, part: str | None) -> str:
    """Return the IRI of the VoID subset of a class part (as the VoID export writes it), or of
    the class partition when the part is not known.
    """
    from hashlib import md5

    from rdfsolve.config import mint

    partition = (
        mint("dataset", dataset)
        + "/partition/class-"
        + md5(cls.encode(), usedforsecurity=False).hexdigest()[:8]
    )
    if part is None:
        return partition
    return f"{partition}-part-{md5(part.encode(), usedforsecurity=False).hexdigest()[:8]}"
