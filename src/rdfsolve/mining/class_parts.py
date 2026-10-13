"""Count the parts of each class of a scan store by how its members are identified.

Each member (a typed subject) falls in one part of its class (rdfsolve.schema_models.
class_parts): a valid identifier of a registered prefix, an IRI in a registered namespace that
is not a valid identifier there, an IRI of another namespace (its scheme, host and first path
segment), an IRI without a scheme (relative), an IRI whose scheme is not valid (by that
text: a blank-node label written as an IRI is one), or a blank node. The store's membership rows are read once, the parts are counted
per class with the streaming engine, and only the counts are held.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from rdfsolve.schema_models.class_parts import ClassPart, ClassParts

if TYPE_CHECKING:
    import polars as pl

__all__ = ["class_parts", "classify", "part_key"]

# A valid IRI scheme and its colon (RFC 3987): a letter, then letters, digits, "+", "." or "-".
_SCHEME = r"^[A-Za-z][A-Za-z0-9+.\-]*:"
# The start of an IRI of no registered namespace: scheme, host and, when a separator follows
# it, the first path segment.
_OTHER = r"^([A-Za-z][A-Za-z0-9+.-]*:(?://[^/#?]*/?)?(?:[^/#?]*[/#])?)"


def class_parts(members: pl.LazyFrame, classes: Iterable[str] | None = None) -> ClassParts:
    """Return the parts of each class from the membership rows *members* (s, c: terms as in
    SPARQL TSV), for *classes* only when given.
    """
    import polars as pl

    from rdfsolve.mappings.keys import IdentifierReader

    reader = IdentifierReader()
    rows = members.select("s", "c").filter(pl.col("c").str.starts_with("<"))
    if classes is not None:
        rows = rows.filter(pl.col("c").is_in([f"<{c}>" for c in classes]))
    rows = rows.unique().with_columns(pl.col("c").str.strip_prefix("<").str.strip_suffix(">"))
    counted = (
        classify(rows, "s", reader)
        .group_by("c", "kind", "prefix", "ns")
        .agg(pl.len().alias("n"))
        .collect(engine="streaming")
    )
    return ClassParts(by_class=_parts(counted.iter_rows(named=True), reader))


def classify(frame: pl.LazyFrame, term: str, reader: Any) -> pl.LazyFrame:
    """Add the part a node falls in (kind, prefix, ns, part: its key, as ClassPart.key) and its
    identifier (node_key, None when it is none) to the rows of *frame*, from the node's term in
    *term* (as in SPARQL TSV: <iri> or _:label).
    """
    import polars as pl

    frame = frame.with_columns(
        pl.col(term).str.starts_with("_:").alias("_blank"),
        pl.col(term).str.strip_prefix("<").str.strip_suffix(">").alias("_iri"),
    )
    keyed = reader.iri_keys(frame, "_iri", out="_key", matched="_m")
    kind = (
        pl.when(pl.col("_blank"))
        .then(pl.lit("blank_node"))
        .when(~pl.col("_iri").str.contains(":", literal=True))
        .then(pl.lit("relative_iri"))
        .when(~pl.col("_iri").str.contains(_SCHEME))
        .then(pl.lit("invalid_scheme"))
        .when(pl.col("_key").is_not_null())
        .then(pl.lit("identifier"))
        .when(pl.col("_m_prefix").is_not_null())
        .then(pl.lit("invalid_identifier"))
        .otherwise(pl.lit("namespace"))
    )
    other = pl.coalesce(pl.col("_iri").str.extract(_OTHER, 1), pl.col("_iri"))
    classified: pl.LazyFrame = (
        keyed.with_columns(kind.alias("kind"))
        .with_columns(
            pl.when(pl.col("kind").is_in(["identifier", "invalid_identifier"]))
            .then(pl.col("_m_prefix"))
            .alias("prefix"),
            pl.when(pl.col("kind").is_in(["blank_node", "relative_iri"]))
            .then(pl.lit(None, dtype=pl.String))
            .when(pl.col("kind") == "invalid_scheme")
            .then(pl.col("_iri").str.extract(r"^([^:]*:)", 1))
            .otherwise(pl.coalesce(pl.col("_m_ns"), other))
            .alias("ns"),
        )
        .with_columns(part_key().alias("part"))
        .rename({"_key": "node_key"})
        .drop("_blank", "_iri", "_m_ns", "_m_prefix")
    )
    return classified


def part_key() -> pl.Expr:
    """Return the key of the part of a classified row (as ClassPart.key)."""
    import polars as pl

    return (
        pl.when(pl.col("kind").is_in(["namespace", "invalid_scheme"]))
        .then(pl.concat_str(pl.col("kind"), pl.lit("|"), pl.col("ns").fill_null("")))
        .otherwise(pl.concat_str(pl.col("kind"), pl.lit("|"), pl.col("prefix").fill_null("")))
    )


def _parts(rows: Iterable[dict[str, Any]], reader: Any) -> dict[str, list[ClassPart]]:
    """Return the parts of each class from counted rows (c, kind, prefix, ns, n): one part per
    (kind, prefix), or per namespace for IRIs of no registered namespace.
    """
    patterns = dict(reader.rules.select("prefix", "pattern").iter_rows())
    peels = {
        row[0]: [p for p in row[1:] if p]
        for row in reader.rules.select("prefix", "peel_1", "peel_2", "peel_3", "peel_4").iter_rows()
    }
    grouped: dict[str, dict[tuple[str, str | None, str | None], dict[str, Any]]] = {}
    for row in rows:
        other = row["kind"] in ("namespace", "invalid_scheme")
        key = (row["kind"], row["prefix"], row["ns"] if other else None)
        part = grouped.setdefault(row["c"], {}).setdefault(key, {"n": 0, "ns": set()})
        part["n"] += row["n"]
        if row["ns"]:
            part["ns"].add(row["ns"])
    return {
        cls: sorted(
            (
                ClassPart(
                    kind=kind,
                    prefix=prefix,
                    namespaces=sorted(found["ns"]),
                    pattern=patterns.get(prefix) if kind == "identifier" else None,
                    peels=sorted(set(peels.get(prefix) or [])) if kind == "identifier" else [],
                    entities=found["n"],
                )
                for (kind, prefix, _), found in parts.items()
            ),
            key=lambda p: (-p.entities, p.key()),
        )
        for cls, parts in sorted(grouped.items())
    }
