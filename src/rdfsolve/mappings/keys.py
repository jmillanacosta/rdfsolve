"""Identifier keys of many terms at once: IRIs, CURIEs and local identifiers read in columns.

rdfsolve.identifiers.parse reads one term at a time. Reading every term of a large store or a
large mapping set so takes too long; IdentifierReader reads a column of terms with Polars, with
the same rules: a registered namespace (Bioregistry's URI prefixes and the URI formats of the
source registry), the standard local identifier (no redundant prefix or banana), and its
validity for the prefix (its pattern). The key of an identifier is its CURIE with the normalized
prefix, as parse writes it; a term that is no valid registered identifier has no key.

The namespace of an IRI is matched at the last separators of the IRI (/ # : = _ ?): the longest
registered namespace that ends there, or that starts there and is a prefix of the IRI.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

__all__ = ["IdentifierReader"]

# The characters after which an IRI's local identifier can start.
SEPARATORS = "/#:=_?"
# How many separators from the end of an IRI its namespace is looked for.
DEPTH = 3
_STEM = r"^(.*[/#:=_?])"
_RESOLVER = re.compile(r"^https?://(?:www\.)?(?:identifiers\.org|n2t\.net)/([^/:]+)[/:]$")


class IdentifierReader:
    """Read identifiers of registered prefixes in columns of terms.

    *prefixes* limits the identifiers read to these normalized prefixes (all when None), which
    keeps the namespace tables small.
    """

    def __init__(self, prefixes: Iterable[str] | None = None) -> None:
        """Build the namespace and rule tables of the prefixes."""
        import polars as pl

        wanted = None if prefixes is None else frozenset(prefixes)
        namespaces = {
            ns: prefix for ns, prefix in _namespaces().items() if wanted is None or prefix in wanted
        }
        ends = {ns: p for ns, p in namespaces.items() if ns[-1] in SEPARATORS}
        self.namespaces = pl.DataFrame(
            {
                "ns": list(ends),
                "ns_prefix": list(ends.values()),
                "ns_fallback": [_FALLBACK.get(ns) for ns in ends],
            },
            schema={"ns": pl.String, "ns_prefix": pl.String, "ns_fallback": pl.String},
        )
        # The few namespaces that do not end in a separator are matched by one expression,
        # the longest first.
        self.tails = {ns: p for ns, p in namespaces.items() if ns[-1] not in SEPARATORS}
        self._tail_pattern = (
            "^(" + "|".join(re.escape(ns) for ns in sorted(self.tails, key=len, reverse=True)) + ")"
            if self.tails
            else None
        )
        rules = _rules(wanted)
        self.rules = pl.DataFrame(
            rules,
            schema={
                "prefix": pl.String,
                "peel_1": pl.String,
                "peel_2": pl.String,
                "peel_3": pl.String,
                "peel_4": pl.String,
                "pattern": pl.String,
            },
            orient="row",
        ).with_columns(known=pl.lit(value=True))
        self.prefixes = wanted

    def iri_keys(
        self, frame: pl.LazyFrame, column: str, out: str = "key", matched: str | None = None
    ) -> pl.LazyFrame:
        """Add the key (*out*) of the IRI in *column* (written without angle brackets).

        With *matched*, also add the registered namespace the IRI starts with (matched_ns) and
        its prefix (matched_prefix), also where the local identifier is not valid for it.
        """
        import polars as pl

        iri = pl.col(column)
        frame = frame.with_columns(iri.str.extract(_STEM, 1).alias("_stem_1"))
        for level in range(2, DEPTH + 1):
            previous = pl.col(f"_stem_{level - 1}")
            frame = frame.with_columns(
                previous.str.slice(0, previous.str.len_chars() - 1)
                .str.extract(_STEM, 1)
                .alias(f"_stem_{level}")
            )
        for level in range(1, DEPTH + 1):
            frame = frame.join(
                self.namespaces.lazy().rename(
                    {
                        "ns": f"_stem_{level}",
                        "ns_prefix": f"_np_{level}",
                        "ns_fallback": f"_nf_{level}",
                    }
                ),
                on=f"_stem_{level}",
                how="left",
                maintain_order="left",
            )
        # The longest namespace that ends at a separator (the first level found) ...
        stem = pl.coalesce(
            *[
                pl.when(pl.col(f"_np_{level}").is_not_null()).then(pl.col(f"_stem_{level}"))
                for level in range(1, DEPTH + 1)
            ]
        )
        stem_prefix = pl.coalesce(*[pl.col(f"_np_{level}") for level in range(1, DEPTH + 1)])
        fallback = pl.coalesce(
            *[
                pl.when(pl.col(f"_np_{level}").is_not_null()).then(pl.col(f"_nf_{level}"))
                for level in range(1, DEPTH + 1)
            ]
        )
        # ... unless a longer namespace that ends elsewhere starts the IRI.
        if self._tail_pattern is not None:
            tail = iri.str.extract(self._tail_pattern, 1)
        else:
            tail = pl.lit(None, dtype=pl.String)
        frame = frame.with_columns(
            stem.alias("_sns"), stem_prefix.alias("_sp"), fallback.alias("_sf"), tail.alias("_tns")
        ).with_columns(
            pl.col("_tns")
            .replace_strict(self.tails, default=None, return_dtype=pl.String)
            .alias("_tp")
        )
        longer = pl.col("_tns").is_not_null() & (
            pl.col("_sns").is_null()
            | (pl.col("_tns").str.len_chars() > pl.col("_sns").str.len_chars())
        )
        frame = frame.with_columns(
            pl.when(longer).then(pl.col("_tns")).otherwise(pl.col("_sns")).alias("_ns"),
            pl.when(longer).then(pl.col("_tp")).otherwise(pl.col("_sp")).alias("_prefix"),
        ).with_columns(iri.str.slice(pl.col("_ns").str.len_chars()).alias("_local"))
        frame = self._standard(frame, "_prefix", "_local", "_named")
        frame = frame.with_columns(
            pl.when(~longer & pl.col("_named").is_null()).then(pl.col("_sf")).alias("_sf")
        )
        frame = self._standard(frame, "_sf", "_local", "_read")
        frame = frame.with_columns(pl.coalesce("_named", "_read").alias(out))
        if matched is not None:
            frame = frame.with_columns(
                pl.col("_ns").alias(f"{matched}_ns"),
                pl.coalesce(pl.col(out).str.extract(r"^([^:]+):", 1), pl.col("_prefix")).alias(
                    f"{matched}_prefix"
                ),
            )
        temporary = [f"_{kind}_{n}" for kind in ("stem", "np", "nf") for n in range(1, DEPTH + 1)]
        return frame.drop(
            *temporary,
            "_sns",
            "_sp",
            "_sf",
            "_tns",
            "_tp",
            "_ns",
            "_prefix",
            "_local",
            "_named",
            "_read",
        )

    def curie_keys(
        self,
        frame: pl.LazyFrame,
        column: str,
        out: str = "key",
        prefix_map: Mapping[str, str] | None = None,
    ) -> pl.LazyFrame:
        """Add the key (*out*) of the CURIE or IRI in *column*.

        A CURIE's prefix is normalized by Bioregistry, else read through the namespace that
        *prefix_map* (the mapping set's own) gives it. A value that starts as an HTTP(S) or URN
        IRI is read as an IRI.
        """
        import polars as pl

        value = pl.col(column)
        is_iri = value.str.contains(r"^(?:https?://|urn:)")
        frame = frame.with_columns(
            pl.when(is_iri).then(value).alias("_iri"),
            pl.when(~is_iri).then(value.str.extract(r"^([^:\s]+):", 1)).alias("_written"),
            pl.when(~is_iri).then(value.str.extract(r"^[^:\s]+:(\S+)$", 1)).alias("_clocal"),
        )
        written = (
            frame.select(pl.col("_written").drop_nulls().unique()).collect()["_written"].to_list()
        )
        normalized = {w: self.prefix_of(w, prefix_map) for w in written}
        frame = frame.with_columns(
            pl.col("_written")
            .replace_strict(normalized, default=None, return_dtype=pl.String)
            .alias("_cprefix")
        )
        frame = self._standard(frame, "_cprefix", "_clocal", "_ckey")
        frame = self.iri_keys(frame, "_iri", "_ikey")
        return frame.with_columns(pl.coalesce("_ckey", "_ikey").alias(out)).drop(
            "_iri", "_written", "_clocal", "_cprefix", "_ckey", "_ikey"
        )

    def local_keys(
        self, frame: pl.LazyFrame, column: str, prefix: str | pl.Expr, out: str = "key"
    ) -> pl.LazyFrame:
        """Add the key (*out*) of the local identifier in *column*, of the given prefix."""
        import polars as pl

        given = pl.lit(prefix) if isinstance(prefix, str) else prefix
        frame = frame.with_columns(given.alias("_lprefix"), pl.col(column).alias("_llocal"))
        return self._standard(frame, "_lprefix", "_llocal", out).drop("_lprefix", "_llocal")

    def prefix_of(self, written: str, prefix_map: Mapping[str, str] | None = None) -> str | None:
        """Return the normalized prefix of a CURIE prefix, or None when it is not registered."""
        found = _normalize(written)
        if found is None and prefix_map and written in prefix_map:
            found = _namespaces().get(prefix_map[written])
        if found is not None and self.prefixes is not None and found not in self.prefixes:
            return None
        return found

    def _standard(self, frame: pl.LazyFrame, prefix: str, local: str, out: str) -> pl.LazyFrame:
        """Add the key of a (prefix, local identifier): the standard local, when valid."""
        import polars as pl

        rules = self.rules.lazy().rename({"prefix": prefix})
        frame = frame.join(rules, on=prefix, how="left", maintain_order="left")
        lowered = pl.col(local).str.to_lowercase()
        stripped = pl.coalesce(
            *[
                pl.when(
                    pl.col(f"peel_{n}").is_not_null() & lowered.str.starts_with(pl.col(f"peel_{n}"))
                ).then(pl.col(local).str.slice(pl.col(f"peel_{n}").str.len_chars()))
                for n in range(1, 5)
            ],
            pl.col(local),
        )
        frame = frame.with_columns(stripped.alias("_standard"))
        valid = (
            pl.col("known").fill_null(value=False)
            & pl.col("_standard").str.len_chars().gt(0).fill_null(value=False)
            & (
                pl.col("pattern").is_null()
                | pl.col("_standard").str.contains(pl.col("pattern"), strict=False)
            ).fill_null(value=False)
        )
        return frame.with_columns(
            pl.when(valid)
            .then(pl.concat_str(pl.col(prefix), pl.lit(":"), pl.col("_standard")))
            .alias(out)
        ).drop("peel_1", "peel_2", "peel_3", "peel_4", "pattern", "known", "_standard")


# The prefix Bioregistry reads a resolver namespace as, where it names another namespace: an
# identifier that is not valid in the named namespace is read so (as parse reads it).
_FALLBACK: dict[str, str] = {}


@functools.cache
def _normalize(written: str) -> str | None:
    """Return Bioregistry's normalized prefix of a written prefix."""
    import bioregistry

    return bioregistry.normalize_prefix(written)


@functools.cache
def _namespaces() -> dict[str, str]:
    """Return every registered namespace with its normalized prefix.

    Bioregistry's URI prefixes, the forms of the resolvers that name a namespace
    (identifiers.org and n2t.net, under the prefix and its MIRIAM prefix), and the URI formats
    the source registry adds. A namespace Bioregistry gives to one prefix is not given to another.
    """
    import bioregistry

    from rdfsolve.identifiers import registry_uri_formats

    found: dict[str, str] = dict(bioregistry.manager.get_converter().reverse_prefix_map)
    # A resolver IRI names its namespace: the namespace it names is kept, where registered, as
    # rdfsolve.identifiers.parse keeps it.
    for namespace in list(found):
        match = _RESOLVER.match(namespace)
        if match and (named := _normalize(match[1])) and named != found[namespace]:
            _FALLBACK[namespace] = found[namespace]
            found[namespace] = named
    for prefix, resource in bioregistry.read_registry().items():
        names = {prefix, resource.get_miriam_prefix() or prefix}
        for name in names:
            for scheme in ("http", "https"):
                for host in ("identifiers.org", "n2t.net"):
                    for separator in ("/", ":"):
                        found.setdefault(f"{scheme}://{host}/{name}{separator}", prefix)
    for prefix, forms in registry_uri_formats().items():
        for form in forms:
            if form.endswith("$1") and form.count("$1") == 1:
                found.setdefault(form[:-2], prefix)
    return found


def _rules(prefixes: frozenset[str] | None) -> list[tuple[str | None, ...]]:
    """Return, per prefix, the redundant prefixes a local identifier may start with (lowercase,
    in the order Bioregistry strips them) and the pattern of a valid local identifier.
    """
    import bioregistry

    rows = []
    for prefix, resource in bioregistry.read_registry().items():
        if prefixes is not None and prefix not in prefixes:
            continue
        banana = resource.get_banana()
        peels: list[str | None] = []
        for peel in (resource.get_banana_peel(), "_"):
            peels.append(f"{banana}{peel}".casefold() if banana else None)
            peels.append(f"{prefix.casefold()}{peel}")
        pattern = resource.get_pattern()
        rows.append((prefix, *peels, f"^(?:{pattern.strip('^$')})$" if pattern else None))
    return rows
