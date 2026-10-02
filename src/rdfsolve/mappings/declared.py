"""Identities that sources declare (cross-references, skos:exactMatch, owl:sameAs), with the checks
of rdfsolve.mappings.identity on each statement.

The statements are kept as the source gives them, also when they are wrong: they are what a user
who treats cross-references as identity reads, and chaining them spreads their errors. Each
statement carries its flags, and a file that holds a statement that fails a check (an
identifier that does not match the Bioregistry pattern of its namespace) is marked as having
flagged statements.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rdfsolve.mappings.identity import identity_flags

__all__ = [
    "DeclaredIdentity",
    "declared_identities",
    "declared_query",
    "is_declared_property",
    "write_declared_identities",
]

IDENTITY_PROPERTIES = (
    "http://www.w3.org/2004/02/skos/core#exactMatch",
    "http://www.w3.org/2002/07/owl#sameAs",
)
# Bio2RDF cross-references: <prefix>_vocabulary:x-<namespace>.
XREF_MARKER = "_vocabulary:x-"


@dataclass
class DeclaredIdentity:
    """One declared identity between two identifiers of different namespaces."""

    subject_id: str
    object_id: str
    source: str
    source_property: str
    flags: list[str] = field(default_factory=list)

    @property
    def comment(self) -> str:
        """Return the source and the local name of the property that declares the identity."""
        return f"{self.source} {re.split(r'[/#:]', self.source_property)[-1]}"


def is_declared_property(iri: str) -> bool:
    """Return True for a property that declares two identifiers to name the same entity."""
    return iri in IDENTITY_PROPERTIES or XREF_MARKER in iri


def declared_query() -> str:
    """Return the SPARQL query that reads every declared identity of a source."""
    values = " ".join(f"<{iri}>" for iri in IDENTITY_PROPERTIES)
    return (
        "SELECT ?s ?p ?o WHERE { ?s ?p ?o FILTER(isIRI(?o)) "
        f'FILTER(?p IN ({values.replace(" ", ", ")}) || CONTAINS(STR(?p), "{XREF_MARKER}")) }}'
    )


def _curie(iri: str) -> str | None:
    """Return the CURIE of an IRI with the namespace the source gave it (no validity check).

    An identifiers.org IRI that Bioregistry does not read (http://identifiers.org/mgi/101757)
    is read as namespace/identifier.
    """
    import bioregistry

    parsed = bioregistry.parse_iri(iri)
    if not parsed or not parsed[0]:
        return None
    prefix, local = parsed
    if prefix == "idot" and "/" in local:
        prefix, _, local = local.partition("/")
    return f"{bioregistry.normalize_prefix(prefix) or prefix}:{local}"


def declared_identities(
    bindings: Iterable[Mapping[str, Mapping[str, str]]], source: str
) -> list[DeclaredIdentity]:
    """Return the declared identities in SPARQL JSON bindings of ?s ?p ?o, each with its flags.

    A pair is kept once (in either direction). A value that is not an identifier IRI, a
    statement of an identifier with itself, and a pair in one namespace are left out.
    """
    rows: dict[tuple[str, str], DeclaredIdentity] = {}
    for binding in bindings:
        subject, obj = _curie(binding["s"]["value"]), _curie(binding["o"]["value"])
        if not subject or not obj or subject.partition(":")[0] == obj.partition(":")[0]:
            continue
        key = (min(subject, obj), max(subject, obj))
        if key not in rows:
            rows[key] = DeclaredIdentity(
                subject, obj, source, binding["p"]["value"], identity_flags(subject, obj)
            )
    return list(rows.values())


def _kind(flag: str) -> str:
    """Return the kind of a flag: namespace, or the flag itself."""
    return "namespace" if flag.startswith("namespace:") else flag


def write_declared_identities(
    rows: Iterable[DeclaredIdentity], out_dir: Path, name: str, *, license_uri: str
) -> dict[str, Any]:
    """Write <name>_declared_identities.sssom.tsv and a summary with the result of the checks.

    The file has flagged statements when a statement fails a check (an identifier that does
    not match the Bioregistry pattern of its namespace). The flags of each statement are in the other column of the SSSOM table. The
    statements are the data of the source, so the table carries the licence of the source.
    """
    import bioregistry
    from curies import Converter
    from sssom import Mapping as SSSOMMapping

    from rdfsolve.config import get_base_uri, mint
    from rdfsolve.mappings.sssom import create_sssom_mappings, write_sssom_tsv

    rows = list(rows)
    counts: Counter[str] = Counter()
    flagged = 0
    for row in rows:
        kinds = {_kind(f) for f in row.flags}
        counts.update(kinds or {"clean"})
        flagged += bool(kinds)
    prefixes = {c.partition(":")[0] for row in rows for c in (row.subject_id, row.object_id)}
    prefix_map = {"rdfsolve": get_base_uri()}
    for prefix in sorted(prefixes):
        prefix_map[prefix] = (
            bioregistry.get_uri_prefix(prefix) or f"https://bioregistry.io/{prefix}:"
        )
    converter = Converter.from_prefix_map(prefix_map)
    mappings = [
        SSSOMMapping(
            subject_id=row.subject_id,
            subject_source=converter.compress(mint("dataset", row.source), strict=True),
            predicate_id="skos:exactMatch",
            object_id=row.object_id,
            mapping_justification="semapv:UnspecifiedMatching",
            comment=row.comment,
            other=json.dumps({"flags": row.flags}),
        )
        for row in rows
    ]
    out_dir.mkdir(parents=True, exist_ok=True)
    table = out_dir / f"{name}_declared_identities.sssom.tsv"
    msdf = create_sssom_mappings(
        mappings,
        mint("mappings", f"declared-identities-{name}"),
        license_uri=license_uri,
        converter=converter,
    )
    write_sssom_tsv(msdf, table)
    summary = {
        "name": name,
        "statements": len(rows),
        "flagged": flagged,
        "check": "flagged_statements" if flagged else "no_flagged_statements",
        "flags": dict(counts.most_common()),
        "table": table.name,
        "license": license_uri,
    }
    (out_dir / f"{name}_declared_identities.json").write_text(json.dumps(summary, indent=1))
    return summary
