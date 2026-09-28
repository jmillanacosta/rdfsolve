"""Checks of a declared identity (exactMatch, sameAs, a cross-reference) between two identifiers.

Linked data states identities that do not hold: a RefSeq accession filed under the Ensembl
namespace, or a gene "exactMatch" its protein. Chaining such statements (A = B, B = C, so
A = C) spreads the error. These checks name the problem of each statement, so that inferred
chains can carry the flags of their steps.

The kind of an identifier is read from its pattern, because one namespace can hold several
kinds (Ensembl: genes ENSG, transcripts ENST, proteins ENSP). KINDS is curated and short; a
prefix that is not in it, or a pattern that names more than one kind of entity (OMIM: genes
and phenotypes), gives no kind.
"""

from __future__ import annotations

import re

__all__ = ["KINDS", "identifier_kind", "identity_flags"]

# (prefix, local identifier pattern, kind). The first match wins; order specific before general.
KINDS: tuple[tuple[str, str, str], ...] = (
    ("ensembl", r"ENS[A-Z]*G\d{11}(\.\d+)?", "gene"),
    ("ensembl", r"ENS[A-Z]*T\d{11}(\.\d+)?", "transcript"),
    ("ensembl", r"ENS[A-Z]*P\d{11}(\.\d+)?", "protein"),
    ("refseq", r"(NM|NR|XM|XR)_\d+(\.\d+)?", "transcript"),
    ("refseq", r"(NP|XP|YP|WP)_\d+(\.\d+)?", "protein"),
    ("refseq", r"(NG|NC|NT|NW)_\d+(\.\d+)?", "genomic region"),
    ("hgnc", r"\d+", "gene"),
    ("ncbigene", r"\d+", "gene"),
    ("mgi", r"(MGI:)?\d+", "gene"),
    ("uniprot", r"[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}", "protein"),
    ("ccds", r"CCDS\d+(\.\d+)?", "coding sequence"),
)
# Prefixes whose patterns name a kind only by their letter code, so that a value filed under
# another namespace can be recognised as theirs.
RECOGNISABLE = ("ensembl", "refseq", "ccds")


def identifier_kind(curie: str) -> tuple[str, str] | None:
    """Return the kind of entity and the namespace that an identifier's pattern belongs to."""
    prefix, _, local = curie.partition(":")
    for owner, pattern, kind in KINDS:
        if owner == prefix and re.fullmatch(pattern, local):
            return kind, owner
    for owner, pattern, kind in KINDS:
        if owner in RECOGNISABLE and owner != prefix and re.fullmatch(pattern, local):
            return kind, owner
    return None


def identity_flags(left: str, right: str) -> list[str]:
    """Return the problems of the statement that *left* and *right* name the same entity."""
    flags = []
    kinds = []
    for curie in (left, right):
        found = identifier_kind(curie)
        if found and found[1] != curie.partition(":")[0]:
            flags.append(f"namespace:{curie} is a {found[1]} identifier")
        kinds.append(found[0] if found else None)
    if None in kinds:
        flags.append("kind:unknown")
    elif kinds[0] != kinds[1]:
        flags.append(f"kind:{kinds[0]}-{kinds[1]}")
    return flags
