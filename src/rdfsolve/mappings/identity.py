"""Checks of a declared identity (exactMatch, sameAs, a cross-reference) between two identifiers.

Linked data states identities that do not hold, and chaining them (A = B, B = C, so A = C)
spreads the error, so each statement carries the flags of its checks. The check reads only
Bioregistry: an identifier is flagged when its local part does not match the pattern that
Bioregistry gives for the namespace it is filed under (a KEGG DRUG identifier D09637 filed as
kegg.compound). A namespace without a pattern in Bioregistry is not checked.

What kind of entity an identifier names (a gene, a protein, a chemical) is not read here: it is
not in Bioregistry, and a curated table of patterns covered only a few gene and protein
namespaces (removed on 2026-10-02 by the owner). The kind comes from data: the class of the
identifier in the source that issues it.
"""

from __future__ import annotations

__all__ = ["identity_flags"]


def identity_flags(left: str, right: str) -> list[str]:
    """Return the problems of the statement that the CURIEs *left* and *right* name one entity."""
    from rdfsolve.identifiers import parse

    flags = []
    for written in (left, right):
        found = parse(written)
        if found is not None and found.valid is False:
            flags.append(f"namespace:{written} does not match the {found.prefix} pattern")
    return flags
