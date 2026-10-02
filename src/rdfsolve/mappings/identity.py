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
    import bioregistry

    flags = []
    for curie in (left, right):
        prefix, _, local = curie.partition(":")
        namespace = bioregistry.normalize_prefix(prefix)
        if namespace is None or not bioregistry.get_pattern(namespace):
            continue
        local = bioregistry.standardize_identifier(namespace, local)
        if not bioregistry.is_valid_identifier(namespace, local):
            flags.append(f"namespace:{curie} does not match the {namespace} pattern")
    return flags
