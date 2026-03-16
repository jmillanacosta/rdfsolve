"""Common utility functions for RDF processing."""

from __future__ import annotations


def resolve_curie(curie: str, prefixes: dict[str, str]) -> str | None:
    """Convert CURIE to full IRI using given prefixes.

    Returns full IRI wrapped in angle brackets, or ``None`` if not resolvable.
    """
    if not curie or curie in ("BN", "null", "", "[]"):
        return None

    curie = str(curie).strip()

    if curie.startswith("<") and curie.endswith(">"):
        return curie
    if curie.startswith("http"):
        return f"<{curie}>"
    if curie == "a":
        return "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
    if ":" in curie:
        prefix, localname = curie.split(":", 1)
        if prefix in prefixes:
            base_uri = prefixes[prefix].strip("<>")
            return f"<{base_uri}{localname}>"
    return None


# ---------------------------------------------------------------------------
# URI display helpers
# ---------------------------------------------------------------------------


def get_local_name(uri: str) -> str:
    """Extract the local name from a URI.

    Examples::

        >>> get_local_name("http://example.org/foo#Bar")
        'Bar'
        >>> get_local_name("http://example.org/foo/Bar")
        'Bar'
    """
    if "#" in uri:
        return uri.split("#")[-1]
    return uri.rstrip("/").rsplit("/", 1)[-1] if "/" in uri else uri


def compact_uri(uri: str, prefixes: dict[str, str]) -> str:
    """Compact a URI using the given prefix map.

    Returns ``prefix:localName`` if a match is found, otherwise the
    original URI.
    """
    for pfx, ns in prefixes.items():
        if uri.startswith(ns):
            return f"{pfx}:{uri[len(ns) :]}"
    return uri


def expand_curie(curie: str, prefixes: dict[str, str]) -> str:
    """Expand a CURIE (prefix:local) to a full URI."""
    if ":" not in curie or curie.startswith("http"):
        return curie
    pfx, local = curie.split(":", 1)
    ns = prefixes.get(pfx)
    return f"{ns}{local}" if ns else curie


# ---------------------------------------------------------------------------
# Label selection
# ---------------------------------------------------------------------------


def pick_label(
    rdfs_label: str | None,
    dc_title: str | None,
    uri: str,
    iao_label: str | None = None,
    skos_pref_label: str | None = None,
    skos_alt_label: str | None = None,
) -> str:
    """Choose the best human-readable label.

    Priority:
    1. ``rdfs:label`` / ``skos:prefLabel``
    2. ``dc:title`` / ``dcterms:title``
    3. ``IAO_0000118`` alternate term (OBO ontologies)
    4. ``skos:altLabel``
    5. Local name from URI
    """
    if rdfs_label and rdfs_label.strip():
        return rdfs_label.strip()
    if skos_pref_label and skos_pref_label.strip():
        return skos_pref_label.strip()
    if dc_title and dc_title.strip():
        return dc_title.strip()
    if iao_label and iao_label.strip():
        return iao_label.strip()
    if skos_alt_label and skos_alt_label.strip():
        return skos_alt_label.strip()
    return get_local_name(uri)
