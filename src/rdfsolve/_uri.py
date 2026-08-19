"""CURIE / URI helpers - expansion & compaction."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable

_log = logging.getLogger(__name__)

_URI_SCHEMES: tuple[str, ...] = ("http://", "https://", "urn:")


# Namespace / prefix extraction


def _ns_from_uri(uri: str) -> str:
    """Extract the namespace portion of a URI.

    Returns everything up to and including ``#`` or ``/``.
    """
    if "#" in uri:
        return uri.rsplit("#", 1)[0] + "#"
    if "/" in uri:
        return uri.rsplit("/", 1)[0] + "/"
    return ""


def _prefix_from_ns(ns: str) -> str:
    """Derive a short prefix from a namespace URI."""
    clean = ns.replace("http://", "").replace("https://", "").replace("www.", "").strip("/#")
    slug = clean.rsplit("/", 1)[-1] if "/" in clean else clean.split(".")[0]
    return re.sub(r"[^a-zA-Z0-9_]", "", slug)[:10]


# Public API: URI -> CURIE


def uri_to_curie(uri: str) -> tuple[str, str, str]:
    """Convert a URI to ``(curie, prefix, namespace)`` via bioregistry.

    uses splitting on ``#`` or ``/`` when bioregistry is
    unavailable or the URI is unknown.
    """
    if uri.startswith(_URI_SCHEMES):
        try:
            from bioregistry import curie_from_iri, parse_iri

            parsed = parse_iri(uri)
            # Guard: parse_iri may return (None, None) which is a truthy
            # tuple but would yield the invalid CURIE "None:None".
            if parsed and parsed[0] is not None and parsed[1] is not None:
                pfx, local = parsed
                # Derive namespace by stripping the local part from the full URI.
                # This preserves infixes like SIO_, IAO_, BAO_, etc. that
                # _ns_from_uri() would drop by splitting on the last "#" or "/".
                ns = uri[: len(uri) - len(local)]
                curie = curie_from_iri(uri) or f"{pfx}:{local}"
                return curie, pfx, ns
        except Exception:
            _log.debug("bioregistry lookup failed for %s", uri, exc_info=True)

    # Fallback: split on # or /
    ns = _ns_from_uri(uri)
    local = uri[len(ns) :] if ns else uri
    pfx = _prefix_from_ns(ns) if ns else ""
    curie = f"{pfx}:{local}" if pfx and local else uri
    return curie, pfx, ns


# Bioregistry prefix map


def _build_br_prefix_map() -> dict[str, str]:
    """Build a ``prefix -> namespace`` dict from bioregistry (once)."""
    result: dict[str, str] = {}
    try:
        from bioregistry import manager as _mgr

        for pfx, res in _mgr.registry.items():
            fmt = res.get_uri_format()
            if fmt and "$1" in fmt:
                ns = fmt.replace("$1", "")
                result[pfx] = ns
                for syn in res.get_synonyms() or []:
                    result.setdefault(syn, ns)
    except Exception:
        _log.debug("bioregistry unavailable for prefix map", exc_info=True)
    return result


# Expander factory (cached closure)


def make_expander(
    context: dict[str, str],
    br_map: dict[str, str] | None = None,
) -> Callable[[str], str]:
    """Return a cached CURIE -> URI expander function.

    The returned closure looks up *context* first, then *br_map*.
    Already-expanded URIs are returned unchanged.  Results are cached
    for the lifetime of the closure.
    """
    cache: dict[str, str] = {}
    br = br_map or {}

    def expand(curie: str) -> str:
        """Expand *curie* to a full URI using *context* and *br_map*."""
        if curie in cache:
            return cache[curie]
        result = curie
        if not curie.startswith(_URI_SCHEMES) and ":" in curie:
            pfx, local = curie.split(":", 1)
            ns = context.get(pfx) or br.get(pfx)
            if ns and isinstance(ns, str):
                result = ns + local
        cache[curie] = result
        return result

    return expand


def expand_curie(curie: str, context: dict[str, str]) -> str:
    """Expand a CURIE using the JSON-LD ``@context``, returning a URI."""
    if curie.startswith(_URI_SCHEMES):
        return curie
    if ":" in curie:
        prefix, local = curie.split(":", 1)
        ns = context.get(prefix)
        if ns and isinstance(ns, str):
            return ns + local
    return curie


def expand_curie_bioregistry(value: str) -> str:
    """Expand a CURIE to a full URI using **bioregistry** only.

    If *value* is already a full URI it is returned unchanged.
    If the prefix is unknown the original string is returned.
    """
    if value.startswith(_URI_SCHEMES):
        return value
    if ":" not in value:
        return value
    prefix, local = value.split(":", 1)
    try:
        import bioregistry

        uri_prefix = bioregistry.get_uri_prefix(prefix)
        if uri_prefix:
            return str(uri_prefix) + local
    except Exception as e:
        _log.warning("Error expanding %s: %s", prefix, e)
    return str(value)


# Compaction & resolution helpers (migrated from utils.py)


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


def resolve_curie(curie: str, prefixes: dict[str, str]) -> str | None:
    """Convert CURIE to full IRI using given prefixes.

    Returns full IRI wrapped in angle brackets, or ``None`` if not resolvable.
    Handles special cases like blank nodes, "a" (rdf:type), etc.
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


# Label selection


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
