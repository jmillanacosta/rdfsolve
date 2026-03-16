"""CURIE / URI helpers - expansion & compaction."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable

_log = logging.getLogger(__name__)

_URI_SCHEMES: tuple[str, ...] = ("http://", "https://", "urn:")


# ---------------------------------------------------------------------------
# Namespace / prefix extraction
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Public API: URI -> CURIE
# ---------------------------------------------------------------------------


def uri_to_curie(uri: str) -> tuple[str, str, str]:
    """Convert a URI to ``(curie, prefix, namespace)`` via bioregistry.

    Falls back to splitting on ``#`` or ``/`` when bioregistry is
    unavailable or the URI is unknown.
    """
    if uri.startswith(_URI_SCHEMES):
        try:
            from bioregistry import curie_from_iri, parse_iri

            parsed = parse_iri(uri)
            if parsed:
                pfx, local = parsed
                ns = _ns_from_uri(uri)
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


# ---------------------------------------------------------------------------
# Bioregistry prefix map
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Expander factory (cached closure)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# One-shot convenience helpers
# ---------------------------------------------------------------------------


def expand_curie(curie: str, context: dict[str, str]) -> str:
    """Expand a CURIE using the JSON-LD ``@context``, returning a URI.

    If *curie* is already a full URI it is returned unchanged.
    """
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
        _log.warning("Error expanding %s: %s", uri_prefix, e)
        pass
    return str(value)
