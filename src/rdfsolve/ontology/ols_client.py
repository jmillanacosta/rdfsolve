"""HTTP client for the EBI OLS4 REST API v2.

Wraps the public OLS4 endpoint (``https://www.ebi.ac.uk/ols4/api/v2/``)
with:

* Automatic pagination over ``/ontologies`` (20 per page).
* Per-response disk caching via ``diskcache`` with a configurable TTL
  (default 24 h) so repeated index builds skip the network.
* Polite rate-limiting: at most ``rate_limit`` requests per second
  (default 10).
* Graceful degradation — network errors are logged and the affected
  ontology / term is skipped rather than aborting the build.

Typical usage::

    from rdfsolve.ontology.ols_client import OlsClient

    client = OlsClient(cache_dir="/tmp/ols_cache")
    for meta in client.get_all_ontologies():
        print(meta["ontologyId"], meta.get("preferredPrefix"))

    terms = client.get_terms("chebi", search="aspirin")
    by_iri = client.get_term_by_iri("chebi", "http://purl.obolibrary.org/obo/CHEBI_15422")
    ancestors = client.get_ancestors("chebi", "http://purl.obolibrary.org/obo/CHEBI_15422")
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

logger = logging.getLogger(__name__)

_OLS_BASE = "https://www.ebi.ac.uk/ols4/api/v2"
_PAGE_SIZE = 20
_DEFAULT_TTL = 86_400  # 24 hours in seconds

__all__ = ["OlsClient"]


class OlsClient:
    """Thin, cache-aware client for the OLS4 REST API v2.

    Parameters
    ----------
    cache_dir:
        Directory for the ``diskcache`` on-disk cache.  Created if
        absent.  Pass ``None`` to disable caching (network hit on every
        call — only recommended for tests).
    ttl:
        Cache entry time-to-live in seconds (default 86 400 = 24 h).
    rate_limit:
        Maximum HTTP requests per second (default 10).
    base_url:
        OLS4 API base URL (override for testing / mirrors).
    timeout:
        Per-request HTTP timeout in seconds (default 30).
    """

    def __init__(
        self,
        cache_dir: str | None = None,
        *,
        ttl: int = _DEFAULT_TTL,
        rate_limit: float = 10.0,
        base_url: str = _OLS_BASE,
        timeout: float = 30.0,
    ) -> None:
        """Initialise the client, opening the disk cache if requested."""
        self._base = base_url.rstrip("/")
        self._ttl = ttl
        self._min_interval = 1.0 / rate_limit
        self._last_request: float = 0.0
        self._timeout = timeout
        self._cache: Any = None  # diskcache.Cache | None

        if cache_dir is not None:
            try:
                import diskcache

                self._cache = diskcache.Cache(cache_dir)
                logger.debug("OlsClient: disk cache at %s (ttl=%ds)", cache_dir, ttl)
            except ImportError:
                logger.warning(
                    "diskcache not installed; OLS responses will not be cached. "
                    "Install it with: pip install diskcache"
                )

    # ── low-level HTTP ────────────────────────────────────────────

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """Make a rate-limited GET request, using the cache when available.

        Parameters
        ----------
        url:
            Full URL to fetch.
        params:
            Optional query-string parameters dict.

        Returns
        -------
        Any
            Parsed JSON response body, or ``None`` on error.
        """
        cache_key = self._make_cache_key(url, params)

        if self._cache is not None:
            hit = self._cache.get(cache_key)
            if hit is not None:
                return hit

        self._throttle()
        try:
            import httpx

            response = httpx.get(url, params=params, timeout=self._timeout)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            logger.warning("OLS4 request failed [%s]: %s", url, exc)
            return None

        if self._cache is not None:
            self._cache.set(cache_key, data, expire=self._ttl)

        return data

    def _throttle(self) -> None:
        """Sleep if necessary to respect the rate limit."""
        elapsed = time.monotonic() - self._last_request
        gap = self._min_interval - elapsed
        if gap > 0:
            time.sleep(gap)
        self._last_request = time.monotonic()

    @staticmethod
    def _make_cache_key(url: str, params: dict[str, Any] | None) -> str:
        """Derive a stable cache key from URL + sorted params."""
        raw = url
        if params:
            raw += "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        return hashlib.sha256(raw.encode()).hexdigest()

    # ── paginated ontology list ───────────────────────────────────

    def get_all_ontologies(self) -> Iterator[dict[str, Any]]:
        """Yield metadata dicts for every ontology known to OLS4.

        Paginates through ``/api/v2/ontologies`` (20 per page) and skips
        ``inactive`` / ``deprecated`` ontologies by default.

        Yields
        ------
        dict
            One OLS4 ontology metadata object per iteration.  Key fields:
            ``ontologyId``, ``preferredPrefix``, ``baseUri``,
            ``importsFrom``, ``exportsTo``, ``numberOfClasses``,
            ``activity_status``, ``domain``.
        """
        page = 0
        while True:
            url = f"{self._base}/ontologies"
            data = self._get(url, params={"page": page, "size": _PAGE_SIZE})
            if data is None:
                break

            elements = data.get("elements", [])
            if not elements:
                break

            for ont in elements:
                status = ont.get("activity_status", "active")
                if status in ("inactive", "deprecated"):
                    logger.debug("Skipping %s (status=%s)", ont.get("ontologyId"), status)
                    continue
                yield ont

            # Determine whether there are more pages.  The OLS4 v2 response
            # has totalPages as a top-level field alongside "elements".
            total_pages = data.get("totalPages")
            if total_pages is not None:
                if page + 1 >= int(total_pages):
                    break
            elif len(elements) < _PAGE_SIZE:
                break
            page += 1

    # ── term search ───────────────────────────────────────────────

    def get_all_terms(
        self,
        ontology_id: str,
        *,
        page_limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield all terms for *ontology_id* by paginating the classes endpoint.

        Uses ``/api/v2/ontologies/{id}/classes`` to enumerate every class
        in the ontology (the ``/terms`` endpoint returns 404 on OLS4 v2).

        Parameters
        ----------
        ontology_id:
            OLS4 ontology identifier (e.g. ``"chebi"``, ``"go"``).
        page_limit:
            Stop after this many pages.  ``None`` (default) fetches all pages.

        Yields
        ------
        dict
            One OLS4 term object per iteration.
        """
        url = f"{self._base}/ontologies/{ontology_id}/classes"
        page = 0
        while True:
            data = self._get(url, params={"page": page, "size": _PAGE_SIZE})
            if data is None:
                break
            elements = data.get("elements", [])
            if not elements:
                break
            yield from elements
            total_pages = data.get("totalPages")
            if total_pages is not None:
                if page + 1 >= int(total_pages):
                    break
            elif len(elements) < _PAGE_SIZE:
                break
            if page_limit is not None and page + 1 >= page_limit:
                break
            page += 1

    def get_terms(
        self,
        ontology_id: str,
        search: str,
        *,
        exact: bool = False,
        rows: int = 10,
    ) -> list[dict[str, Any]]:
        """Search for terms matching *search* within *ontology_id*.

        Parameters
        ----------
        ontology_id:
            OLS4 ontology identifier (e.g. ``"chebi"``, ``"go"``).
        search:
            Free-text or exact label/synonym to search for.
        exact:
            When ``True``, request exact-match results only.
        rows:
            Maximum number of results to return (default 10).

        Returns
        -------
        list[dict]
            List of OLS4 term objects.  Each has ``iri``, ``label``,
            ``synonyms``, ``description``, ``ontologyId``.
            Returns an empty list on error or no results.
        """
        url = f"{self._base}/ontologies/{ontology_id}/terms"
        params: dict[str, Any] = {
            "search": search,
            "exactMatch": str(exact).lower(),
            "rows": rows,
        }
        data = self._get(url, params=params)
        if data is None:
            return []
        return list(data.get("elements", []))

    def get_term_by_iri(
        self,
        ontology_id: str,
        iri: str,
    ) -> dict[str, Any] | None:
        """Fetch a single term by its full IRI within *ontology_id*.

        Parameters
        ----------
        ontology_id:
            OLS4 ontology identifier.
        iri:
            Full IRI of the term
            (e.g. ``"http://purl.obolibrary.org/obo/CHEBI_15422"``).

        Returns
        -------
        dict or None
            OLS4 term object, or ``None`` if not found / on error.
        """
        url = f"{self._base}/ontologies/{ontology_id}/classes"
        data = self._get(url, params={"iri": iri})
        if data is None:
            return None
        elements = data.get("elements", [])
        return elements[0] if elements else None

    def get_ancestors(
        self,
        ontology_id: str,
        iri: str,
    ) -> list[dict[str, Any]]:
        """Return hierarchical ancestors of *iri* within *ontology_id*.

        Parameters
        ----------
        ontology_id:
            OLS4 ontology identifier.
        iri:
            Full IRI of the term whose ancestors to fetch.

        Returns
        -------
        list[dict]
            List of ancestor term objects (nearest first), each with
            ``iri`` and ``label``.  Returns an empty list on error.
        """
        encoded = quote(quote(iri, safe=""))
        url = f"{self._base}/ontologies/{ontology_id}/classes/{encoded}/hierarchicalAncestors"
        data = self._get(url)
        if data is None:
            return []
        return list(data.get("elements", []))

    # ── ontology metadata helpers ─────────────────────────────────

    def get_ontology(self, ontology_id: str) -> dict[str, Any] | None:
        """Fetch metadata for a single ontology by its OLS4 identifier.

        Parameters
        ----------
        ontology_id:
            OLS4 ontology identifier (e.g. ``"chebi"``).

        Returns
        -------
        dict or None
            OLS4 ontology metadata object, or ``None`` on error.
        """
        url = f"{self._base}/ontologies/{ontology_id}"
        result: dict[str, Any] | None = self._get(url)
        return result

    def close(self) -> None:
        """Close the disk cache if open."""
        if self._cache is not None:
            self._cache.close()
            self._cache = None

    def __enter__(self) -> OlsClient:
        """Support use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close cache on context manager exit."""
        self.close()
