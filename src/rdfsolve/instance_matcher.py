"""Instance-based matching: probe SPARQL endpoints for bioregistry URI patterns.

Given a bioregistry resource prefix (e.g. ``"ensembl"``), this module
queries every rdfsolve data source for the RDF classes whose instances
match the resource's known URI prefixes.  When two datasets both contain
instances of the same resource, a mapping edge is emitted between their
respective classes.

The result is an :class:`~rdfsolve.models.InstanceMapping` that can be
serialised to JSON-LD and imported into the rdfsolve database alongside
mined schemas.  The JSON-LD format is identical to a mined schema's, so
the frontend ``parseJSONLD`` pipeline works without any changes —
``skos:narrowMatch`` edges become walkable graph edges in the UI.

Typical usage::

    import pandas as pd
    from rdfsolve.instance_matcher import probe_resource

    datasources = pd.read_csv("data/sources.csv")
    mapping = probe_resource("ensembl", datasources)
    jsonld = mapping.to_jsonld()
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from rdfsolve.models import (
    AboutMetadata,
    InstanceMatchResult,
    InstanceMapping,
    MappingEdge,
    SKOS_NARROW_MATCH,
)
from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = ["probe_resource"]


def _get_uri_formats(prefix: str) -> list[str]:
    """Return deduplicated URI prefix strings for a bioregistry resource.

    Delegates to :meth:`bioregistry.Resource.get_uri_prefixes`, which
    already handles clipping the ``$1`` placeholder and skipping formats
    where ``$1`` does not appear at the end (e.g. CGI-style URLs like
    ``mesh.2012``'s ``…index=$1&view=expanded``).

    Args:
        prefix: Bioregistry prefix (e.g. ``"ensembl"``).

    Returns:
        List of URI prefix strings (may be empty if bioregistry has no
        formats registered for this resource).

    Raises:
        ValueError: If *prefix* is unknown to bioregistry.
    """
    import bioregistry

    resource = bioregistry.get_resource(prefix)
    if resource is None:
        raise ValueError(
            f"Unknown bioregistry prefix: {prefix!r}. "
            "Check https://bioregistry.io/ for valid prefixes."
        )
    # get_uri_prefixes() already clips the trailing $1 and skips formats
    # where $1 is not at the end (e.g. mesh.2012's CGI-style URLs).
    raw = resource.get_uri_prefixes() or set()
    seen: set[str] = set()
    formats: list[str] = []
    for prefix_str in sorted(raw):  # sorted for deterministic order
        if prefix_str and prefix_str not in seen:
            seen.add(prefix_str)
            formats.append(prefix_str)
    return formats


def _probe_dataset(
    dataset_name: str,
    endpoint_url: str,
    uri_formats: list[str],
    timeout: float,
) -> list[InstanceMatchResult]:
    """Run all URI-format probes against one SPARQL endpoint.

    Args:
        dataset_name: Human-readable name of the dataset.
        endpoint_url: SPARQL endpoint URL.
        uri_formats: List of URI prefix strings to probe.
        timeout: HTTP timeout per request.

    Returns:
        One :class:`InstanceMatchResult` per (uri_format, class_uri) hit.
        Empty if the endpoint is unreachable or returns no results.
    """
    results: list[InstanceMatchResult] = []
    try:
        sparql = SparqlHelper(endpoint_url, timeout=timeout)
    except Exception as exc:
        logger.warning(
            "Could not create SparqlHelper for %s (%s): %s",
            dataset_name, endpoint_url, exc,
        )
        return results

    for uri_format in uri_formats:
        logger.info(
            "Probing  dataset=%-20s  endpoint=%s  pattern=%s",
            dataset_name, endpoint_url, uri_format,
        )
        try:
            classes = sparql.find_classes_for_uri_pattern(uri_format)
        except Exception as exc:
            logger.warning(
                "Probe failed — dataset=%s format=%s: %s",
                dataset_name, uri_format, exc,
            )
            continue

        if classes:
            logger.info(
                "  → %d hit(s): %s",
                len(classes), ", ".join(classes),
            )
        else:
            logger.debug("  → no hits")

        for cls_uri in classes:
            results.append(InstanceMatchResult(
                dataset_name=dataset_name,
                endpoint_url=endpoint_url,
                uri_format=uri_format,
                matched_class=cls_uri,
            ))

    return results


def _build_edges(
    match_results: list[InstanceMatchResult],
    predicate: str,
) -> list[MappingEdge]:
    """Generate pairwise mapping edges from probe results.

    An edge is created for every pair of hits that:

    * have **different class URIs** (never map a class to itself), and
    * are not already represented by a reverse edge.

    This includes intra-dataset pairs: when the same dataset exposes two
    distinct classes that both contain instances of the same resource
    (e.g. ``Gene`` and ``GeneAnnotation`` both with Ensembl URIs), the
    edge between them is meaningful and must be kept.

    Duplicate pairs (same source/target regardless of direction) are
    suppressed via a canonicalised key.

    Args:
        match_results: Raw hits from :func:`_probe_dataset`.
        predicate: Mapping predicate URI.

    Returns:
        Deduplicated list of :class:`MappingEdge` instances.
    """
    hits = [r for r in match_results if r.matched_class]
    edges: list[MappingEdge] = []
    seen: set[tuple[str, str, str, str]] = set()

    for i, a in enumerate(hits):
        for b in hits[i + 1:]:
            assert a.matched_class is not None
            assert b.matched_class is not None
            # Skip exact duplicates (same dataset AND same class)
            if (
                a.dataset_name == b.dataset_name
                and a.matched_class == b.matched_class
            ):
                continue
            # Canonicalise order so (A→B) and (B→A) count as one edge
            src, tgt = (a, b) if (
                (a.dataset_name, a.matched_class)
                <= (b.dataset_name, b.matched_class)
            ) else (b, a)
            key = (
                src.dataset_name, src.matched_class,
                tgt.dataset_name, tgt.matched_class,
            )
            if key in seen:
                continue
            seen.add(key)
            edges.append(MappingEdge(
                source_class=src.matched_class,
                target_class=tgt.matched_class,
                predicate=predicate,
                source_dataset=src.dataset_name,
                target_dataset=tgt.dataset_name,
                source_endpoint=src.endpoint_url,
                target_endpoint=tgt.endpoint_url,
            ))

    return edges


def probe_resource(
    prefix: str,
    datasources: pd.DataFrame,
    predicate: str = SKOS_NARROW_MATCH,
    dataset_names: Optional[list[str]] = None,
    timeout: float = 60.0,
) -> InstanceMapping:
    """Probe SPARQL endpoints for a bioregistry resource.

    Steps:

    1. Resolve URI format prefixes for *prefix* via bioregistry.
    2. Optionally filter *datasources* to *dataset_names*.
    3. For each dataset, query its endpoint with each URI prefix using
       ``STRSTARTS``-based ``SELECT DISTINCT ?c``.
    4. Build pairwise :class:`MappingEdge` instances between any two
       **distinct classes** that both matched the resource — including
       two classes within the *same* dataset (e.g. ``Gene`` and
       ``GeneAnnotation`` in the same endpoint both having Ensembl
       instance URIs are linked just like cross-dataset classes).
    5. Return an :class:`InstanceMapping` ready for ``.to_jsonld()``.

    Args:
        prefix: Bioregistry prefix, e.g. ``"ensembl"``.
        datasources: DataFrame with at least columns
            ``dataset_name`` and ``endpoint_url``.
        predicate: Mapping predicate URI.  Defaults to
            ``skos:narrowMatch``.  Override to ``skos:exactMatch``,
            ``owl:sameAs``, etc. as appropriate.
        dataset_names: If given, only probe these datasets.
        timeout: SPARQL HTTP timeout per request in seconds.

    Returns:
        :class:`InstanceMapping` with :attr:`edges`, :attr:`match_results`,
        and provenance :attr:`about`.

    Raises:
        ValueError: If *prefix* is unknown to bioregistry.
    """
    uri_formats = _get_uri_formats(prefix)
    if not uri_formats:
        logger.warning(
            "Bioregistry prefix %r has no URI formats — no probes to run.",
            prefix,
        )

    # Filter datasources
    df = datasources.copy()
    if dataset_names:
        df = df[df["dataset_name"].isin(dataset_names)]

    required_cols = {"dataset_name", "endpoint_url"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"datasources DataFrame is missing columns: {missing}. "
            f"Available: {list(df.columns)}"
        )

    # Probe each dataset
    all_results: list[InstanceMatchResult] = []
    for _, row in df.iterrows():
        dataset = str(row["dataset_name"])
        endpoint = str(row["endpoint_url"])
        if not endpoint:
            logger.info("Skipping %s: no endpoint_url", dataset)
            continue
        logger.info(
            "── Probing dataset=%s  endpoint=%s  (%d uri formats)",
            dataset, endpoint, len(uri_formats),
        )
        results = _probe_dataset(dataset, endpoint, uri_formats, timeout)
        logger.info(
            "   dataset=%s  total hits=%d", dataset, len(results),
        )
        all_results.extend(results)

    # Build cross-dataset edges
    edges = _build_edges(all_results, predicate)

    logger.info(
        "probe_resource(%r): %d hits, %d edges generated",
        prefix, len(all_results), len(edges),
    )

    about = AboutMetadata.build(
        dataset_name=f"{prefix}_instance_mapping",
        strategy="instance_matcher",
        pattern_count=len(edges),
    )

    return InstanceMapping(
        edges=edges,
        about=about,
        resource_prefix=prefix,
        uri_formats=uri_formats,
        match_results=all_results,
    )
