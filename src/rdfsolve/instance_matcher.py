"""Probe SPARQL endpoints to discover entity classes across datasets using URI pattern matching."""

from __future__ import annotations

import logging
from typing import Any

from rdfsolve.mapping_models.core import (
    SKOS_NARROW_MATCH,
    InstanceMatchResult,
    MappingEdge,
)
from rdfsolve.mapping_models.instance import InstanceMapping
from rdfsolve.schema_models.core import AboutMetadata
from rdfsolve.sparql_helper import SparqlHelper

_log = logging.getLogger(__name__)

__all__ = ["probe_endpoint", "probe_resource"]


def probe_endpoint(
    endpoint_url: str,
    uri_prefix: str,
    *,
    timeout: float = 60.0,
    limit: int = 100,
) -> InstanceMatchResult | None:
    """Probe a single endpoint for entities matching a URI prefix.

    Parameters
    ----------
    endpoint_url
        SPARQL endpoint URL.
    uri_prefix
        URI prefix to match (e.g., "http://identifiers.org/ncbigene/").
    timeout
        Query timeout in seconds.
    limit
        Maximum number of results.

    Returns
    -------
    InstanceMatchResult or None
        Match result with discovered class, or None on error.
    """
    helper = SparqlHelper(endpoint_url=endpoint_url, timeout=timeout)

    # Query for entities matching the URI prefix and their types
    query = f"""
    SELECT DISTINCT ?class (COUNT(?s) as ?count)
    WHERE {{
        ?s a ?class .
        FILTER(STRSTARTS(STR(?s), "{uri_prefix}"))
    }}
    GROUP BY ?class
    ORDER BY DESC(?count)
    LIMIT {limit}
    """

    try:
        result = helper.select(query, purpose="probe_resource")
        bindings = result.get("results", {}).get("bindings", [])

        if bindings:
            # Return the most common class
            top_class = bindings[0].get("class", {}).get("value")
            return InstanceMatchResult(
                dataset_name=endpoint_url,
                endpoint_url=endpoint_url,
                uri_format=uri_prefix,
                matched_class=top_class,
            )
        return InstanceMatchResult(
            dataset_name=endpoint_url,
            endpoint_url=endpoint_url,
            uri_format=uri_prefix,
            matched_class=None,
        )
    except Exception as e:
        _log.debug("Probe failed for %s at %s: %s", uri_prefix, endpoint_url, e)
        return None


def probe_resource(
    prefix: str,
    datasources: Any,
    *,
    predicate: str = SKOS_NARROW_MATCH,
    dataset_names: list[str] | None = None,
    timeout: float = 60.0,
) -> InstanceMapping:
    """Probe all endpoints for a bioregistry resource.

    Parameters
    ----------
    prefix
        Bioregistry prefix (e.g., "ensembl").
    datasources
        DataFrame with columns [dataset_name, endpoint_url].
    predicate
        Mapping predicate URI.
    dataset_names
        Optional subset of datasets to query.
    timeout
        SPARQL request timeout in seconds.

    Returns
    -------
    InstanceMapping
        Mapping with discovered edges.
    """
    try:
        import bioregistry
    except ImportError:
        _log.warning("bioregistry not installed; cannot resolve URI formats")
        return _empty_mapping(prefix)

    # Get URI formats for this prefix
    resource = bioregistry.get_resource(prefix)
    if not resource:
        _log.warning("Unknown bioregistry prefix: %s", prefix)
        return _empty_mapping(prefix)

    uri_prefixes = _get_uri_prefixes(resource, prefix)
    if not uri_prefixes:
        _log.warning("No URI formats found for prefix: %s", prefix)
        return _empty_mapping(prefix)

    # Probe endpoints
    match_results: list[InstanceMatchResult] = []
    edges: list[MappingEdge] = []

    for _, row in datasources.iterrows():
        ds_name = row.get("dataset_name", row.get("name", ""))
        endpoint = row.get("endpoint_url", row.get("endpoint", ""))

        if dataset_names and ds_name not in dataset_names:
            continue

        for uri_prefix in uri_prefixes:
            result = probe_endpoint(
                endpoint_url=endpoint,
                uri_prefix=uri_prefix,
                timeout=timeout,
            )
            if result:
                result.dataset_name = ds_name
                match_results.append(result)

                if result.matched_class:
                    # Create edges between all matching classes
                    for other_result in match_results:
                        if other_result.matched_class and other_result.dataset_name != ds_name:
                            edges.append(
                                MappingEdge(
                                    source_class=result.matched_class,
                                    target_class=other_result.matched_class,
                                    predicate=predicate,
                                    source_dataset=ds_name,
                                    target_dataset=other_result.dataset_name,
                                    source_endpoint=endpoint,
                                    target_endpoint=other_result.endpoint_url,
                                    source_uri_format=uri_prefix,
                                    target_uri_format=other_result.uri_format,
                                )
                            )

    about = AboutMetadata.build(
        dataset_name=f"{prefix}_instance_mapping",
        pattern_count=len(edges),
        strategy="instance_matcher",
    )

    return InstanceMapping(
        edges=edges,
        about=about,
        resource_prefix=prefix,
        uri_formats=uri_prefixes,
        match_results=match_results,
    )


def _get_uri_prefixes(resource: Any, prefix: str) -> list[str]:
    """Extract URI prefixes from a bioregistry resource."""
    prefixes: list[str] = []

    # Standard URI prefix
    uri_prefix = getattr(resource, "get_uri_prefix", lambda: None)()
    if uri_prefix:
        prefixes.append(uri_prefix)

    # identifiers.org pattern
    prefixes.append(f"http://identifiers.org/{prefix}/")
    prefixes.append(f"https://identifiers.org/{prefix}/")

    return list(set(prefixes))


def _empty_mapping(prefix: str) -> InstanceMapping:
    """Create an empty InstanceMapping."""
    about = AboutMetadata.build(
        dataset_name=f"{prefix}_instance_mapping",
        pattern_count=0,
        strategy="instance_matcher",
    )
    return InstanceMapping(
        edges=[],
        about=about,
        resource_prefix=prefix,
        uri_formats=[],
        match_results=[],
    )
