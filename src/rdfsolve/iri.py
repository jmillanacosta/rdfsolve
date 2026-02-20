"""IRI resolution — pure-library module (no Flask dependency).

Resolves IRIs against SPARQL endpoints to discover their ``rdf:type``
and human-readable labels (``rdfs:label``, ``dc:title``).

Uses :class:`~rdfsolve.sparql_helper.SparqlHelper` for all HTTP
communication instead of raw ``requests``.
"""

from __future__ import annotations

import logging
from typing import Any

from rdfsolve.sparql_helper import SparqlHelper
from rdfsolve.utils import normalize_label, pick_label

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────


def _validate_iris(iris: list[str]) -> list[str]:
    """Only allow valid HTTP(S) IRIs."""
    return [
        iri for iri in iris
        if iri.startswith(("http://", "https://"))
    ]


# ── Public function ──────────────────────────────────────────────


def resolve_iris(
    iris: list[str],
    endpoints: list[dict[str, Any]],
    timeout: int = 15,
) -> dict[str, Any]:
    """Resolve IRIs against SPARQL endpoints to find their ``rdf:type``.

    Parameters
    ----------
    iris:
        List of IRI strings to look up.
    endpoints:
        Each dict must have ``endpoint`` (URL) and may have ``name``
        and ``graph``.
    timeout:
        Per-endpoint timeout in seconds.

    Returns
    -------
    dict
        ``{"resolved": {...}, "not_found": [...], "errors": [...]}``
    """
    safe_iris = _validate_iris(iris)
    resolved: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []

    for ep in endpoints:
        ep_name = ep.get("name", "unknown")
        ep_url = ep["endpoint"]
        graph = ep.get("graph")

        try:
            bindings = _query_endpoint(
                ep_url, graph, safe_iris, timeout,
            )
        except Exception as exc:
            logger.warning(
                "IRI resolution failed for %s: %s", ep_url, exc,
            )
            errors.append({
                "endpoint": ep_url,
                "dataset": ep_name,
                "error": str(exc),
            })
            continue

        for binding in bindings:
            iri = binding["iri"]
            rdf_type = binding["type"]
            label = binding.get("label")

            if iri not in resolved:
                resolved[iri] = {
                    "types": [],
                    "found_in": [],
                    "label": None,
                }

            if rdf_type not in resolved[iri]["types"]:
                resolved[iri]["types"].append(rdf_type)

            # Keep the first non-empty label we find
            if label and not resolved[iri]["label"]:
                resolved[iri]["label"] = label

            existing = next(
                (
                    f for f in resolved[iri]["found_in"]
                    if f["endpoint"] == ep_url
                ),
                None,
            )
            if existing:
                if rdf_type not in existing["types"]:
                    existing["types"].append(rdf_type)
            else:
                resolved[iri]["found_in"].append({
                    "dataset": ep_name,
                    "endpoint": ep_url,
                    "graph": graph,
                    "types": [rdf_type],
                })

    not_found = [iri for iri in safe_iris if iri not in resolved]

    return {
        "resolved": resolved,
        "not_found": not_found,
        "errors": errors,
    }


# ── Private helpers ──────────────────────────────────────────────


def _query_endpoint(
    endpoint: str,
    graph: str | None,
    iris: list[str],
    timeout: int,
) -> list[dict[str, str]]:
    """Send a VALUES-based type query using :class:`SparqlHelper`.

    Also fetches ``rdfs:label`` and ``dc:title`` with OPTIONAL
    clauses, resolving the best label via :func:`pick_label`.
    """
    values = " ".join(f"<{iri}>" for iri in iris)

    label_clause = (
        "OPTIONAL { ?iri rdfs:label ?_rdfsLabel . }\n"
        "OPTIONAL { ?iri dc:title ?_dcTitle . }\n"
    )
    prefixes = (
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
        "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
    )

    if graph:
        query = (
            f"{prefixes}"
            "SELECT ?iri ?type ?_rdfsLabel ?_dcTitle WHERE { "
            f"VALUES ?iri {{ {values} }} "
            f"GRAPH <{graph}> {{ ?iri a ?type . "
            f"{label_clause} }} }}"
        )
    else:
        query = (
            f"{prefixes}"
            "SELECT ?iri ?type ?_rdfsLabel ?_dcTitle WHERE { "
            f"VALUES ?iri {{ {values} }} "
            f"?iri a ?type . {label_clause} }}"
        )

    helper = SparqlHelper(endpoint, timeout=float(timeout))
    json_result = helper.select(query, purpose="iri/resolve")

    bindings = json_result.get("results", {}).get("bindings", [])
    results: list[dict[str, str]] = []
    for b in bindings:
        if "iri" not in b or "type" not in b:
            continue
        rdfs_lbl = b.get("_rdfsLabel", {}).get("value")
        dc_lbl = b.get("_dcTitle", {}).get("value")
        label = pick_label(rdfs_lbl, dc_lbl, b["iri"]["value"])
        results.append({
            "iri": b["iri"]["value"],
            "type": b["type"]["value"],
            "label": label,
        })
    return results


# ── Label-based resolution ───────────────────────────────────────


def resolve_by_label(
    label: str,
    endpoints: list[dict[str, Any]],
    timeout: int = 15,
) -> list[dict[str, Any]]:
    """Find IRIs that match a human-readable label.

    Generates normalized variants of *label* via
    :func:`~rdfsolve.utils.normalize_label` and queries each endpoint
    for resources whose ``rdfs:label`` or ``dc:title`` matches any
    variant (case-insensitive FILTER).

    Parameters
    ----------
    label:
        A human-readable string (e.g. ``"Protein"``, ``"homo sapiens"``).
    endpoints:
        Same format as :func:`resolve_iris`.
    timeout:
        Per-endpoint timeout in seconds.

    Returns
    -------
    list[dict]
        Each dict has ``iri``, ``type``, ``label``, ``endpoint``.
    """
    variants = normalize_label(label)
    if not variants:
        return []

    results: list[dict[str, Any]] = []

    for ep in endpoints:
        ep_url = ep["endpoint"]
        graph = ep.get("graph")
        ep_name = ep.get("name", "unknown")

        try:
            hits = _query_by_label(
                ep_url, graph, variants, timeout,
            )
            for h in hits:
                h["endpoint"] = ep_url
                h["dataset"] = ep_name
            results.extend(hits)
        except Exception as exc:
            logger.warning(
                "Label resolution failed for %s: %s",
                ep_url, exc,
            )

    return results


def _query_by_label(
    endpoint: str,
    graph: str | None,
    variants: list[str],
    timeout: int,
) -> list[dict[str, str]]:
    """SPARQL query that matches any of the label variants."""
    # Build a FILTER with LCASE for case-insensitive matching
    lc_values = ", ".join(
        f'"{v.lower()}"' for v in variants
    )
    filter_clause = (
        f"FILTER(LCASE(STR(?label)) IN ({lc_values}))"
    )

    g_open = ""
    g_close = ""
    if graph:
        g_open = f"GRAPH <{graph}> {{"
        g_close = "}"

    query = (
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
        "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
        "SELECT DISTINCT ?iri ?type ?label WHERE {\n"
        f"  {g_open}\n"
        "    {{ ?iri rdfs:label ?label . }}\n"
        "    UNION\n"
        "    {{ ?iri dc:title ?label . }}\n"
        f"    {filter_clause}\n"
        "    ?iri a ?type .\n"
        f"  {g_close}\n"
        "} LIMIT 200"
    )

    helper = SparqlHelper(endpoint, timeout=float(timeout))
    json_result = helper.select(query, purpose="iri/resolve-by-label")
    bindings = (
        json_result.get("results", {}).get("bindings", [])
    )

    results: list[dict[str, str]] = []
    for b in bindings:
        if "iri" not in b or "type" not in b:
            continue
        results.append({
            "iri": b["iri"]["value"],
            "type": b["type"]["value"],
            "label": b.get("label", {}).get("value", ""),
        })
    return results
