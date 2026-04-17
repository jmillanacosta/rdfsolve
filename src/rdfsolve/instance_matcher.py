"""Instance-based matching: probe SPARQL endpoints for bioregistry URI patterns.

Given a bioregistry resource prefix (e.g. ``"ensembl"``), this module
queries every rdfsolve data source for the RDF classes whose instances
match the resource's known URI prefixes.  When two datasets both contain
instances of the same resource, a mapping edge is emitted between their
respective classes.

The result is an :class:`~rdfsolve.mapping_models.instance.InstanceMapping`
that can be
serialised to JSON-LD and imported into the rdfsolve database alongside
mined schemas.  The JSON-LD format is identical to a mined schema's, so
the frontend ``parseJSONLD`` pipeline works without any changes -
``skos:narrowMatch`` edges become walkable graph edges in the UI.

Typical usage::

    from rdfsolve.sources import load_sources_dataframe
    from rdfsolve.instance_matcher import probe_resource

    datasources = load_sources_dataframe()
    mapping = probe_resource("ensembl", datasources)
    jsonld = mapping.to_jsonld()
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from rdfsolve.mapping_models import (
    SKOS_NARROW_MATCH,
    AboutMetadata,
    InstanceMapping,
    InstanceMatchResult,
    MappingEdge,
)
from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = [
    "discover_mapping_prefixes",
    "probe_resource",
    "seed_instance_mappings",
]


# ──────────────────────────────────────────────────────────────────────────────
# Prefix discovery from mapping files
# ──────────────────────────────────────────────────────────────────────────────

# Prefixes that are structural/metadata and should not be treated as entity
# namespaces worth probing in instance-matching.
_STRUCTURAL_PREFIXES: frozenset[str] = frozenset({
    "rdfsolve", "void", "dcterms", "foaf", "skos", "sd",
    "rdf", "rdfs", "owl", "xsd", "sh", "shacl",
    "prov", "dcat", "schema",
})


def _extract_entity_prefixes_from_jsonld(
    data: dict[str, Any],
) -> set[str]:
    """Extract unique entity CURIE prefixes from a mapping JSON-LD document.

    Walks the ``@graph`` array and collects the CURIE prefix part of every
    ``@id`` that looks like ``prefix:localname`` (i.e. not a full URI and
    not a structural/metadata prefix).

    Args:
        data: Parsed JSON-LD dict with ``@context`` and ``@graph``.

    Returns:
        Set of bioregistry-style prefix strings (e.g. ``{'mesh', 'chebi'}``).
    """
    prefixes: set[str] = set()

    def _maybe_add(iri: str) -> None:
        if not iri or iri.startswith(("http://", "https://", "urn:", "_:")):
            return
        if ":" not in iri:
            return
        pfx = iri.split(":", 1)[0]
        if pfx and pfx not in _STRUCTURAL_PREFIXES:
            prefixes.add(pfx)

    for node in data.get("@graph", []):
        _maybe_add(node.get("@id", ""))
        for _key, val in node.items():
            if _key.startswith("@") or _key in ("void:inDataset", "dcterms:created"):
                continue
            targets = val if isinstance(val, list) else [val]
            for tgt in targets:
                if isinstance(tgt, dict):
                    _maybe_add(tgt.get("@id", ""))
                elif isinstance(tgt, str):
                    _maybe_add(tgt)

    return prefixes


def discover_mapping_prefixes(
    *mapping_dirs: str,
    glob_pattern: str = "*.jsonld",
) -> list[str]:
    """Discover all unique bioregistry prefixes from mapping JSON-LD files.

    Scans one or more directories for JSON-LD files and extracts the set
    of entity CURIE prefixes used in ``@graph`` entries.  Prefixes that
    are purely structural (``void``, ``dcterms``, …) are excluded.

    The returned list is sorted and deduplicated.  Each entry is a
    bioregistry-compatible prefix string (e.g. ``"mesh"``, ``"chebi"``).

    Typical usage::

        prefixes = discover_mapping_prefixes(
            "output/mappings/sssom",
            "output/mappings/semra",
            "output/mappings/instance_matching",
        )

    Args:
        mapping_dirs: One or more directory paths to scan.
        glob_pattern: Glob pattern for JSON-LD files (default ``*.jsonld``).

    Returns:
        Sorted list of unique entity prefixes found across all files.
    """
    import json as _json
    from pathlib import Path as _Path

    all_prefixes: set[str] = set()
    files_scanned = 0

    for dir_str in mapping_dirs:
        dir_path = _Path(dir_str)
        if not dir_path.is_dir():
            logger.warning("discover_mapping_prefixes: %s is not a directory, skipping", dir_str)
            continue
        for jsonld_file in sorted(dir_path.rglob(glob_pattern)):
            try:
                data = _json.loads(jsonld_file.read_text(encoding="utf-8"))
                pfxs = _extract_entity_prefixes_from_jsonld(data)
                all_prefixes.update(pfxs)
                files_scanned += 1
            except Exception as exc:
                logger.warning("discover_mapping_prefixes: skipping %s: %s", jsonld_file, exc)

    # Validate prefixes against bioregistry - keep only those that resolve
    valid_prefixes: list[str] = []
    try:
        import bioregistry
        for pfx in sorted(all_prefixes):
            resource = bioregistry.get_resource(pfx)
            if resource is not None:
                valid_prefixes.append(pfx)
            else:
                logger.debug("Prefix %r not in bioregistry, skipping", pfx)
    except ImportError:
        logger.warning("bioregistry not installed; returning all prefixes unvalidated")
        valid_prefixes = sorted(all_prefixes)

    logger.info(
        "discover_mapping_prefixes: scanned %d files across %d dirs -> %d valid prefixes (from %d raw)",
        files_scanned,
        len(mapping_dirs),
        len(valid_prefixes),
        len(all_prefixes),
    )
    return valid_prefixes


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
            dataset_name,
            endpoint_url,
            exc,
        )
        return results

    for uri_format in uri_formats:
        logger.info(
            "Probing  dataset=%-20s  endpoint=%s  pattern=%s",
            dataset_name,
            endpoint_url,
            uri_format,
        )
        try:
            classes = sparql.find_classes_for_uri_pattern(uri_format)
        except Exception as exc:
            logger.warning(
                "Probe failed - dataset=%s format=%s: %s",
                dataset_name,
                uri_format,
                exc,
            )
            continue

        if classes:
            logger.info(
                "  -> %d hit(s): %s",
                len(classes),
                ", ".join(classes),
            )
        else:
            logger.debug("  -> no hits")

        for cls_uri in classes:
            results.append(
                InstanceMatchResult(
                    dataset_name=dataset_name,
                    endpoint_url=endpoint_url,
                    uri_format=uri_format,
                    matched_class=cls_uri,
                )
            )

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
    seen: set[tuple[str, str | None, str, str | None]] = set()

    for i, a in enumerate(hits):
        for b in hits[i + 1 :]:
            # Skip exact duplicates (same dataset AND same class)
            if a.dataset_name == b.dataset_name and a.matched_class == b.matched_class:
                continue
            # Canonicalise order so (A->B) and (B->A) count as one edge
            src, tgt = (
                (a, b)
                if ((a.dataset_name, a.matched_class) <= (b.dataset_name, b.matched_class))
                else (b, a)
            )
            key = (
                src.dataset_name,
                src.matched_class,
                tgt.dataset_name,
                tgt.matched_class,
            )
            if key in seen:
                continue
            seen.add(key)
            edges.append(
                MappingEdge(
                    source_class=src.matched_class,
                    target_class=tgt.matched_class,
                    predicate=predicate,
                    source_dataset=src.dataset_name,
                    target_dataset=tgt.dataset_name,
                    source_endpoint=src.endpoint_url,
                    target_endpoint=tgt.endpoint_url,
                    source_uri_format=src.uri_format,
                    target_uri_format=tgt.uri_format,
                )
            )

    return edges


def probe_resource(
    prefix: str,
    datasources: pd.DataFrame,
    predicate: str = SKOS_NARROW_MATCH,
    dataset_names: list[str] | None = None,
    timeout: float = 60.0,
) -> InstanceMapping:
    """Probe SPARQL endpoints for a bioregistry resource.

    Steps:

    1. Resolve URI format prefixes for *prefix* via bioregistry.
    2. Optionally filter *datasources* to *dataset_names*.
    3. For each dataset, query its endpoint with each URI prefix using
       ``STRSTARTS``-based ``SELECT DISTINCT ?c``.
    4. Build pairwise :class:`MappingEdge` instances between any two
       **distinct classes** that both matched the resource - including
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
            "Bioregistry prefix %r has no URI formats- no probes to run.",
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
            f"datasources DataFrame is missing columns: {missing}. Available: {list(df.columns)}"
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
            dataset,
            endpoint,
            len(uri_formats),
        )
        results = _probe_dataset(dataset, endpoint, uri_formats, timeout)
        logger.info(
            "   dataset=%s  total hits=%d",
            dataset,
            len(results),
        )
        all_results.extend(results)

    # Build cross-dataset edges
    edges = _build_edges(all_results, predicate)

    logger.info(
        "probe_resource(%r): %d hits, %d edges generated",
        prefix,
        len(all_results),
        len(edges),
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


def seed_instance_mappings(
    prefixes: list[str],
    sources_csv: str | None = None,
    *,
    sources: str | None = None,
    output_dir: str = "docker/mappings/instance_matching",
    predicate: str = "http://www.w3.org/2004/02/skos/core#narrowMatch",
    dataset_names: list[str] | None = None,
    timeout: float = 60.0,
    skip_existing: bool = False,
    ports_json: str | None = None,
) -> dict[str, Any]:
    """Probe multiple bioregistry resources and write mapping JSON-LD files.

    Iterates over *prefixes*, runs :func:`probe_resource` for each, and
    writes the result to ``{output_dir}/{prefix}_instance_mapping.jsonld``.

    When a file already exists on disk the new probe results are **merged**
    into it rather than overwriting it.

    Args:
        prefixes: List of bioregistry prefixes to process.
        sources_csv: **Deprecated** - use *sources* instead.
        sources: Path to the sources file (JSON-LD or CSV).
        output_dir: Directory where JSON-LD files are written.
        predicate: Mapping predicate URI.
        dataset_names: Restrict probing to these dataset names.
        timeout: SPARQL request timeout per request.
        skip_existing: If ``True``, skip prefixes whose output file
            already exists without re-probing.
        ports_json: Path to QLever ``ports.json`` mapping
            ``{dataset_name: port}``.  When supplied, queries go to
            local QLever (``http://localhost:{port}``) instead of the
            remote endpoints in ``sources.yaml``.

    Returns:
        Summary dict: ``{"succeeded": [...], "failed": [...]}``.
    """
    import json as _json
    from pathlib import Path as _Path

    from rdfsolve.mapping_models.instance import merge_instance_jsonld
    from rdfsolve.sources import load_sources_dataframe

    out = _Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    src_path = sources or sources_csv or None
    datasources = load_sources_dataframe(src_path, ports_json=ports_json)

    succeeded: list[str] = []
    failed: list[dict[str, str]] = []

    for prefix in prefixes:
        logger.info("Querying prefix: %s", prefix)
        outfile = out / f"{prefix}_instance_mapping.jsonld"

        if skip_existing and outfile.exists():
            logger.info(
                "Skipping %s: already exists at %s (skip_existing=True)",
                prefix,
                outfile,
            )
            succeeded.append(prefix)
            continue

        try:
            mapping = probe_resource(
                prefix=prefix,
                datasources=datasources,
                predicate=predicate,
                dataset_names=dataset_names,
                timeout=timeout,
            )
            new_jsonld = mapping.to_jsonld()

            if outfile.exists():
                try:
                    existing_jsonld = _json.loads(outfile.read_text())
                    merged = merge_instance_jsonld(existing_jsonld, new_jsonld)
                    outfile.write_text(_json.dumps(merged, indent=2))
                    logger.info("Merged into existing: %s", outfile)
                except Exception as merge_exc:
                    logger.warning(
                        "Could not merge into %s (%s); overwriting.",
                        outfile,
                        merge_exc,
                    )
                    outfile.write_text(_json.dumps(new_jsonld, indent=2))
                    logger.info("Overwritten: %s", outfile)
            else:
                outfile.write_text(_json.dumps(new_jsonld, indent=2))
                logger.info("Written: %s", outfile)

            succeeded.append(prefix)
        except Exception as exc:
            logger.error("Failed %s: %s", prefix, exc)
            failed.append({"prefix": prefix, "error": str(exc)})

    return {"succeeded": succeeded, "failed": failed}
