"""Converter layer between rdfsolve mapping types and semra types.

This module is the **only** place where rdfsolve and semra types meet.
All other rdfsolve modules import from here; they never import semra
directly.

Key functions
-------------
rdfsolve_edges_to_semra
    Convert a list of :class:`~rdfsolve.mapping_models.core.MappingEdge`
    + provenance into ``list[semra.Mapping]``.

semra_to_rdfsolve_edges
    Convert ``list[semra.Mapping]`` back to
    :class:`~rdfsolve.mapping_models.core.MappingEdge` list.

semra_evidence_to_jsonld_about
    Serialise a semra evidence chain into a JSON-LD ``@about`` fragment.
"""

from __future__ import annotations

import functools
import json as _json
import logging
from typing import TYPE_CHECKING, Any

from bioregistry import (
    get_homepage,
    get_iri,
    get_registry_map,
    get_uri_prefix,
    parse_iri,
)
from pyobo import Reference
from semra.sources.wikidata import get_wikidata_mappings_by_prefix

from rdfsolve._uri import expand_curie_bioregistry
from rdfsolve.mapping_models.core import MappingEdge
from rdfsolve.mapping_models.semra import SemraMapping
from rdfsolve.schema_models.core import AboutMetadata

if TYPE_CHECKING:
    from semra.struct import Mapping as SemraMapping_
    from semra.struct import ReasonedEvidence, SimpleEvidence

logger = logging.getLogger(__name__)

__all__ = [
    "import_source",
    "rdfsolve_edges_to_semra",
    "seed_semra_mappings",
    "semra_evidence_to_jsonld_about",
    "semra_to_rdfsolve_edges",
]

# ---------------------------------------------------------------------------
# Predicate URI <-> semra Reference mapping
# ---------------------------------------------------------------------------


def _build_predicate_maps() -> tuple[dict[str, Any], dict[Any, str]]:
    """Build the bidirectional predicate URI <-> semra Reference map.

    Deferred so semra is only imported when the function is first called.
    """
    from semra.vocabulary import (
        BROAD_MATCH,
        CLOSE_MATCH,
        DB_XREF,
        EQUIVALENT_TO,
        EXACT_MATCH,
        NARROW_MATCH,
        REPLACED_BY,
        SUBCLASS,
    )

    fwd: dict[str, Any] = {
        "http://www.w3.org/2004/02/skos/core#exactMatch": EXACT_MATCH,
        "http://www.w3.org/2004/02/skos/core#narrowMatch": NARROW_MATCH,
        "http://www.w3.org/2004/02/skos/core#broadMatch": BROAD_MATCH,
        "http://www.w3.org/2004/02/skos/core#closeMatch": CLOSE_MATCH,
        "http://www.w3.org/2004/02/skos/core#related": DB_XREF,
        "http://www.w3.org/2002/07/owl#equivalentClass": EQUIVALENT_TO,
        "http://www.w3.org/2002/07/owl#sameAs": EQUIVALENT_TO,
        "http://www.w3.org/2000/01/rdf-schema#subClassOf": SUBCLASS,
        "http://purl.obolibrary.org/obo/IAO_0100001": REPLACED_BY,
        "http://www.geneontology.org/formats/oboInOwl#hasDbXref": DB_XREF,
    }
    inv: dict[Any, str] = {}
    # Build inverse: first entry wins for duplicate semra References
    for uri, ref in fwd.items():
        if ref not in inv:
            inv[ref] = uri
    return fwd, inv


@functools.lru_cache(maxsize=1)
def _get_maps() -> tuple[dict[str, Any], dict[Any, str]]:
    """Return ``(forward, inverse)`` predicate maps, built once."""
    return _build_predicate_maps()


# ---------------------------------------------------------------------------
# Helper: justification from strategy string
# ---------------------------------------------------------------------------


def _strategy_to_justification(strategy: str) -> Any:
    """Map an rdfsolve strategy identifier to a semra justification Reference."""
    from semra.vocabulary import UNSPECIFIED_MAPPING

    map = {
        "instance_matcher": UNSPECIFIED_MAPPING,
        "semra_import": UNSPECIFIED_MAPPING,
        "inferenced": UNSPECIFIED_MAPPING,
        "miner": UNSPECIFIED_MAPPING,
    }
    return map.get(strategy, UNSPECIFIED_MAPPING)


# ---------------------------------------------------------------------------
# Helper: URI <-> bioregistry Reference
# ---------------------------------------------------------------------------


def _uri_to_reference(uri: str) -> Reference | None:
    """Convert a full URI (or CURIE) to a semra/pyobo ``Reference``, or ``None``.

    Strategy:
    1. Expand CURIEs via :func:`expand_curie_bioregistry`.
    2. Try bioregistry.parse_iri for clean canonical prefix resolution.
    3. Fall back to splitting on the last ``#`` or ``/`` and using the
       local fragment as identifier and the namespace tail as prefix -
       no bioregistry call needed, always succeeds for well-formed URIs.

    The fallback is intentionally simple: it preserves the full URI
    information losslessly so the roundtrip through semra does not drop
    any edges.
    """
    # Expand CURIE -> full URI first
    uri = expand_curie_bioregistry(uri)

    # Try bioregistry for clean prefix resolution
    try:
        parsed = parse_iri(uri)
        if parsed:
            prefix, identifier = parsed
            return Reference(prefix=prefix, identifier=identifier)
    except Exception as e:
        logger.warning("URI to Reference error with URI %s: %s", uri, e)
    # Direct split - works for any http(s) URI with a fragment or path local name.
    # Use the namespace tail as a short prefix so the Reference is round-trippable
    # back to the original URI via _reference_to_uri.
    sep = max(uri.rfind("#"), uri.rfind("/"))
    if sep >= 0 and sep < len(uri) - 1:
        identifier = uri[sep + 1 :]
        namespace = uri[: sep + 1]
        # Derive a stable prefix from the namespace (last path component)
        prefix = namespace.rstrip("/#").rsplit("/", 1)[-1].lower() or "unknown"
        try:
            return Reference(prefix=prefix, identifier=identifier)
        except Exception as e:
            logger.warning("Direct split URI to Reference error with URI %s: %s", uri, e)
    return None


def _reference_to_uri(ref: Any) -> str | None:
    """Convert a semra/pyobo ``Reference`` to a full URI.

    Resolution order:
    1. ``bioregistry.get_iri(prefix, identifier)`` - canonical URI.
    2. ``bioregistry.get_uri_prefix(prefix) + identifier`` - namespace expansion.
    3. CURIE string ``prefix:identifier`` - last resort (should not end up
       stored in JSON-LD; callers must warn when this path is taken).
    """
    try:
        uri = get_iri(ref.prefix, ref.identifier)
        if uri:
            return str(uri)
        uri_prefix = get_uri_prefix(ref.prefix)
        if uri_prefix:
            return str(uri_prefix) + str(ref.identifier)
    except Exception:
        logger.debug("Could not convert reference %s to uri", ref.identifier)
    # Last-resort CURIE - callers should log a warning
    return f"{ref.prefix}:{ref.identifier}"


def _bioregistry_iri(prefix: str) -> str | None:
    """Return the upstream homepage IRI for *prefix* from bioregistry."""
    try:
        result = get_homepage(prefix)
        return str(result) if result else None
    except Exception:
        logger.debug("Could not find homepage for %s to uri", prefix)
        return None


# ---------------------------------------------------------------------------
# Public conversion functions
# ---------------------------------------------------------------------------


def rdfsolve_edges_to_semra(
    edges: list[MappingEdge],
    about: AboutMetadata | None = None,
) -> list[SemraMapping_]:
    """Convert rdfsolve MappingEdge list to semra Mapping list.

    Each :class:`~rdfsolve.mapping_models.core.MappingEdge` becomes one
    ``semra.Mapping`` with a single ``SimpleEvidence``.  The evidence
    carries:

    * ``justification`` derived from ``about.strategy`` (defaults to
      ``semapv:UnspecifiedMatchingProcess``).
    * ``mapping_set`` whose ``name`` is the source dataset and whose
      ``purl`` is the source endpoint URL (if available).

    Predicates in the curated map are converted to their canonical semra
    ``Reference``.  Any other predicate URI is parsed directly into a
    ``Reference`` via bioregistry; only edges whose predicate URI cannot be
    resolved at all are dropped (and logged at DEBUG level).

    Args:
        edges: List of :class:`~rdfsolve.mapping_models.core.MappingEdge`
            to convert.
        about: Optional provenance metadata; used for justification lookup.

    Returns:
        List of ``semra.Mapping`` objects.
    """
    from semra.struct import Mapping, MappingSet, SimpleEvidence

    fwd, _ = _get_maps()
    strategy = about.strategy if about else "unknown"
    justification = _strategy_to_justification(strategy)

    results: list[SemraMapping_] = []
    for edge in edges:
        pred_ref = fwd.get(edge.predicate)
        if pred_ref is None:
            # Not in the curated map - construct a Reference directly from the
            # predicate URI so no edge is ever silently dropped.
            pred_ref = _uri_to_reference(edge.predicate)
            if pred_ref is None:
                logger.debug(
                    "rdfsolve_edges_to_semra: cannot parse predicate URI %r - skipping",
                    edge.predicate,
                )
                continue
            logger.debug(
                "rdfsolve_edges_to_semra: predicate %r not in curated map; using raw Reference %r",
                edge.predicate,
                pred_ref,
            )

        subject = _uri_to_reference(edge.source_class)
        object_ = _uri_to_reference(edge.target_class)
        if subject is None or object_ is None:
            logger.debug(
                "rdfsolve_edges_to_semra: cannot parse URIs %r / %r - skipping",
                edge.source_class,
                edge.target_class,
            )
            continue

        mapping_set = MappingSet(
            name=f"{edge.source_dataset}_{edge.target_dataset}",
            purl=edge.source_endpoint or "",
            version="",
            license="",
        )
        evidence = SimpleEvidence(
            justification=justification,
            mapping_set=mapping_set,
            confidence=None,  # TODO
        )
        results.append(
            Mapping(
                subject=subject,
                predicate=pred_ref,
                object=object_,
                evidence=[evidence],
            )
        )
    return results


def semra_to_rdfsolve_edges(
    mappings: list[SemraMapping_],
    dataset_hint: str = "semra",
    endpoint_hint: str = "",
) -> list[MappingEdge] | list[None]:
    """Convert semra Mapping list _ rdfsolve MappingEdge list.

    Confidence is omitted (left as ``None``) intentionally - see the
    integration plan for discussion of confidence aggregation.

    Args:
        mappings: semra ``Mapping`` objects to convert.
        dataset_hint: Fallback dataset name when evidence doesn't carry one.
        endpoint_hint: Fallback endpoint URL.

    Returns:
        List of :class:`~rdfsolve.mapping_models.core.MappingEdge`.
    """
    _, inv = _get_maps()

    edges: list[MappingEdge] = []
    for mapping in mappings:
        source_uri = _reference_to_uri(mapping.subject)
        target_uri = _reference_to_uri(mapping.object)
        predicate_uri = inv.get(mapping.predicate)
        if predicate_uri is None:
            # Not in curated inverse map - reconstruct full URI from the
            # Reference using the same resolution order as _reference_to_uri.
            predicate_uri = _reference_to_uri(mapping.predicate)
            if (
                predicate_uri is not None
                and ":" in predicate_uri
                and not predicate_uri.startswith(("http://", "https://", "urn:"))
            ):
                # _reference_to_uri fell back to a bare CURIE - log it
                logger.warning(
                    "semra_to_rdfsolve_edges: could not resolve predicate "
                    "Reference(%r, %r) to a full URI; stored as CURIE %r",
                    mapping.predicate.prefix,
                    mapping.predicate.identifier,
                    predicate_uri,
                )

        # Extract dataset/endpoint from first SimpleEvidence
        source_dataset = dataset_hint
        source_endpoint = endpoint_hint
        for ev in mapping.evidence:
            ms = getattr(ev, "mapping_set", None)
            if ms is not None:
                source_dataset = getattr(ms, "name", dataset_hint) or dataset_hint
                purl = getattr(ms, "purl", None)
                if purl:
                    source_endpoint = purl
                else:
                    # Fall back to the upstream homepage for the prefix
                    source_endpoint = _bioregistry_iri(source_dataset) or endpoint_hint
                break

        # Resolve target dataset from the object's prefix (guard against None)
        obj_prefix = getattr(mapping.object, "prefix", None) if mapping.object else None
        target_dataset = obj_prefix or source_dataset
        target_endpoint = _bioregistry_iri(target_dataset) or source_endpoint

        edges.append(
            MappingEdge(
                source_class=source_uri,
                target_class=target_uri,
                predicate=predicate_uri,
                source_dataset=source_dataset,
                target_dataset=target_dataset,
                source_endpoint=source_endpoint or None,
                target_endpoint=target_endpoint or None,
                confidence=None,  # deliberately omitted
            )
        )
    return edges


def semra_evidence_to_jsonld_about(
    evidence_list: list[SimpleEvidence | ReasonedEvidence],
) -> list[dict[str, Any]]:
    """Serialise a semra evidence chain into a list of JSON-LD dicts.

    Returns a list suitable for embedding in ``@about.evidence``.

    Each ``SimpleEvidence`` becomes::

        {
            "type": "simple",
            "justification": "<prefix>:<identifier>",
            "mapping_set": "<name>",
            "purl": "<purl>",
        }

    Each ``ReasonedEvidence`` becomes::

        {
            "type": "reasoned",
            "justification": "<prefix>:<identifier>",
            "source_mapping_hexdigests": ["<hex1>", ...],
            "confidence_factor": <float>
        }
    """
    out: list[dict[str, Any]] = []
    for ev in evidence_list:
        ev_type = getattr(ev, "evidence_type", None)
        justification = getattr(ev, "justification", None)
        j_str = (
            f"{justification.prefix}:{justification.identifier}"
            if justification is not None
            else "unknown"
        )

        if ev_type == "simple" or hasattr(ev, "mapping_set"):
            ms = getattr(ev, "mapping_set", None)
            entry: dict[str, Any] = {
                "type": "simple",
                "justification": j_str,
            }
            if ms is not None:
                entry["mapping_set"] = getattr(ms, "name", "")
                purl = getattr(ms, "purl", "")
                if purl:
                    entry["purl"] = purl
            out.append(entry)
        else:
            # ReasonedEvidence
            source_mappings = getattr(ev, "mappings", [])
            entry = {
                "type": "reasoned",
                "justification": j_str,
                "source_mapping_hexdigests": [
                    m.hexdigest() if hasattr(m, "hexdigest") else str(m) for m in source_mappings
                ],
            }
            cf = getattr(ev, "confidence_factor", None)
            if cf is not None:
                entry["confidence_factor"] = cf
            out.append(entry)
    return out


# -------------------------------------------------------------------
# High-level import orchestrator
# -------------------------------------------------------------------


def _build_semra_mapping(
    group: list[SemraMapping_],
    source: str,
    prefix: str,
    mapping_type: str = "instance",
) -> dict[str, Any]:
    """Build a SemraMapping JSON-LD dict from a group of semra Mappings."""
    edges = semra_to_rdfsolve_edges(group, dataset_hint=source)
    evidence_chain: list[dict[str, Any]] = []
    for m in group:
        evidence_chain.extend(
            semra_evidence_to_jsonld_about(m.evidence),
        )
    about = AboutMetadata.build(
        dataset_name=f"{source}_{prefix}_mapping",
        pattern_count=len(edges),
        strategy=mapping_type,
    )
    mapping = SemraMapping(
        edges=edges,
        about=about,
        source_name=source,
        source_prefix=prefix,
        evidence_chain=evidence_chain,
        mapping_type=mapping_type,
    )
    return mapping.to_jsonld()


def import_source(
    source: str,
    keep_prefixes: list[str] | None = None,
    output_dir: str = "docker/mappings/semra",
    mapping_type: str = "instance",
) -> dict[str, Any]:
    """Fetch mappings from a SeMRA source and write JSON-LD files.

    For each unique subject prefix in the fetched mappings, writes
    ``{output_dir}/{source}_{prefix}.jsonld``.

    Handles the Wikidata special case (per-prefix fetch via
    ``get_wikidata_mappings_by_prefix``).

    Args:
        source: SeMRA source key (e.g. ``"biomappings"``).
        keep_prefixes: Optional prefix filter.
        output_dir: Directory for output files.
        mapping_type: ``"instance"`` (default) or ``"class"``.
            Stored in the ``@about.mapping_type`` field of each
            output JSON-LD file.

    Returns:
        Summary dict
        ``{"succeeded": [...], "failed": [...], "skipped": [...]}``.
    """
    import json as _json
    from collections import defaultdict
    from pathlib import Path

    from semra.api import keep_prefixes as _keep_prefixes
    from semra.sources import SOURCE_RESOLVER

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    succeeded: list[str] = []
    failed: list[dict[str, str]] = []

    try:
        logger.info("Fetching semra source: %s", source)

        # ── Wikidata special case ────────────────────────────
        if source.lower() in (
            "wikidata",
            "getwikidatamappings",
        ):
            return _import_wikidata(
                keep_prefixes,
                out,
                succeeded,
                failed,
                mapping_type=mapping_type,
            )

        fn = SOURCE_RESOLVER.lookup(source)
        semra_mappings = fn()
    except Exception as exc:
        logger.error(
            "Failed to load semra source %r: %s",
            source,
            exc,
        )
        return {
            "succeeded": [],
            "failed": [{"source": source, "error": str(exc)}],
            "skipped": [],
        }

    if keep_prefixes:
        semra_mappings = _keep_prefixes(
            semra_mappings,
            keep_prefixes,
        )

    by_prefix: dict[str, list[SemraMapping_]] = defaultdict(list)
    for m in semra_mappings:
        pfx = getattr(m.subject, "prefix", None) or "unknown"
        by_prefix[pfx].append(m)

    logger.info(
        "Source %r: %d mappings across %d prefixes",
        source,
        len(semra_mappings),
        len(by_prefix),
    )

    for prefix, group in sorted(by_prefix.items()):
        outfile = out / f"{source}_{prefix}.jsonld"
        try:
            doc = _build_semra_mapping(group, source, prefix, mapping_type=mapping_type)
            outfile.write_text(
                _json.dumps(doc, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            logger.info(
                "Written: %s (%d edges)",
                outfile,
                len(group),
            )
            succeeded.append(f"{source}_{prefix}")
        except Exception as exc:
            logger.error(
                "Failed %s/%s: %s",
                source,
                prefix,
                exc,
            )
            failed.append(
                {
                    "source": source,
                    "prefix": prefix,
                    "error": str(exc),
                }
            )

    return {
        "succeeded": succeeded,
        "failed": failed,
        "skipped": [],
    }


def _import_wikidata(
    keep_prefixes: list[str] | None,
    out: Any,
    succeeded: list[str],
    failed: list[dict[str, str]],
    mapping_type: str = "instance",
) -> dict[str, Any]:
    """Handle the Wikidata special case for import_source."""
    available = set(
        get_registry_map("wikidata").keys(),
    )
    targets = [p for p in keep_prefixes if p in available] if keep_prefixes else sorted(available)
    if not targets:
        logger.warning(
            "wikidata: none of the requested prefixes have a "
            "Wikidata property mapping. Available: %s",
            sorted(available)[:20],
        )
        return {
            "succeeded": [],
            "failed": [],
            "skipped": ["wikidata"],
        }

    for wd_prefix in targets:
        outfile = out / f"wikidata_{wd_prefix}.jsonld"
        try:
            logger.info(
                "wikidata: fetching prefix %r",
                wd_prefix,
            )
            grp = get_wikidata_mappings_by_prefix(wd_prefix)
            doc = _build_semra_mapping(
                grp,
                "wikidata",
                wd_prefix,
                mapping_type=mapping_type,
            )
            outfile.write_text(
                _json.dumps(
                    doc,
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            logger.info(
                "Written: %s (%d edges)",
                outfile,
                len(grp),
            )
            succeeded.append(f"wikidata_{wd_prefix}")
        except Exception as exc:
            logger.error(
                "Failed wikidata/%s: %s",
                wd_prefix,
                exc,
            )
            failed.append(
                {
                    "source": "wikidata",
                    "prefix": wd_prefix,
                    "error": str(exc),
                }
            )

    return {
        "succeeded": succeeded,
        "failed": failed,
        "skipped": [],
    }


def seed_semra_mappings(
    sources: list[str],
    keep_prefixes: list[str] | None = None,
    output_dir: str = "docker/mappings/semra",
    mapping_type: str = "instance",
) -> dict[str, Any]:
    """Seed semra mapping files for multiple sources.

    Calls :func:`import_source` for each entry in *sources* and
    aggregates the results.

    Args:
        sources: List of SeMRA source keys.
        keep_prefixes: Optional shared prefix filter applied to all sources.
        output_dir: Directory for output files.
        mapping_type: ``"instance"`` (default) or ``"class"``.

    Returns:
        Aggregated summary with keys ``"succeeded"``, ``"failed"``,
        ``"skipped"``.
    """
    succeeded: list[str] = []
    failed: list[dict[str, str]] = []
    skipped: list[str] = []

    for source in sources:
        result = import_source(
            source=source,
            keep_prefixes=keep_prefixes,
            output_dir=output_dir,
            mapping_type=mapping_type,
        )
        succeeded.extend(result.get("succeeded", []))
        failed.extend(result.get("failed", []))
        skipped.extend(result.get("skipped", []))

    return {"succeeded": succeeded, "failed": failed, "skipped": skipped}
