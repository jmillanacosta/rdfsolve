"""Converter layer between rdfsolve mapping types and semra types.

This module is the **only** place where rdfsolve and semra types meet.
All other rdfsolve modules import from here; they never import semra
directly.

Key functions
-------------
rdfsolve_edges_to_semra
    Convert a list of :class:`~rdfsolve.models.MappingEdge` + provenance
    into ``list[semra.Mapping]``.

semra_to_rdfsolve_edges
    Convert ``list[semra.Mapping]`` back to :class:`MappingEdge` list.

semra_evidence_to_jsonld_about
    Serialise a semra evidence chain into a JSON-LD ``@about`` fragment.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from semra.struct import Mapping as SemraMapping_
    from semra.struct import SimpleEvidence, ReasonedEvidence
    from rdfsolve.models import MappingEdge, AboutMetadata

logger = logging.getLogger(__name__)

__all__ = [
    "PREDICATE_MAP",
    "PREDICATE_MAP_INV",
    "rdfsolve_edges_to_semra",
    "semra_to_rdfsolve_edges",
    "semra_evidence_to_jsonld_about",
]

# ---------------------------------------------------------------------------
# Predicate URI ↔ semra Reference mapping
# ---------------------------------------------------------------------------

def _build_predicate_maps() -> (
    "tuple[dict[str, Any], dict[Any, str]]"
):
    """Build the bidirectional predicate URI ↔ semra Reference map.

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
        "http://www.w3.org/2004/02/skos/core#exactMatch":  EXACT_MATCH,
        "http://www.w3.org/2004/02/skos/core#narrowMatch": NARROW_MATCH,
        "http://www.w3.org/2004/02/skos/core#broadMatch":  BROAD_MATCH,
        "http://www.w3.org/2004/02/skos/core#closeMatch":  CLOSE_MATCH,
        "http://www.w3.org/2004/02/skos/core#related":     DB_XREF,
        "http://www.w3.org/2002/07/owl#equivalentClass":   EQUIVALENT_TO,
        "http://www.w3.org/2002/07/owl#sameAs":            EQUIVALENT_TO,
        "http://www.w3.org/2000/01/rdf-schema#subClassOf": SUBCLASS,
        "http://purl.obolibrary.org/obo/IAO_0100001":      REPLACED_BY,
        "http://www.geneontology.org/formats/oboInOwl#hasDbXref": DB_XREF,
    }
    inv: dict[Any, str] = {}
    # Build inverse: first entry wins for duplicate semra References
    for uri, ref in fwd.items():
        if ref not in inv:
            inv[ref] = uri
    return fwd, inv


# Lazy singletons — populated on first use
_PREDICATE_MAP: "dict[str, Any] | None" = None
_PREDICATE_MAP_INV: "dict[Any, str] | None" = None


def _get_maps() -> "tuple[dict[str, Any], dict[Any, str]]":
    global _PREDICATE_MAP, _PREDICATE_MAP_INV
    if _PREDICATE_MAP is None:
        _PREDICATE_MAP, _PREDICATE_MAP_INV = _build_predicate_maps()
    return _PREDICATE_MAP, _PREDICATE_MAP_INV


@property  # type: ignore[misc]
def PREDICATE_MAP() -> "dict[str, Any]":  # noqa: N802
    return _get_maps()[0]


@property  # type: ignore[misc]
def PREDICATE_MAP_INV() -> "dict[Any, str]":  # noqa: N802
    return _get_maps()[1]


# ---------------------------------------------------------------------------
# Helper: justification from strategy string
# ---------------------------------------------------------------------------

def _strategy_to_justification(strategy: str) -> "Any":
    """Map an rdfsolve strategy identifier to a semra justification Reference."""
    from semra.vocabulary import UNSPECIFIED_MAPPING

    _MAP = {
        "instance_matcher": UNSPECIFIED_MAPPING,
        "semra_import": UNSPECIFIED_MAPPING,
        "inferenced": UNSPECIFIED_MAPPING,  # overridden per-evidence by semra
        "miner": UNSPECIFIED_MAPPING,
    }
    return _MAP.get(strategy, UNSPECIFIED_MAPPING)


# ---------------------------------------------------------------------------
# Helper: URI ↔ bioregistry Reference
# ---------------------------------------------------------------------------

def _uri_to_reference(uri: str) -> "Any | None":
    """Convert a full URI to a semra/pyobo ``Reference``, or ``None``."""
    try:
        import bioregistry
        from pyobo import Reference

        parsed = bioregistry.parse_iri(uri)
        if parsed:
            prefix, identifier = parsed
            return Reference(prefix=prefix, identifier=identifier)
    except Exception:
        pass
    # Fallback: treat everything after last / or # as identifier
    sep = max(uri.rfind("/"), uri.rfind("#"))
    if sep >= 0:
        identifier = uri[sep + 1:]
        namespace = uri[: sep + 1]
        try:
            import bioregistry
            from pyobo import Reference

            prefix = bioregistry.normalize_prefix(
                namespace.rstrip("/#").rsplit("/", 1)[-1]
            ) or "unknown"
            return Reference(prefix=prefix, identifier=identifier)
        except Exception:
            pass
    return None


def _reference_to_uri(ref: "Any") -> str:
    """Convert a semra/pyobo ``Reference`` to a full URI."""
    try:
        import bioregistry

        uri = bioregistry.get_iri(ref.prefix, ref.identifier)
        if uri:
            return uri
    except Exception:
        pass
    # Fallback CURIE
    return f"{ref.prefix}:{ref.identifier}"


def _bioregistry_iri(prefix: str) -> str | None:
    """Return the upstream homepage IRI for *prefix* from bioregistry."""
    try:
        import bioregistry
        return bioregistry.get_homepage(prefix)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public conversion functions
# ---------------------------------------------------------------------------

def rdfsolve_edges_to_semra(
    edges: "list[MappingEdge]",
    about: "AboutMetadata | None" = None,
) -> "list[SemraMapping_]":
    """Convert rdfsolve MappingEdge list to semra Mapping list.

    Each :class:`~rdfsolve.models.MappingEdge` becomes one
    ``semra.Mapping`` with a single ``SimpleEvidence``.  The evidence
    carries:

    * ``justification`` derived from ``about.strategy`` (defaults to
      ``semapv:UnspecifiedMatchingProcess``).
    * ``mapping_set`` whose ``name`` is the source dataset and whose
      ``purl`` is the source endpoint URL (if available).

    Edges whose predicates are unknown to the predicate map are logged
    and skipped.

    Args:
        edges: List of :class:`~rdfsolve.models.MappingEdge` to convert.
        about: Optional provenance metadata; used for justification lookup.

    Returns:
        List of ``semra.Mapping`` objects (may be shorter than *edges* if
        some predicates could not be mapped).
    """
    from semra.struct import Mapping, MappingSet, SimpleEvidence

    fwd, _ = _get_maps()
    strategy = about.strategy if about else "unknown"
    justification = _strategy_to_justification(strategy)

    results: list[SemraMapping_] = []  # type: ignore[type-arg]
    for edge in edges:
        pred_ref = fwd.get(edge.predicate)
        if pred_ref is None:
            logger.debug(
                "rdfsolve_edges_to_semra: unknown predicate %r — skipping",
                edge.predicate,
            )
            continue

        subject = _uri_to_reference(edge.source_class)
        object_ = _uri_to_reference(edge.target_class)
        if subject is None or object_ is None:
            logger.debug(
                "rdfsolve_edges_to_semra: cannot parse URIs %r / %r — skipping",
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
    mappings: "list[SemraMapping_]",
    dataset_hint: str = "semra",
    endpoint_hint: str = "",
) -> "list[MappingEdge]":
    """Convert semra Mapping list _ rdfsolve MappingEdge list.

    Confidence is omitted (left as ``None``) intentionally — see the
    integration plan for discussion of confidence aggregation.

    Args:
        mappings: semra ``Mapping`` objects to convert.
        dataset_hint: Fallback dataset name when evidence doesn't carry one.
        endpoint_hint: Fallback endpoint URL.

    Returns:
        List of :class:`~rdfsolve.models.MappingEdge`.
    """
    from rdfsolve.models import MappingEdge

    _, inv = _get_maps()

    edges: list[MappingEdge] = []
    for mapping in mappings:
        source_uri = _reference_to_uri(mapping.subject)
        target_uri = _reference_to_uri(mapping.object)
        predicate_uri = inv.get(mapping.predicate)
        if predicate_uri is None:
            # Use CURIE as fallback predicate
            predicate_uri = (
                f"http://www.w3.org/2004/02/skos/core#{mapping.predicate.identifier}"
                if mapping.predicate.prefix == "skos"
                else f"{mapping.predicate.prefix}:{mapping.predicate.identifier}"
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
                    source_endpoint = (
                        _bioregistry_iri(source_dataset) or endpoint_hint
                    )
                break

        # Resolve target dataset from the object's prefix (guard against None)
        obj_prefix = (
            getattr(mapping.object, "prefix", None) if mapping.object else None
        )
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
    evidence_list: "list[SimpleEvidence | ReasonedEvidence]",
) -> list[dict[str, Any]]:
    """Serialise a semra evidence chain into a list of JSON-LD dicts.

    Returns a list suitable for embedding in ``@about.evidence``.

    Each ``SimpleEvidence`` becomes::

        {
            "type": "simple",
            "justification": "<prefix>:<identifier>",
            "mapping_set": "<name>",
            "purl": "<purl>"
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
                    m.hexdigest() if hasattr(m, "hexdigest") else str(m)
                    for m in source_mappings
                ],
            }
            cf = getattr(ev, "confidence_factor", None)
            if cf is not None:
                entry["confidence_factor"] = cf
            out.append(entry)
    return out
