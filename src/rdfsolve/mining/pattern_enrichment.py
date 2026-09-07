"""Pattern enrichment functions for adding counts and labels."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from rdfsolve._outcomes import QueryFailure, QueryOutcome
from rdfsolve._uri import get_local_name, pick_label
from rdfsolve.mining.query_builders import (
    _build_batched_literal_count_query,
    _build_batched_typed_count_query,
    _build_batched_untyped_count_query,
    _build_label_query,
    _graph_clause,
)
from rdfsolve.mining.query_fallbacks import query_with_bisect, select_outcome
from rdfsolve.models import SchemaPattern

if TYPE_CHECKING:
    from collections.abc import Callable

    from rdfsolve.mining.report_tracking import ReportCollector
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = [
    "enrich_patterns_with_counts",
    "enrich_patterns_with_labels",
]


def query_class_entity_counts(
    classes: list[str],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    report: ReportCollector,
    batch_size: int = 50,
    delay: float = 0,
) -> dict[str, int]:
    """Count distinct typed entities, not overlapping pattern rows."""
    counts: dict[str, int] = {}
    if batch_size < 1:
        raise ValueError("Class count batch size must be positive")
    batch_size = min(batch_size, 10)
    opening, closing = _graph_clause(graph_uris)
    phase = report.start_phase("class-entity-counts")
    for offset in range(0, len(classes), batch_size):
        batch = classes[offset : offset + batch_size]
        aggregate = "COUNT(DISTINCT ?entity)" if graph_uris and len(graph_uris) > 1 else "COUNT(*)"
        branches = [
            f"{{ SELECT (<{iri}> AS ?class) ({aggregate} AS ?count) WHERE {{ "
            f"{opening} ?entity a <{iri}> . {closing} }} }}"
            for iri in batch
        ]
        query = "SELECT ?class ?count WHERE { " + " UNION ".join(branches) + " }"
        started = time.monotonic()
        outcome = select_outcome(query, "class-entity-counts", helper, batch, graph_uris)
        report.record_query(
            "class-entity-counts", time.monotonic() - started, success=outcome.state == "complete"
        )
        for row in outcome.rows:
            try:
                iri = row["class"]["value"]
                count = int(row["count"]["value"])
                if iri not in batch or count < 0 or iri in counts:
                    raise ValueError("Invalid or duplicate class count")
                counts[iri] = count
            except (KeyError, TypeError, ValueError) as error:
                outcome.state = "partial"
                outcome.failures.append(
                    QueryFailure(
                        "invalid_response", str(error), "class-entity-counts", batch, graph_uris
                    )
                )
        missing = sorted(set(batch) - counts.keys())
        if missing:
            outcome.state = "partial" if outcome.rows else "failed"
            outcome.failures.append(
                QueryFailure(
                    "invalid_response",
                    "Missing class counts",
                    "class-entity-counts",
                    missing,
                    graph_uris,
                )
            )
        report.record_outcome(outcome)
        if delay and offset + batch_size < len(classes):
            time.sleep(delay)
    report.finish_phase(phase, items=len(counts))
    return counts


def enrich_patterns_with_counts(
    patterns: list[SchemaPattern],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    report: ReportCollector,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    class_batch_size: int,
    class_chunk_size: int | None,
    unsafe_paging: bool,
    delay: float,
) -> list[SchemaPattern]:
    """Run COUNT queries and merge counts into patterns.

    Count queries use the same batched VALUES / bisect infrastructure
    as the pattern queries so that they remain feasible on large
    endpoints.  Each query type (typed-object, literal, untyped-URI)
    is run per class batch; failures are handled per-batch (logged
    and skipped) rather than aborting all counts.

    Args:
        patterns: Patterns to enrich with counts
        helper: SPARQL helper for query execution
        graph_uris: Named graphs to restrict queries to
        report: Report collector for tracking query execution
        collect_bindings: Function to collect paginated results
        class_batch_size: Number of classes per batch
        class_chunk_size: Page size for class pagination
        unsafe_paging: Drop DISTINCT for faster paging
        delay: Delay between batches (seconds)

    Returns:
        Patterns with count field populated
    """
    # Collect unique subject classes from already-mined patterns
    subject_classes = sorted({p.subject_class for p in patterns})
    if not subject_classes:
        return patterns

    bs = class_batch_size
    total = len(subject_classes)
    n_batches = (total + bs - 1) // bs
    logger.info(
        "Counting phase: %d classes in %d batches of ≤%d …",
        total,
        n_batches,
        bs,
    )

    # Build lookup: (sc, p, oc) -> count
    counts: dict[tuple[str, str, str], int] = {}

    for batch_idx in range(n_batches):
        start = batch_idx * bs
        batch = subject_classes[start : start + bs]
        label = f"batch {batch_idx + 1}/{n_batches}"

        _fetch_typed_count_batch(
            batch,
            label,
            counts,
            helper,
            graph_uris,
            collect_bindings,
            report,
            class_chunk_size or 10000,
            unsafe_paging,
        )
        _fetch_literal_count_batch(
            batch,
            label,
            counts,
            helper,
            graph_uris,
            collect_bindings,
            report,
            class_chunk_size or 10000,
            unsafe_paging,
        )
        _fetch_untyped_count_batch(
            batch,
            label,
            counts,
            helper,
            graph_uris,
            collect_bindings,
            report,
            class_chunk_size or 10000,
            unsafe_paging,
        )

        # Delay between batches
        if delay > 0:
            time.sleep(delay)

    logger.info(
        "Counting phase: collected %d count entries",
        len(counts),
    )

    # Merge counts into patterns
    enriched: list[SchemaPattern] = []
    for pat in patterns:
        if pat.object_class == "Literal":
            dt_key = f"Literal:{pat.datatype}" if pat.datatype else "Literal"
            key = (
                pat.subject_class,
                pat.property_uri,
                dt_key,
            )
        else:
            key = (
                pat.subject_class,
                pat.property_uri,
                pat.object_class,
            )
        cnt = counts.get(key)
        enriched.append(
            pat.model_copy(update={"count": cnt}),
        )

    return enriched


def _fetch_typed_count_batch(
    batch: list[str],
    label: str,
    counts: dict[tuple[str, str, str], int],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    report: ReportCollector,
    chunk_size: int,
    unsafe_paging: bool,
) -> None:
    """Query typed-object counts for one class batch and update *counts*."""
    try:
        t0 = time.monotonic()
        outcome = query_with_bisect(
            batch,
            graph_uris,
            _build_batched_typed_count_query,
            "counts/typed-object",
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
        )
        report.record_outcome(outcome)
        report.record_query(
            "counts/typed-object",
            time.monotonic() - t0,
            success=outcome.state == "complete",
        )
        for b in outcome.rows:
            key = (
                b.get("class", {}).get("value", ""),
                b.get("p", {}).get("value", ""),
                b.get("oc", {}).get("value", ""),
            )
            cnt = b.get("cnt", {}).get("value")
            if cnt:
                counts[key] = int(cnt)
    except (ValueError, TypeError) as e:
        report.record_outcome(
            QueryOutcome(
                state="failed",
                failures=[
                    QueryFailure(
                        "invalid_response", str(e), "counts/typed-object", batch, graph_uris
                    )
                ],
            )
        )
        logger.warning(
            "Typed-object count query failed (%s): %s",
            label,
            e,
        )


def _fetch_literal_count_batch(
    batch: list[str],
    label: str,
    counts: dict[tuple[str, str, str], int],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    report: ReportCollector,
    chunk_size: int,
    unsafe_paging: bool,
) -> None:
    """Query literal counts for one class batch and update *counts*."""
    try:
        t0 = time.monotonic()
        outcome = query_with_bisect(
            batch,
            graph_uris,
            _build_batched_literal_count_query,
            "counts/literal",
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
        )
        report.record_outcome(outcome)
        report.record_query(
            "counts/literal",
            time.monotonic() - t0,
            success=outcome.state == "complete",
        )
        for b in outcome.rows:
            dt = b.get("dt", {}).get("value", "")
            key = (
                b.get("class", {}).get("value", ""),
                b.get("p", {}).get("value", ""),
                f"Literal:{dt}" if dt else "Literal",
            )
            cnt = b.get("cnt", {}).get("value")
            if cnt:
                counts[key] = int(cnt)
    except (ValueError, TypeError) as e:
        report.record_outcome(
            QueryOutcome(
                state="failed",
                failures=[
                    QueryFailure("invalid_response", str(e), "counts/literal", batch, graph_uris)
                ],
            )
        )
        logger.warning(
            "Literal count query failed (%s): %s",
            label,
            e,
        )


def _fetch_untyped_count_batch(
    batch: list[str],
    label: str,
    counts: dict[tuple[str, str, str], int],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    report: ReportCollector,
    chunk_size: int,
    unsafe_paging: bool,
) -> None:
    """Query untyped-URI counts for one class batch and update *counts*."""
    try:
        t0 = time.monotonic()
        outcome = query_with_bisect(
            batch,
            graph_uris,
            _build_batched_untyped_count_query,
            "counts/untyped-uri",
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
        )
        report.record_outcome(outcome)
        report.record_query(
            "counts/untyped-uri",
            time.monotonic() - t0,
            success=outcome.state == "complete",
        )
        for b in outcome.rows:
            key = (
                b.get("class", {}).get("value", ""),
                b.get("p", {}).get("value", ""),
                "Resource",
            )
            cnt = b.get("cnt", {}).get("value")
            if cnt:
                counts[key] = int(cnt)
    except (ValueError, TypeError) as e:
        report.record_outcome(
            QueryOutcome(
                state="failed",
                failures=[
                    QueryFailure(
                        "invalid_response", str(e), "counts/untyped-uri", batch, graph_uris
                    )
                ],
            )
        )
        logger.warning(
            "Untyped-URI count query failed (%s): %s",
            label,
            e,
        )


def enrich_patterns_with_labels(
    patterns: list[SchemaPattern],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    report: ReportCollector,
) -> list[SchemaPattern]:
    """Fetch rdfs:label / dc:title for all URIs in patterns.

    URIs are queried in batches (max 50 per request) to avoid
    HTTP 414 URI-too-long errors on endpoints that reject large
    GET query strings.

    Args:
        patterns: Patterns to enrich with labels
        helper: SPARQL helper for query execution
        graph_uris: Named graphs to restrict queries to
        report: Report collector for tracking query execution

    Returns:
        Patterns with labels populated
    """
    # Collect all unique URIs that need labels
    all_uris: set[str] = set()
    for pat in patterns:
        all_uris.add(pat.subject_class)
        all_uris.add(pat.property_uri)
        if pat.object_class not in ("Literal", "Resource"):
            all_uris.add(pat.object_class)

    if not all_uris:
        return patterns

    # Fetch labels in batches to keep query size small
    label_map: dict[str, str] = {}
    batch_size = 50
    uri_list = sorted(all_uris)

    for start in range(0, len(uri_list), batch_size):
        batch = uri_list[start : start + batch_size]
        _fetch_label_batch(batch, label_map, helper, graph_uris, report)

    # Fill in labels using local name as fallback
    enriched = _enrich_with_local(patterns, label_map)

    return enriched


def _fetch_label_batch(
    batch: list[str],
    label_map: dict[str, str],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    report: ReportCollector,
) -> None:
    """Query labels for one batch of URIs and update label_map in place.

    Args:
        batch: URIs to look up labels for
        label_map: Mapping that will be updated with {uri: label} entries
        helper: SPARQL helper for query execution
        graph_uris: Named graphs to restrict queries to
        report: Report collector for tracking query execution
    """
    t0 = time.monotonic()
    try:
        q = _build_label_query(batch, graph_uris)
        result = helper.select(q, purpose="labels")
        report.record_query(
            "labels",
            time.monotonic() - t0,
        )
        bindings = result.get("results", {}).get("bindings", [])
        for b in bindings:
            uri = b.get("uri", {}).get("value", "")
            if not uri or uri in label_map:
                continue
            rdfs_lbl = b.get("rdfsLabel", {}).get("value")
            dc_lbl = b.get("dcTitle", {}).get("value")
            iao_lbl = b.get("iaoLabel", {}).get("value")
            skos_pref = b.get("skosPrefLabel", {}).get("value")
            skos_alt = b.get("skosAltLabel", {}).get("value")
            label_map[uri] = pick_label(
                rdfs_lbl,
                dc_lbl,
                uri,
                iao_label=iao_lbl,
                skos_pref_label=skos_pref,
                skos_alt_label=skos_alt,
            )
    except Exception as e:
        report.record_query(
            "labels",
            time.monotonic() - t0,
            success=False,
        )
        logger.warning("Label batch failed (%d URIs) : %s", len(batch), e)


def _enrich_with_local(
    patterns: list[SchemaPattern],
    label_map: dict[str, str],
) -> list[SchemaPattern]:
    """Fill in labels using fetched labels with local name as fallback."""
    enriched: list[SchemaPattern] = []
    for pat in patterns:
        updates: dict[str, Any] = {}
        updates["subject_label"] = label_map.get(
            pat.subject_class,
            get_local_name(pat.subject_class),
        )
        updates["property_label"] = label_map.get(
            pat.property_uri,
            get_local_name(pat.property_uri),
        )
        if pat.object_class in ("Literal", "Resource"):
            updates["object_label"] = pat.object_class
        else:
            updates["object_label"] = label_map.get(
                pat.object_class,
                get_local_name(pat.object_class),
            )
        enriched.append(pat.model_copy(update=updates))
    return enriched
