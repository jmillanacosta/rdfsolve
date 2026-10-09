"""Pattern enrichment functions for adding counts and labels."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from rdfsolve._outcomes import QueryFailure, QueryOutcome, QueryState
from rdfsolve._uri import get_local_name, pick_label
from rdfsolve.mining.query_builders import (
    _build_batched_blank_node_count_query,
    _build_batched_literal_count_query,
    _build_batched_literal_objects_query,
    _build_batched_typed_count_query,
    _build_batched_untyped_count_query,
    _build_label_query,
    _graph_clause,
    _graph_scope,
    _subject_type_pattern,
)
from rdfsolve.mining.query_fallbacks import query_with_bisect, select_outcome
from rdfsolve.mining.sampling import refusal, sample_of, sampled_select
from rdfsolve.models import SchemaPattern
from rdfsolve.sparql_helper import EndpointRateLimitError

if TYPE_CHECKING:
    from collections.abc import Callable

    from rdfsolve.mining.report_tracking import ReportCollector
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _PatternCount:
    """One graph-attributed count result for a schema pattern."""

    triples: int
    distinct_subjects: int | None
    distinct_objects: int | None
    # The provenance of the sample that gave the counts (lower bounds), or None.
    sample: dict[str, Any] | None = None


def _count_from_binding(binding: dict[str, Any]) -> _PatternCount | None:
    """Parse one count row without turning malformed metrics into zeros."""
    try:
        triples = int(binding["cnt"]["value"])
    except (KeyError, TypeError, ValueError):
        return None

    def optional_int(name: str) -> int | None:
        """Return an integer binding, or None when it is absent or malformed."""
        value = binding.get(name, {}).get("value")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return _PatternCount(
        triples=triples,
        distinct_subjects=optional_int("subjects"),
        distinct_objects=optional_int("objects"),
        sample=sample_of(binding),
    )


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
    states_out: dict[str, QueryState] | None = None,
    type_context_graph_uris: list[str] | None = None,
) -> dict[str, int]:
    """Count distinct typed entities, not overlapping pattern rows."""
    counts: dict[str, int] = {}
    if batch_size < 1:
        raise ValueError("Class count batch size must be positive")
    batch_size = min(batch_size, 10)
    dataset, _, _ = _graph_scope(graph_uris, type_context_graph_uris)
    phase = report.start_phase("class-entity-counts")
    for offset in range(0, len(classes), batch_size):
        batch = classes[offset : offset + batch_size]
        aggregate = "COUNT(DISTINCT ?entity)"
        branches = [
            f"{{ SELECT (<{iri}> AS ?class) ({aggregate} AS ?count) WHERE {{ "
            f"{_subject_type_pattern('?entity', f'<{iri}>', type_context_graph_uris)} }} }}"
            for iri in batch
        ]
        query = f"SELECT ?class ?count {dataset} WHERE {{ " + " UNION ".join(branches) + " }"
        started = time.monotonic()
        outcome = select_outcome(query, "class-entity-counts", helper, batch, graph_uris)
        if refusal(outcome) is not None:
            outcome = _entity_counts_alone(batch, helper, graph_uris, type_context_graph_uris)
        report.record_query(
            "class-entity-counts", time.monotonic() - started, success=outcome.state == "complete"
        )
        batch_found: set[str] = set()
        for row in outcome.rows:
            try:
                iri = row["class"]["value"]
                count = int(row["count"]["value"])
                if iri not in batch or count < 0 or iri in counts:
                    raise ValueError("Invalid or duplicate class count")
                counts[iri] = count
                batch_found.add(iri)
            except (KeyError, TypeError, ValueError) as error:
                outcome.state = "partial"
                outcome.failures.append(
                    QueryFailure(
                        "invalid_response", str(error), "class-entity-counts", batch, graph_uris
                    )
                )
        missing = sorted(set(batch) - batch_found)
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
        sampled = {row["class"]["value"] for row in outcome.rows if sample_of(row)}
        if states_out is not None:
            for iri in batch_found:
                # A count of a sample is a lower bound (state partial), not a failure.
                states_out[iri] = "partial" if iri in sampled else outcome.state
            for iri in missing:
                states_out[iri] = "failed" if not outcome.rows else "partial"
        report.record_outcome(outcome)
        if delay and offset + batch_size < len(classes):
            time.sleep(delay)
    report.finish_phase(phase, items=len(counts))
    return counts


def _entity_counts_alone(
    batch: list[str],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None,
) -> QueryOutcome:
    """Count the members of each class of a refused batch alone, then over a sample.

    A class whose count is refused alone is counted over its first members (a sub-select with
    LIMIT, rdfsolve.mining.sampling): the count is a lower bound. A class whose samples are
    refused too keeps the refusal.
    """
    dataset, _, _ = _graph_scope(graph_uris, type_context_graph_uris)
    combined = QueryOutcome()
    for iri in batch:
        members = _subject_type_pattern("?entity", f"<{iri}>", type_context_graph_uris)

        def query(limit: int | None, members: str = members, iri: str = iri) -> str:
            """Count the members of the class, or of its first *limit* members."""
            bound = f" LIMIT {limit}" if limit else ""
            return (
                f"SELECT (<{iri}> AS ?class) (COUNT(*) AS ?count) {dataset} WHERE {{ "
                f"{{ SELECT DISTINCT ?entity WHERE {{ {members} }}{bound} }} }}"
            )

        alone = select_outcome(query(None), "class-entity-counts", helper, [iri], graph_uris)
        alone = sampled_select(
            query,
            "class-entity-counts",
            helper,
            alone,
            unit="members",
            classes=[iri],
            graph_uris=graph_uris,
        )
        combined = combined.merge(alone)
    return combined


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
    class_batches: list[list[str]] | None = None,
    type_context_graph_uris: list[str] | None = None,
    shared_extensions: dict[str, str] | None = None,
    resumed: dict[tuple[str, ...], list[dict[str, Any]]] | None = None,
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
        type_context_graph_uris: Extra graphs for subject and object types
        report: Report collector for tracking query execution
        collect_bindings: Function to collect paginated results
        class_batch_size: Number of classes per batch
        class_chunk_size: Page size for class pagination
        unsafe_paging: Drop DISTINCT for faster paging
        delay: Delay between batches (seconds)
        class_batches: Batches planned during pattern mining; subject classes
            they do not cover are counted in fixed batches of *class_batch_size*
        shared_extensions: Classes verified to have the same members as another
            class; their counts are copied from that class instead of queried
        resumed: Checkpointed batches of an earlier run; the counts of a batch kept there
            ("counts" and its classes) are reused instead of queried. The counts of each
            batch are checkpointed.

    Returns:
        Patterns with count field populated
    """
    # The patterns of untyped subjects come counted (rdfsolve.mining.untyped_subjects).
    untyped = [p for p in patterns if p.untyped_subject]
    patterns = [p for p in patterns if not p.untyped_subject]
    # Collect unique subject classes from already-mined patterns
    shared = shared_extensions or {}
    subject_classes = sorted({p.subject_class for p in patterns} - shared.keys())
    if not subject_classes:
        return [*patterns, *untyped]

    bs = class_batch_size
    total = len(subject_classes)
    wanted = set(subject_classes)
    batches = [kept for batch in class_batches or [] if (kept := [c for c in batch if c in wanted])]
    covered = {c for batch in batches for c in batch}
    rest = [c for c in subject_classes if c not in covered]
    batches.extend(rest[i : i + bs] for i in range(0, len(rest), bs))
    n_batches = len(batches)
    logger.info("Counting phase: %d classes in %d batches …", total, n_batches)

    # Build lookup: (sc, p, oc) -> {graph or "" : count}
    counts: dict[tuple[str, str, str], dict[str, _PatternCount]] = {}

    for batch_idx, batch in enumerate(batches):
        label = f"batch {batch_idx + 1}/{n_batches}"
        key = ("counts", *batch)
        if resumed and key in resumed:
            logger.info("Counting %s: %d classes reused from the checkpoint", label, len(batch))
            for row in resumed[key]:
                counts.setdefault(tuple(row["key"]), {})[row["graph"]] = _PatternCount(
                    row["triples"],
                    row["distinct_subjects"],
                    row["distinct_objects"],
                    row.get("sample"),
                )
            continue
        logger.info("Counting %s: %d classes", label, len(batch))
        failed = len(report.report.query_failures)

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
            type_context_graph_uris,
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
            type_context_graph_uris,
        )
        _fetch_resource_count_batch(
            batch,
            label,
            counts,
            helper,
            graph_uris,
            collect_bindings,
            report,
            class_chunk_size or 10000,
            unsafe_paging,
            type_context_graph_uris,
        )

        if any(p.object_class == "BlankNode" and p.subject_class in batch for p in patterns):
            _fetch_resource_count_batch(
                batch,
                label,
                counts,
                helper,
                graph_uris,
                collect_bindings,
                report,
                class_chunk_size or 10000,
                unsafe_paging,
                type_context_graph_uris,
                object_class="BlankNode",
            )

        members = set(batch)
        report.checkpoint(
            "counts",
            list(key),
            [
                {"key": list(k), "graph": graph, **vars(metric)}
                for k, per_graph in counts.items()
                if k[0] in members
                for graph, metric in per_graph.items()
            ],
            "complete" if len(report.report.query_failures) == failed else "partial",
        )

        # Delay between batches
        if delay > 0:
            time.sleep(delay)

    for copy, source in shared.items():
        for (cls, prop, obj), metric in list(counts.items()):
            if cls == source:
                counts[(copy, prop, obj)] = metric
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
        per_graph = counts.get(key)
        if per_graph is None:
            enriched.append(pat.model_copy(update={"count": None}))
            continue
        enriched.append(apply_counts(pat, per_graph, graph_uris))

    return [*enriched, *untyped]


def apply_counts(
    pattern: SchemaPattern, per_graph: dict[str, _PatternCount], graph_uris: list[str] | None
) -> SchemaPattern:
    """Return *pattern* with the counts of its rows in each edge graph ("" without graphs).

    A count from a sample of a refused query flags the pattern sampled, with count_bound
    lower_bound. Distinct counts are only safe to expose directly when the selected scope has at
    most one named graph (or the endpoint default graph). Summing per-graph distinct counts across
    several graphs would double count subjects/objects repeated in multiple graphs.
    Dataset-scope support is represented separately by PropertyUsageEvidence.
    """
    from rdfsolve.mining.sampling import flag

    attributed = {graph: metric.triples for graph, metric in per_graph.items() if graph}
    distinct_subjects = None
    distinct_objects = None
    if not graph_uris or len(graph_uris) <= 1:
        metrics = list(per_graph.values())
        if len(metrics) == 1:
            distinct_subjects = metrics[0].distinct_subjects
            distinct_objects = metrics[0].distinct_objects
    counted = pattern.model_copy(
        update={
            "count": sum(metric.triples for metric in per_graph.values()),
            "count_semantics": (
                "quad_occurrences"
                if len(graph_uris or []) > 1
                else "triples_in_graph"
                if graph_uris
                else "endpoint_default"
            ),
            "graphs": attributed or None,
            "distinct_subjects": distinct_subjects,
            "distinct_objects": distinct_objects,
        }
    )
    # Counts from a sample of a refused query are lower bounds (rdfsolve.mining.sampling).
    sample = next((m.sample for m in per_graph.values() if m.sample is not None), None)
    if sample is not None:
        flag(counted, sample, ["counts"])
    return counted


def _fetch_typed_count_batch(
    batch: list[str],
    label: str,
    counts: dict[tuple[str, str, str], dict[str, _PatternCount]],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    report: ReportCollector,
    chunk_size: int,
    unsafe_paging: bool,
    type_context_graph_uris: list[str] | None = None,
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
            type_context_graph_uris,
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
            metric = _count_from_binding(b)
            if metric is not None:
                counts.setdefault(key, {})[b.get("_g", {}).get("value", "")] = metric
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


def _literal_key(binding: dict[str, Any]) -> tuple[str, str, str]:
    """Return the count key of a literal row: class, property and ``Literal[:datatype]``."""
    dt = binding.get("dt", {}).get("value", "")
    return (
        binding.get("class", {}).get("value", ""),
        binding.get("p", {}).get("value", ""),
        f"Literal:{dt}" if dt else "Literal",
    )


def _fetch_literal_count_batch(
    batch: list[str],
    label: str,
    counts: dict[tuple[str, str, str], dict[str, _PatternCount]],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    report: ReportCollector,
    chunk_size: int,
    unsafe_paging: bool,
    type_context_graph_uris: list[str] | None = None,
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
            type_context_graph_uris,
        )
        report.record_outcome(outcome)
        report.record_query(
            "counts/literal",
            time.monotonic() - t0,
            success=outcome.state == "complete",
        )
        for b in outcome.rows:
            metric = _count_from_binding(b)
            if metric is not None:
                counts.setdefault(_literal_key(b), {})[b.get("_g", {}).get("value", "")] = metric

        t0 = time.monotonic()
        objects = query_with_bisect(
            batch,
            graph_uris,
            _build_batched_literal_objects_query,
            "counts/literal-objects",
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
            type_context_graph_uris,
        )
        report.record_query(
            "counts/literal-objects",
            time.monotonic() - t0,
            success=objects.state == "complete",
        )
        if objects.state != "complete":
            logger.warning("Distinct literal objects unavailable (%s)", label)
            return
        for b in objects.rows:
            key, graph = _literal_key(b), b.get("_g", {}).get("value", "")
            metric = counts.get(key, {}).get(graph)
            value = b.get("objects", {}).get("value")
            # Distinct objects of a sample are not given to a count of all edges.
            if metric is not None and value is not None and (metric.sample or not sample_of(b)):
                counts[key][graph] = replace(metric, distinct_objects=int(value))
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


def _fetch_resource_count_batch(
    batch: list[str],
    label: str,
    counts: dict[tuple[str, str, str], dict[str, _PatternCount]],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    report: ReportCollector,
    chunk_size: int,
    unsafe_paging: bool,
    type_context_graph_uris: list[str] | None = None,
    *,
    object_class: str = "Resource",
) -> None:
    """Count untyped URI or blank-node edges for one class batch."""
    purpose = "counts/blank-node" if object_class == "BlankNode" else "counts/untyped-uri"
    builder = (
        _build_batched_blank_node_count_query
        if object_class == "BlankNode"
        else _build_batched_untyped_count_query
    )
    try:
        t0 = time.monotonic()
        outcome = query_with_bisect(
            batch,
            graph_uris,
            builder,
            purpose,
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
            type_context_graph_uris,
        )
        report.record_outcome(outcome)
        report.record_query(
            purpose,
            time.monotonic() - t0,
            success=outcome.state == "complete",
        )
        for b in outcome.rows:
            key = (
                b.get("class", {}).get("value", ""),
                b.get("p", {}).get("value", ""),
                object_class,
            )
            metric = _count_from_binding(b)
            if metric is not None:
                counts.setdefault(key, {})[b.get("_g", {}).get("value", "")] = metric
    except (ValueError, TypeError) as e:
        report.record_outcome(
            QueryOutcome(
                state="failed",
                failures=[QueryFailure("invalid_response", str(e), purpose, batch, graph_uris)],
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
        if pat.object_class not in ("Literal", "Resource", "BlankNode"):
            all_uris.add(pat.object_class)

    if not all_uris:
        return patterns

    # Fetch labels in batches to keep query size small
    label_map: dict[str, str] = {}
    batch_size = 50
    uri_list = sorted(all_uris)

    for start in range(0, len(uri_list), batch_size):
        batch = uri_list[start : start + batch_size]
        if not _fetch_label_batch(batch, label_map, helper, graph_uris, report):
            logger.warning(
                "Labels stopped: the endpoint asked for a longer pause than the wait budget; "
                "%d IRIs keep their local names",
                len(uri_list) - start,
            )
            break

    # Fill in labels using local name as fallback
    enriched = _enrich_with_local(patterns, label_map)

    return enriched


def _fetch_label_batch(
    batch: list[str],
    label_map: dict[str, str],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    report: ReportCollector,
) -> bool:
    """Query labels for one batch of URIs and update label_map in place.

    Return False when the endpoint refuses because of a rate limit, so that no more batches
    are sent.

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
        candidates: dict[str, dict[str, dict[str, str]]] = {}
        for row in bindings:
            uri = row.get("uri", {}).get("value")
            if uri not in batch:
                continue
            fields = candidates.setdefault(uri, {})
            for key, term in row.items():
                if key == "uri" or not term.get("value"):
                    continue
                old = fields.get(key)
                rank = (term.get("xml:lang") not in (None, "en"), term["value"])
                if old is None or rank < (old.get("xml:lang") not in (None, "en"), old["value"]):
                    fields[key] = term
        for uri, row in candidates.items():
            label_map[uri] = pick_label(
                row.get("rdfsLabel", {}).get("value"),
                row.get("dcTitle", {}).get("value"),
                uri,
                iao_label=row.get("iaoLabel", {}).get("value"),
                skos_pref_label=row.get("skosPrefLabel", {}).get("value"),
                skos_alt_label=row.get("skosAltLabel", {}).get("value"),
            )
    except Exception as e:
        report.record_query(
            "labels",
            time.monotonic() - t0,
            success=False,
        )
        if isinstance(e, EndpointRateLimitError):
            return False  # The next batches would be refused too.
        logger.warning("Label batch failed (%d IRIs): %s", len(batch), type(e).__name__)
    return True


def _enrich_with_local(
    patterns: list[SchemaPattern],
    label_map: dict[str, str],
) -> list[SchemaPattern]:
    """Fill in labels using fetched labels, then a label the pattern has, then the local name.

    A pattern has a label of its own when its class is anonymous (scan mining names it by its
    expression in Manchester syntax).
    """
    enriched: list[SchemaPattern] = []
    for pat in patterns:
        updates: dict[str, Any] = {}
        updates["subject_label"] = label_map.get(
            pat.subject_class,
            pat.subject_label or get_local_name(pat.subject_class),
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
                pat.object_label or get_local_name(pat.object_class),
            )
        enriched.append(pat.model_copy(update=updates))
    return enriched
