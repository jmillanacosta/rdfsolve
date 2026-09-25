"""Two-phase mining strategy for large endpoints."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from pydantic import ValidationError

from rdfsolve._outcomes import QueryOutcome
from rdfsolve.mining.blank_nodes import blank_node_patterns
from rdfsolve.mining.query_builders import (
    _build_batched_blank_node_query,
    _build_batched_literal_query,
    _build_batched_typed_object_query,
    _build_batched_untyped_uri_query,
    _build_class_discovery_query,
    _build_class_discovery_query_plain,
    _build_class_weight_query,
    _build_same_members_query,
)
from rdfsolve.mining.query_fallbacks import query_with_bisect, select_outcome
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.models import SchemaPattern
from rdfsolve.sparql_helper import ResponseLimitError

logger = logging.getLogger(__name__)

__all__ = ["TwoPhaseStrategy", "plan_class_batches"]

# Above this many classes, batch by instance count instead of a fixed size.
WEIGHTED_BATCHING_ABOVE = 300
MAX_CLASSES_PER_BATCH = 500
MAX_INSTANCES_PER_BATCH = 1_000_000


def plan_class_batches(
    classes: list[str],
    weights: dict[str, int],
    *,
    max_classes: int = MAX_CLASSES_PER_BATCH,
    max_instances: int = MAX_INSTANCES_PER_BATCH,
) -> list[list[str]]:
    """Pack classes into batches bounded by class count and typed-instance count.

    Heavy classes get batches of their own; many light classes (for example
    ontology terms that type a few instances each) share one query.
    """
    ordered = sorted(classes, key=lambda c: (-weights.get(c, 1), c))
    batches: list[list[str]] = []
    batch: list[str] = []
    load = 0
    for cls in ordered:
        weight = max(weights.get(cls, 1), 1)
        if batch and (len(batch) >= max_classes or load + weight > max_instances):
            batches.append(batch)
            batch, load = [], 0
        batch.append(cls)
        load += weight
    if batch:
        batches.append(batch)
    return batches


class TwoPhaseStrategy(MiningStrategy):
    """Two-phase mining strategy for large endpoints.

    Phase 1: Discover classes via SELECT DISTINCT ?class
    Phase 2: For each class, run scoped queries for typed-object, literal,
             untyped-URI, and blank-node patterns

    This avoids massive unscoped triple-joins that choke large endpoints.
    """

    @property
    def name(self) -> str:
        """Return strategy name."""
        return "two-phase"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Execute two-phase mining.

        Args:
            context: Mining context with dependencies

        Returns:
            List of mined schema patterns
        """
        # Phase 1 - discover classes
        p1 = context.report.start_phase("class-discovery")
        classes = self._discover_classes(context)
        if not classes and not context.graph_uris:
            classes = self._discover_classes_in_named_graphs(context)
        if not classes:
            context.report.finish_phase(p1, items=0)
            context.report.report.config["class_schema_state"] = "no_observed_data_classes"
            return []

        # Merge with ontology classes if available
        if context.ontology_classes:
            # Merge, keeping unique classes
            classes_set = set(classes)
            new_from_ontology = [c for c in context.ontology_classes if c not in classes_set]
            if new_from_ontology:
                logger.info(f"  -> Adding {len(new_from_ontology)} classes from ontology structure")
                classes.extend(new_from_ontology)
            logger.info(f"  -> {len(classes)} total classes (data + ontology)")

        context.report.finish_phase(p1, items=len(classes))
        context.class_batches = self._plan_batches(classes, context)

        # Phase 2 - batched per-class pattern discovery
        p2 = context.report.start_phase("per-class-patterns")
        patterns, abort_reason = self._run_phase2_batches(
            classes,
            context.graph_uris,
            context,
            batches=context.class_batches,
        )

        logger.info(f"  -> {len(patterns)} total patterns from {len(classes)} classes")
        context.report.finish_phase(p2, items=len(patterns), error=abort_reason)
        if abort_reason:
            context.report.set_abort_reason(abort_reason)
        return patterns

    def _plan_batches(self, classes: list[str], context: MiningContext) -> list[list[str]]:
        """Use fixed batches, or instance-count batches when there are many classes."""
        size = context.class_batch_size
        fixed = [classes[i : i + size] for i in range(0, len(classes), size)]
        if len(classes) <= WEIGHTED_BATCHING_ABOVE and context.helper.sparql_engine != "qlever":
            return fixed
        t0 = time.monotonic()
        try:
            rows = context.collect_bindings(
                _build_class_weight_query(context.graph_uris, context.type_context_graph_uris),
                "two-phase/class-weights",
                context.chunk_size,
            )
        except Exception as error:
            context.report.record_query(
                "two-phase/class-weights", time.monotonic() - t0, success=False
            )
            logger.warning("Instance counts per class failed (%s); using fixed batches", error)
            return fixed
        context.report.record_query("two-phase/class-weights", time.monotonic() - t0)
        weights: dict[str, int] = {}
        for row in rows:
            try:
                weights[row["class"]["value"]] = int(row["n"]["value"])
            except (KeyError, TypeError, ValueError):
                continue
        max_classes = size if len(classes) <= WEIGHTED_BATCHING_ABOVE else MAX_CLASSES_PER_BATCH
        context.class_weights = weights
        batches = plan_class_batches(classes, weights, max_classes=max_classes)
        logger.info(
            "  -> %d classes packed into %d batches by instance count (was %d fixed batches)",
            len(classes),
            len(batches),
            len(fixed),
        )
        return batches

    def _discover_classes(self, context: MiningContext) -> list[str]:
        """Discover named classes in the current scope."""
        ccs = context.class_chunk_size
        if ccs is None:
            logger.info("Phase 1: discovering classes (no pagination) …")
            q = _build_class_discovery_query_plain(
                context.graph_uris, context.type_context_graph_uris
            )
            t0 = time.monotonic()
            try:
                try:
                    result = context.helper.select(q, purpose="two-phase/classes")
                    class_bindings = result.get("results", {}).get("bindings", [])
                except ResponseLimitError:
                    logger.warning("Class listing exceeded the response limit; paging it")
                    class_bindings = context.collect_bindings(
                        _build_class_discovery_query(
                            context.graph_uris, context.type_context_graph_uris
                        ),
                        "two-phase/classes",
                        context.chunk_size,
                    )
                context.report.record_query(
                    "two-phase/classes",
                    time.monotonic() - t0,
                )
            except Exception:
                context.report.record_query(
                    "two-phase/classes",
                    time.monotonic() - t0,
                    success=False,
                )
                raise
        else:
            logger.info("Phase 1: discovering classes (chunk_size=%d) …", ccs)
            q = _build_class_discovery_query(context.graph_uris, context.type_context_graph_uris)
            class_bindings = context.collect_bindings(q, "two-phase/classes", ccs)

        classes = []
        non_iri_count = 0
        for b in class_bindings:
            binding = b.get("class", {})
            if binding.get("type") == "uri":
                value = binding.get("value", "")
                if value:
                    classes.append(value)
            else:
                non_iri_count += 1
        if non_iri_count:
            logger.info(f"  -> Skipped {non_iri_count} non-IRI type values")
        logger.info(f"  -> {len(classes)} data classes found")
        return classes

    def _discover_classes_in_named_graphs(self, context: MiningContext) -> list[str]:
        """Retry Phase 1 in the named graphs when the default graph has no types."""
        from rdfsolve.mining.graph_selection import discover_data_graphs

        graphs = discover_data_graphs(
            context.helper, excluded_prefixes=context.excluded_graph_prefixes
        )
        companion = set(context.type_context_graph_uris or []) | set(
            context.ontology_graph_uris or []
        )
        graphs = [graph for graph in graphs if graph not in companion]
        if not graphs:
            return []
        logger.info(
            "Default graph holds no typed data; retrying Phase 1 in %d named graphs", len(graphs)
        )
        context.graph_uris = graphs
        return self._discover_classes(context)

    def _run_phase2_batches(
        self,
        classes: list[str],
        graph_uris: list[str] | None,
        context: MiningContext,
        batches: list[list[str]] | None = None,
    ) -> tuple[list[SchemaPattern], str | None]:
        """Execute Phase 2 batched queries for classes, in fixed or given batches."""
        if batches is None:
            bs = context.class_batch_size
            batches = [classes[i : i + bs] for i in range(0, len(classes), bs)]
        total = len(classes)
        n_batches = len(batches)

        scope = f"{len(graph_uris)} named graphs" if graph_uris else "default graph"
        logger.info(
            "Phase 2: mining patterns in %d batches (%d classes total, scope: %s) …",
            n_batches,
            total,
            scope,
        )

        patterns: list[SchemaPattern] = []
        abort_reason: str | None = None
        anonymous_classes = 0

        def query_bisect(
            batch: list[str],
            build_fn: Callable[..., str],
            purpose: str,
        ) -> QueryOutcome:
            """Run a query group and record unresolved failures."""
            nonlocal abort_reason
            outcome = query_with_bisect(
                batch,
                graph_uris,
                build_fn,
                purpose,
                context.helper,
                lambda q, p, cs=None: context.collect_bindings(  # type: ignore[misc]
                    q, p, cs or context.chunk_size
                ),
                context.chunk_size,
                unsafe_paging=context.unsafe_paging,
                type_context_graph_uris=context.type_context_graph_uris,
            )
            context.report.record_outcome(outcome)
            if outcome.state != "complete":
                abort_reason = context.report.report.abort_reason
            return outcome

        done = 0
        mined: dict[str, tuple[int, int]] = {}  # single mined class -> its pattern slice
        for batch_idx, batch in enumerate(batches):
            batch_label = (
                f"batch {batch_idx + 1}/{n_batches} "
                f"(classes {done + 1}-{done + len(batch)}/{total})"
            )
            done += len(batch)
            logger.info("  %s", batch_label)
            first = len(patterns)
            if tuple(batch) in context.resumed:
                rows = context.resumed[tuple(batch)]
                patterns.extend(SchemaPattern.model_validate(row) for row in rows)
                context.report.report.config["resumed_batches"].append(list(batch))
                context.report.checkpoint("patterns", batch, rows)
                mined[batch[0]] = (first, len(patterns))
                continue
            source = _same_members(batch, mined, context)
            if source:
                start, end = mined[source]
                copies = [
                    p.model_copy(update={"subject_class": batch[0]}) for p in patterns[start:end]
                ]
                patterns.extend(copies)
                context.shared_extensions[batch[0]] = source
                context.report.report.config.setdefault("shared_extensions", {})[batch[0]] = source
                context.report.checkpoint(
                    "patterns", batch, [p.model_dump(mode="json") for p in copies]
                )
                continue

            # 2a. Typed-object patterns
            t0 = time.monotonic()
            typed_bindings = query_bisect(
                batch, _build_batched_typed_object_query, "two-phase/typed-object"
            )
            context.report.record_query(
                "two-phase/typed-object",
                time.monotonic() - t0,
                success=typed_bindings.state == "complete",
            )
            anonymous_typed: list[dict[str, Any]] = []
            for b in typed_bindings.rows:
                if b.get("class", {}).get("type") == "bnode":
                    # An anonymous subject class cannot name a pattern.
                    anonymous_classes += 1
                    continue
                if b.get("oc", {}).get("type") == "bnode":
                    # Keep the edge, aggregated by the blank object class it points at.
                    anonymous_typed.append(b)
                    continue
                cls = b.get("class", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                oc = b.get("oc", {}).get("value", "")
                if cls and p and oc:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=cls,
                                property_uri=p,
                                object_class=oc,
                            )
                        )
                    except (ValueError, ValidationError):
                        context.report.record_dropped_uri(f"{cls} {p} {oc}")
            patterns.extend(blank_node_patterns(anonymous_typed, context, "class"))

            # 2b. Literal patterns
            t0 = time.monotonic()
            literal_bindings = query_bisect(
                batch, _build_batched_literal_query, "two-phase/literal"
            )
            context.report.record_query(
                "two-phase/literal",
                time.monotonic() - t0,
                success=literal_bindings.state == "complete",
            )
            for b in literal_bindings.rows:
                cls = b.get("class", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                dt = b.get("dt", {}).get("value")
                if cls and p:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=cls,
                                property_uri=p,
                                object_class="Literal",
                                datatype=dt if dt else None,
                            )
                        )
                    except (ValueError, ValidationError):
                        context.report.record_dropped_uri(f"{cls} {p} Literal")

            # 2c. Untyped-URI patterns
            t0 = time.monotonic()
            untyped_bindings = query_bisect(
                batch, _build_batched_untyped_uri_query, "two-phase/untyped-uri"
            )
            context.report.record_query(
                "two-phase/untyped-uri",
                time.monotonic() - t0,
                success=untyped_bindings.state == "complete",
            )
            untyped_oc = (
                "http://www.w3.org/2002/07/owl#Class" if context.untyped_as_classes else "Resource"
            )
            for b in untyped_bindings.rows:
                cls = b.get("class", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                if cls and p:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=cls,
                                property_uri=p,
                                object_class=untyped_oc,
                            )
                        )
                    except (ValueError, ValidationError):
                        context.report.record_dropped_uri(f"{cls} {p} {untyped_oc}")

            # 2d. Blank node patterns
            t0 = time.monotonic()
            blank_bindings = query_bisect(
                batch, _build_batched_blank_node_query, "two-phase/blank-node"
            )
            context.report.record_query(
                "two-phase/blank-node",
                time.monotonic() - t0,
                success=blank_bindings.state == "complete",
            )
            patterns.extend(blank_node_patterns(blank_bindings.rows, context, "class"))
            context.report.checkpoint(
                "patterns", batch, [p.model_dump(mode="json") for p in patterns[first:]]
            )
            if len(batch) == 1:
                mined[batch[0]] = (first, len(patterns))

        if anonymous_classes:
            logger.info(
                "  -> Skipped %d rows whose subject or object class is an anonymous node",
                anonymous_classes,
            )
        return patterns, abort_reason


def _same_members(
    batch: list[str], mined: dict[str, tuple[int, int]], context: MiningContext
) -> str | None:
    """Return a mined class with exactly the same typed subjects as a one-class batch.

    Equal populations are confirmed by counting subjects typed with both classes;
    identical subject sets give identical class-property patterns and counts.
    """
    if len(batch) != 1 or batch[0] not in context.class_weights:
        return None
    size = context.class_weights[batch[0]]
    for other in mined:
        if context.class_weights.get(other) != size or other in context.shared_extensions:
            continue
        query = _build_same_members_query(
            other, batch[0], context.graph_uris, context.type_context_graph_uris
        )
        outcome = select_outcome(query, "two-phase/same-members", context.helper, batch)
        rows = outcome.rows if outcome.state == "complete" else []
        if rows and rows[0].get("n", {}).get("value") == str(size):
            return other
    return None
