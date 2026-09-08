"""Two-phase mining strategy for large endpoints."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from pydantic import ValidationError

from rdfsolve._outcomes import QueryOutcome
from rdfsolve.mining.query_builders import (
    _build_batched_blank_node_query,
    _build_batched_literal_query,
    _build_batched_typed_object_query,
    _build_batched_untyped_uri_query,
    _build_class_discovery_query,
    _build_class_discovery_query_plain,
)
from rdfsolve.mining.query_fallbacks import query_with_bisect
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.mining.types import ONTOLOGY_METACLASSES
from rdfsolve.models import SchemaPattern

logger = logging.getLogger(__name__)

__all__ = ["TwoPhaseStrategy"]


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
        ccs = context.class_chunk_size
        if ccs is None:
            logger.info("Phase 1: discovering classes (no pagination) …")
            q = _build_class_discovery_query_plain(context.graph_uris)
            t0 = time.monotonic()
            try:
                result = context.helper.select(q, purpose="two-phase/classes")
                class_bindings = result.get("results", {}).get("bindings", [])
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
            q = _build_class_discovery_query(context.graph_uris)
            class_bindings = context.collect_bindings(q, "two-phase/classes", ccs)

        # Extract class URIs - only keep IRI bindings, skip literals/bnodes
        # Filter out ontology metaclasses (owl:Class, rdfs:Class, etc.)
        classes = []
        non_iri_count = 0
        metaclass_count = 0
        for b in class_bindings:
            binding = b.get("class", {})
            if binding.get("type") == "uri":
                value = binding.get("value", "")
                if value:
                    if value in ONTOLOGY_METACLASSES:
                        metaclass_count += 1
                    else:
                        classes.append(value)
            else:
                non_iri_count += 1
        if non_iri_count:
            logger.info(f"  -> Skipped {non_iri_count} non-IRI type values")
        if metaclass_count:
            logger.info(f"  -> Filtered {metaclass_count} ontology metaclasses")
        logger.info(f"  -> {len(classes)} data classes found")

        # Merge with ontology classes if available
        if context.ontology_classes:
            # Filter out metaclasses from ontology classes too
            ontology_classes_filtered = [
                c for c in context.ontology_classes if c not in ONTOLOGY_METACLASSES
            ]
            # Merge, keeping unique classes
            classes_set = set(classes)
            new_from_ontology = [c for c in ontology_classes_filtered if c not in classes_set]
            if new_from_ontology:
                logger.info(f"  -> Adding {len(new_from_ontology)} classes from ontology structure")
                classes.extend(new_from_ontology)
            logger.info(f"  -> {len(classes)} total classes (data + ontology)")

        context.report.finish_phase(p1, items=len(classes))

        # Phase 2 - batched per-class pattern discovery
        p2 = context.report.start_phase("per-class-patterns")
        patterns, abort_reason = self._run_phase2_batches(
            classes,
            context.graph_uris,
            context,
        )

        logger.info(f"  -> {len(patterns)} total patterns from {len(classes)} classes")
        context.report.finish_phase(p2, items=len(patterns), error=abort_reason)
        if abort_reason:
            context.report.set_abort_reason(abort_reason)
        return patterns

    def _run_phase2_batches(
        self,
        classes: list[str],
        graph_uris: list[str] | None,
        context: MiningContext,
    ) -> tuple[list[SchemaPattern], str | None]:
        """Execute Phase 2 batched queries for classes."""
        bs = context.class_batch_size
        total = len(classes)
        n_batches = (total + bs - 1) // bs

        scope = f"GRAPH <{', '.join(graph_uris)}>" if graph_uris else "default graph"
        logger.info(
            "Phase 2: mining patterns in %d batches of ≤%d classes (%d classes total, scope: %s) …",
            n_batches,
            bs,
            total,
            scope,
        )

        patterns: list[SchemaPattern] = []
        abort_reason: str | None = None

        def query_bisect(
            batch: list[str],
            build_fn: Callable[[list[str], list[str] | None, bool, bool], str],
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
            )
            context.report.record_outcome(outcome)
            if outcome.state != "complete":
                abort_reason = context.report.report.abort_reason
            return outcome

        for batch_idx in range(n_batches):
            batch_start = batch_idx * bs
            batch = classes[batch_start : batch_start + bs]
            batch_label = (
                f"batch {batch_idx + 1}/{n_batches} "
                f"(classes {batch_start + 1}"
                f"-{batch_start + len(batch)}/{total})"
            )
            logger.info("  %s", batch_label)

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
            for b in typed_bindings.rows:
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
            for b in blank_bindings.rows:
                cls = b.get("class", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                bn_pred = b.get("bnPred", {}).get("value")
                if cls and p:
                    # Represent blank nodes with structural signature if available
                    if bn_pred:
                        object_class = f"BlankNode[{bn_pred}]"
                    else:
                        object_class = "BlankNode"
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=cls,
                                property_uri=p,
                                object_class=object_class,
                            )
                        )
                    except (ValueError, ValidationError):
                        context.report.record_dropped_uri(f"{cls} {p} {object_class}")

        return patterns, abort_reason
