"""One-shot mining strategy for fast local endpoints."""

from __future__ import annotations

import logging
import time
from typing import Any

from pydantic import ValidationError

from rdfsolve.mining.query_builders import (
    _build_blank_node_query_plain,
    _build_literal_query_plain,
    _build_typed_object_query_plain,
    _build_untyped_uri_query_plain,
)
from rdfsolve.mining.query_fallbacks import select_outcome
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.models import OneShotQueryResult, PatternType, SchemaPattern

logger = logging.getLogger(__name__)

__all__ = ["OneShotStrategy"]


class OneShotStrategy(MiningStrategy):
    """One-shot mining strategy using unbounded SELECT queries.

    No LIMIT/OFFSET, no bisection, no pagination fallback. Intended for
    local QLever endpoints that can return unlimited result sets in one
    HTTP response.
    """

    @property
    def name(self) -> str:
        """Return strategy name."""
        return "one-shot"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Execute one-shot mining.

        Args:
            context: Mining context with dependencies

        Returns:
            List of mined schema patterns
        """
        oc_default = (
            "http://www.w3.org/2002/07/owl#Class" if context.untyped_as_classes else "Resource"
        )

        _specs: list[tuple[str, str]] = [
            ("typed-object", _build_typed_object_query_plain(context.graph_uris)),
            ("literal", _build_literal_query_plain(context.graph_uris)),
            ("untyped-uri", _build_untyped_uri_query_plain(context.graph_uris)),
            ("blank-node", _build_blank_node_query_plain(context.graph_uris)),
        ]

        patterns: list[SchemaPattern] = []
        results: list[OneShotQueryResult] = []

        for qtype, query in _specs:
            bindings, result = self._run_one_shot_query(qtype, query, context)
            results.append(result)
            if result.success:
                patterns.extend(
                    self._parse_one_shot_bindings(
                        qtype,
                        bindings,
                        oc_default,
                        context,
                    )
                )

        # Store one-shot results in report for comparison
        context.report.report.one_shot_results = results

        return patterns

    def _run_one_shot_query(
        self,
        qtype: str,
        query: str,
        context: MiningContext,
    ) -> tuple[list[dict[str, Any]], OneShotQueryResult]:
        """Execute a single one-shot SELECT and record analytics."""
        phase = context.report.start_phase(f"one-shot/{qtype}")
        t0 = time.monotonic()
        outcome = select_outcome(
            query,
            f"one-shot/{qtype}",
            context.helper,
            graph_uris=context.graph_uris,
        )
        duration = time.monotonic() - t0
        context.report.record_outcome(outcome)
        success = outcome.state == "complete"
        error = "; ".join(failure.message for failure in outcome.failures) or None
        context.report.record_query(f"one-shot/{qtype}", duration, success=success)
        context.report.finish_phase(phase, items=len(outcome.rows), error=error)
        return outcome.rows, OneShotQueryResult(
            query_type=qtype,
            success=success,
            duration_s=round(duration, 3),
            row_count=len(outcome.rows) if success else None,
            error=error,
        )

    def _parse_one_shot_bindings(
        self,
        qtype: str,
        bindings: list[dict[str, Any]],
        oc_default: str,
        context: MiningContext,
    ) -> list[SchemaPattern]:
        """Convert raw SPARQL bindings to SchemaPattern objects."""
        patterns: list[SchemaPattern] = []
        if qtype == "typed-object":
            for b in bindings:
                sc = b.get("sc", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                oc = b.get("oc", {}).get("value", "")
                if sc and p and oc:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=sc,
                                property_uri=p,
                                object_class=oc,
                                pattern_type=PatternType.OBJECT_PROPERTY,
                            )
                        )
                    except (ValueError, ValidationError) as exc:
                        context.report.record_dropped_uri(f"{sc} {p} {oc}")
                        logger.debug("Skipping invalid pattern (%s %s %s): %s", sc, p, oc, exc)
        elif qtype == "literal":
            for b in bindings:
                sc = b.get("sc", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                dt = b.get("dt", {}).get("value")
                if sc and p:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=sc,
                                property_uri=p,
                                object_class="Literal",
                                datatype=dt if dt else None,
                                pattern_type=PatternType.DATATYPE_PROPERTY,
                            )
                        )
                    except (ValueError, ValidationError) as exc:
                        context.report.record_dropped_uri(f"{sc} {p} Literal")
                        logger.debug("Skipping invalid pattern (%s %s Literal): %s", sc, p, exc)
        elif qtype == "blank-node":
            # Group bindings by (sc, p) to collect all blank node predicates
            bn_map: dict[tuple[str, str], list[str]] = {}
            for b in bindings:
                sc = b.get("sc", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                bn_pred = b.get("bnPred", {}).get("value")
                if sc and p:
                    key = (sc, p)
                    if key not in bn_map:
                        bn_map[key] = []
                    if bn_pred and bn_pred not in bn_map[key]:
                        bn_map[key].append(bn_pred)
            # Create patterns with collected blank node predicates
            for (sc, p), bn_preds in bn_map.items():
                try:
                    patterns.append(
                        SchemaPattern(
                            subject_class=sc,
                            property_uri=p,
                            object_class="BlankNode",
                            blank_node_predicates=bn_preds if bn_preds else None,
                            pattern_type=PatternType.BLANK_NODE_PROPERTY,
                        )
                    )
                except (ValueError, ValidationError) as exc:
                    context.report.record_dropped_uri(f"{sc} {p} BlankNode")
                    logger.debug("Skipping invalid pattern (%s %s BlankNode): %s", sc, p, exc)
        else:  # untyped-uri
            for b in bindings:
                sc = b.get("sc", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                if sc and p:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=sc,
                                property_uri=p,
                                object_class=oc_default,
                                pattern_type=PatternType.OBJECT_PROPERTY,
                            )
                        )
                    except (ValueError, ValidationError) as exc:
                        context.report.record_dropped_uri(f"{sc} {p} {oc_default}")
                        logger.debug(
                            "Skipping invalid pattern (%s %s %s): %s", sc, p, oc_default, exc
                        )
        return patterns
