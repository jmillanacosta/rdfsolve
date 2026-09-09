"""Mine typed objects, literals, untyped IRIs and blank-node patterns separately."""

from __future__ import annotations

import logging

from pydantic import ValidationError

from rdfsolve.mining.blank_nodes import blank_node_patterns
from rdfsolve.mining.query_builders import (
    _build_blank_node_query,
    _build_literal_query,
    _build_typed_object_query,
    _build_untyped_uri_query,
)
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.models import SchemaPattern

logger = logging.getLogger(__name__)

__all__ = ["SinglePassStrategy"]


class SinglePassStrategy(MiningStrategy):
    """Run one paginated query for each object kind."""

    @property
    def name(self) -> str:
        """Return strategy name."""
        return "single-pass"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Execute single-pass mining.

        Args:
            context: Mining context with dependencies

        Returns:
            List of mined schema patterns
        """
        patterns: list[SchemaPattern] = []

        # Phase 1: Typed-object patterns
        phase = context.report.start_phase("typed-object")
        logger.info("Mining typed-object patterns …")
        typed = self._run_typed_object(context)
        patterns.extend(typed)
        logger.info(f"  -> {len(typed)} typed-object patterns")
        context.report.finish_phase(phase, items=len(typed))

        # Phase 2: Literal patterns
        phase = context.report.start_phase("literal")
        logger.info("Mining literal patterns …")
        literals = self._run_literal(context)
        patterns.extend(literals)
        logger.info(f"  -> {len(literals)} literal patterns")
        context.report.finish_phase(phase, items=len(literals))

        # Phase 3: Untyped-URI patterns
        phase = context.report.start_phase("untyped-uri")
        logger.info("Mining untyped-URI patterns …")
        untyped = self._run_untyped_uri(context)
        patterns.extend(untyped)
        logger.info(f"  -> {len(untyped)} untyped-URI patterns")
        context.report.finish_phase(phase, items=len(untyped))

        phase = context.report.start_phase("blank-node")
        bindings = context.collect_bindings(
            _build_blank_node_query(context.graph_uris), "mining/blank-node", None
        )
        blank_nodes = blank_node_patterns(bindings, context)
        patterns.extend(blank_nodes)
        context.report.finish_phase(phase, items=len(blank_nodes))
        return patterns

    def _run_typed_object(self, context: MiningContext) -> list[SchemaPattern]:
        """Run the typed-object SELECT query."""
        q = _build_typed_object_query(context.graph_uris)
        bindings = context.collect_bindings(q, "mining/typed-object", None)
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            oc = b.get("oc", {}).get("value", "")
            if sc and p and oc:
                try:
                    results.append(
                        SchemaPattern(
                            subject_class=sc,
                            property_uri=p,
                            object_class=oc,
                        )
                    )
                except (ValueError, ValidationError):
                    context.report.record_dropped_uri(f"{sc} {p} {oc}")
        return results

    def _run_literal(self, context: MiningContext) -> list[SchemaPattern]:
        """Run the literal-property SELECT query."""
        q = _build_literal_query(context.graph_uris)
        bindings = context.collect_bindings(q, "mining/literal", None)
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            dt = b.get("dt", {}).get("value")
            if sc and p:
                try:
                    results.append(
                        SchemaPattern(
                            subject_class=sc,
                            property_uri=p,
                            object_class="Literal",
                            datatype=dt if dt else None,
                        )
                    )
                except (ValueError, ValidationError):
                    context.report.record_dropped_uri(f"{sc} {p} Literal")
        return results

    def _run_untyped_uri(self, context: MiningContext) -> list[SchemaPattern]:
        """Run the untyped-URI SELECT query."""
        q = _build_untyped_uri_query(context.graph_uris)
        bindings = context.collect_bindings(q, "mining/untyped-uri", None)
        oc = "http://www.w3.org/2002/07/owl#Class" if context.untyped_as_classes else "Resource"
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            if sc and p:
                try:
                    results.append(
                        SchemaPattern(
                            subject_class=sc,
                            property_uri=p,
                            object_class=oc,
                            count=None,
                            datatype=None,
                            subject_label=None,
                            object_label=None,
                            property_label=None,
                        )
                    )
                except (ValueError, ValidationError):
                    context.report.record_dropped_uri(f"{sc} {p} {oc}")
        return results
