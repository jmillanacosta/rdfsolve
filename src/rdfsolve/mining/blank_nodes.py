"""Collect observed predicates on blank-node objects, not persistent node IDs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rdfsolve.schema_models.pattern import PatternType, SchemaPattern

if TYPE_CHECKING:
    from rdfsolve.mining.strategy import MiningContext


def blank_node_patterns(
    rows: list[dict[str, Any]], context: MiningContext, class_variable: str = "sc"
) -> list[SchemaPattern]:
    """Combine blank-node fields for each source class and predicate."""
    groups: dict[tuple[str, str], set[str]] = {}
    for row in rows:
        cls = row.get(class_variable, {}).get("value", "")
        predicate = row.get("p", {}).get("value", "")
        if cls and predicate:
            fields = groups.setdefault((cls, predicate), set())
            if field := row.get("bnPred", {}).get("value"):
                fields.add(field)
    patterns = []
    for (cls, predicate), fields in sorted(groups.items()):
        try:
            patterns.append(
                SchemaPattern(
                    subject_class=cls,
                    property_uri=predicate,
                    object_class="BlankNode",
                    blank_node_predicates=sorted(fields) or None,
                    pattern_type=PatternType.BLANK_NODE_PROPERTY,
                )
            )
        except ValueError:
            context.report.record_dropped_uri(f"{cls} {predicate} BlankNode")
    return patterns
