"""Derive class-to-class mappings from instance-level mapping edges using ClassIndex."""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from rdfsolve.class_index import ClassIndex
from rdfsolve.mapping_models.core import MappingEdge

_log = logging.getLogger(__name__)

__all__ = ["ClassPair", "derive_class_mappings"]


@dataclass
class ClassPair:
    """A derived class-to-class mapping with supporting evidence."""

    source_class: str
    target_class: str
    source_dataset: str
    target_dataset: str
    predicate: str
    instance_count: int = 0
    confidence: float = 0.0
    source_entities: set[str] = field(default_factory=set)
    target_entities: set[str] = field(default_factory=set)

    def to_mapping_edge(self) -> MappingEdge:
        """Convert to a MappingEdge."""
        return MappingEdge(
            source_class=self.source_class,
            target_class=self.target_class,
            predicate=self.predicate,
            source_dataset=self.source_dataset,
            target_dataset=self.target_dataset,
            confidence=self.confidence,
        )


def derive_class_mappings(
    instance_edges: list[MappingEdge],
    class_index: ClassIndex,
    *,
    min_instance_count: int = 1,
    min_confidence: float = 0.0,
) -> tuple[list[ClassPair], dict[str, Any]]:
    """Derive class-level mappings from instance-level edges.

    For each instance edge (entity_a -> entity_b), looks up the classes
    of both entities in the ClassIndex and creates (class_a -> class_b)
    pairs. Aggregates evidence and computes confidence.

    Parameters
    ----------
    instance_edges
        List of instance-level MappingEdge objects.
    class_index
        ClassIndex with entity-to-class mappings.
    min_instance_count
        Minimum number of instance edges to support a class pair.
    min_confidence
        Minimum confidence score for output pairs.

    Returns
    -------
    tuple[list[ClassPair], dict]
        List of derived ClassPair objects and statistics dict.

    Example
    -------
    >>> edges = [
    ...     MappingEdge(
    ...         source_class="http://id.org/gene/1",
    ...         target_class="http://id.org/protein/A",
    ...         predicate="skos:exactMatch",
    ...         source_dataset="ncbigene",
    ...         target_dataset="uniprot",
    ...     )
    ... ]
    >>> pairs, stats = derive_class_mappings(edges, class_index)
    """
    # Dictionary mapping (source_class, target_class, predicate, src_ds, tgt_ds) to ClassPair
    pair_evidence: dict[tuple[str, str, str, str, str], ClassPair] = {}

    skipped_no_source = 0
    skipped_no_target = 0
    processed = 0

    for edge in instance_edges:
        source_entity = edge.source_class  # In instance edges, "class" is actually entity IRI
        target_entity = edge.target_class

        source_classes = class_index.get_classes(source_entity)
        target_classes = class_index.get_classes(target_entity)

        if not source_classes:
            skipped_no_source += 1
            continue
        if not target_classes:
            skipped_no_target += 1
            continue

        processed += 1

        # Create all class pairs from the cartesian product
        for sc in source_classes:
            for tc in target_classes:
                key = (sc, tc, edge.predicate, edge.source_dataset, edge.target_dataset)
                if key not in pair_evidence:
                    pair_evidence[key] = ClassPair(
                        source_class=sc,
                        target_class=tc,
                        source_dataset=edge.source_dataset,
                        target_dataset=edge.target_dataset,
                        predicate=edge.predicate,
                    )
                pair = pair_evidence[key]
                pair.instance_count += 1
                pair.source_entities.add(source_entity)
                pair.target_entities.add(target_entity)

    # Compute confidence and filter
    output_pairs: list[ClassPair] = []
    for pair in pair_evidence.values():
        # Simple confidence: proportion of distinct source entities
        # that map to this target class
        pair.confidence = min(1.0, pair.instance_count / max(1, len(pair.source_entities)))

        if pair.instance_count >= min_instance_count and pair.confidence >= min_confidence:
            output_pairs.append(pair)

    # Sort by confidence descending
    output_pairs.sort(key=lambda p: (-p.confidence, -p.instance_count))

    # Compute statistics
    confidences = [p.confidence for p in output_pairs]
    predicate_counts = Counter(p.predicate for p in output_pairs)

    stats = {
        "input_edges": len(instance_edges),
        "processed_edges": processed,
        "skipped_no_source_class": skipped_no_source,
        "skipped_no_target_class": skipped_no_target,
        "class_pairs_found": len(pair_evidence),
        "output_edges": len(output_pairs),
        "min_instance_count": min_instance_count,
        "min_confidence": min_confidence,
        "confidence_mean": sum(confidences) / len(confidences) if confidences else 0,
        "confidence_max": max(confidences) if confidences else 0,
        "predicates_distribution": dict(predicate_counts),
    }

    _log.info(
        "Derived %d class pairs from %d instance edges",
        len(output_pairs),
        len(instance_edges),
    )

    return output_pairs, stats
