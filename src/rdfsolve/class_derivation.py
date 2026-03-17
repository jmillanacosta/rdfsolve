"""Derive class-level mappings from instance-level evidence.

Given a set of instance-level mapping edges (from SSSOM or SeMRA) and a
ClassIndex (built by querying actual entity IRIs against the LSLOD
QLever endpoint), produces ClassDerivedMapping objects where each edge
connects two RDF classes that are linked by aggregated instance evidence.
"""

from __future__ import annotations

import logging
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from rdfsolve.class_index import ClassIndex
from rdfsolve.mapping_models.core import MappingEdge

logger = logging.getLogger(__name__)

__all__ = [
    "ClassPairEvidence",
    "compute_confidence",
    "derive_class_mappings",
]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class ClassPairEvidence:
    """Aggregated evidence for a single (source_class, target_class) pair.

    Attributes:
        source_class: URI of the source class.
        target_class: URI of the target class.
        predicate: Most frequent predicate among supporting edges.
        source_graphs: LSLOD graphs where subjects were typed as
            source_class.
        target_graphs: LSLOD graphs where objects were typed as
            target_class.
        instance_count: Number of supporting instance-level edges.
        distinct_subjects: Number of distinct subject entity IRIs.
        distinct_objects: Number of distinct object entity IRIs.
        confidence: Computed confidence score in [0.0, 1.0].
    """

    source_class: str
    target_class: str
    predicate: str
    source_graphs: set[str] = field(default_factory=set)
    target_graphs: set[str] = field(default_factory=set)
    instance_count: int = 0
    distinct_subjects: int = 0
    distinct_objects: int = 0
    confidence: float = 0.0

    def to_mapping_edge(self) -> MappingEdge:
        """Convert to a MappingEdge for use in a ClassDerivedMapping."""
        return MappingEdge(
            source_class=self.source_class,
            target_class=self.target_class,
            predicate=self.predicate,
            source_dataset=", ".join(sorted(self.source_graphs)),
            target_dataset=", ".join(sorted(self.target_graphs)),
            confidence=self.confidence,
        )


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------


def compute_confidence(evidence: ClassPairEvidence) -> float:
    """Compute confidence for a class pair based on instance evidence.

    Placeholder formula — to be refined with domain expertise.

    The current heuristic considers only raw instance count, using a
    log2 scale capped at 1.0::

        confidence = min(1.0, log2(1 + instance_count) / 10)

    This gives approximately:
        1 instance   -> 0.10
        10 instances -> 0.35
        100 instances -> 0.67
        1000 instances -> 1.00

    Args:
        evidence: Populated ClassPairEvidence object.

    Returns:
        Float in [0.0, 1.0].
    """
    return min(1.0, math.log2(1 + evidence.instance_count) / 10)


# ---------------------------------------------------------------------------
# Derivation engine
# ---------------------------------------------------------------------------


def derive_class_mappings(
    instance_edges: list[MappingEdge],
    class_index: ClassIndex,
    *,
    min_instance_count: int = 1,
    min_confidence: float = 0.0,
) -> tuple[list[ClassPairEvidence], dict[str, Any]]:
    """Derive class-level mapping pairs from instance evidence.

    Algorithm:
    1. For each instance edge (S, P, O):
       a. Look up S in class_index -> get {graph: [classes]}.
       b. Look up O in class_index -> get {graph: [classes]}.
       c. For each (source_class, target_class) pair across all graph
          combinations, accumulate counters.
    2. Compute confidence for each class pair.
    3. Filter by min_instance_count and min_confidence.
    4. Return sorted by confidence descending.

    Because each entity IRI was already expanded to all its bioregistry
    URI forms during class index building, the derivation engine only
    needs to look up the canonical IRI in the index.

    Args:
        instance_edges: Instance-level MappingEdge list.
        class_index: ClassIndex with per-entity class memberships.
        min_instance_count: Minimum supporting instances to keep a pair.
        min_confidence: Minimum confidence to keep a pair.

    Returns:
        Tuple of (class_pairs, derivation_stats).
        derivation_stats keys:
            input_edges, class_pairs_found, class_pairs_after_filter,
            output_edges, min_instance_count, min_confidence,
            confidence_mean, confidence_median, confidence_max,
            predicates_distribution, top_class_pairs.
    """
    # Accumulators per (source_class, target_class) key
    # Each value: {predicates: Counter, subjects: set, objects: set,
    #              source_graphs: set, target_graphs: set}
    counters: dict[
        tuple[str, str],
        dict[str, Any],
    ] = defaultdict(
        lambda: {
            "predicates": Counter(),
            "subjects": set(),
            "objects": set(),
            "source_graphs": set(),
            "target_graphs": set(),
        }
    )

    skipped = 0
    for edge in instance_edges:
        s_graph_classes = class_index.classes_for_entity(edge.source_class)
        o_graph_classes = class_index.classes_for_entity(edge.target_class)

        if not s_graph_classes or not o_graph_classes:
            skipped += 1
            continue

        for s_graph, s_classes in s_graph_classes.items():
            for s_class in s_classes:
                for o_graph, o_classes in o_graph_classes.items():
                    for o_class in o_classes:
                        key = (s_class, o_class)
                        acc = counters[key]
                        acc["predicates"][edge.predicate] += 1
                        acc["subjects"].add(edge.source_class)
                        acc["objects"].add(edge.target_class)
                        acc["source_graphs"].add(s_graph)
                        acc["target_graphs"].add(o_graph)

    # Build ClassPairEvidence objects
    all_pairs: list[ClassPairEvidence] = []
    for (s_class, o_class), acc in counters.items():
        predicate = acc["predicates"].most_common(1)[0][0]
        instance_count = sum(acc["predicates"].values())
        ev = ClassPairEvidence(
            source_class=s_class,
            target_class=o_class,
            predicate=predicate,
            source_graphs=acc["source_graphs"],
            target_graphs=acc["target_graphs"],
            instance_count=instance_count,
            distinct_subjects=len(acc["subjects"]),
            distinct_objects=len(acc["objects"]),
        )
        ev.confidence = compute_confidence(ev)
        all_pairs.append(ev)

    class_pairs_found = len(all_pairs)

    # Filter
    filtered = [
        ev
        for ev in all_pairs
        if ev.instance_count >= min_instance_count and ev.confidence >= min_confidence
    ]
    filtered.sort(key=lambda e: e.confidence, reverse=True)

    logger.info(
        "Derivation: %d input edges, %d class pairs found, "
        "%d after filter (skipped %d edges without class info)",
        len(instance_edges),
        class_pairs_found,
        len(filtered),
        skipped,
    )

    # Build stats
    confidences = [ev.confidence for ev in filtered]
    pred_dist: Counter[str] = Counter()
    for ev in filtered:
        pred_dist[ev.predicate] += 1

    top_pairs = [
        {
            "source_class": ev.source_class,
            "target_class": ev.target_class,
            "instance_count": ev.instance_count,
            "confidence": round(ev.confidence, 4),
        }
        for ev in filtered[:20]
    ]

    stats: dict[str, Any] = {
        "input_edges": len(instance_edges),
        "class_pairs_found": class_pairs_found,
        "class_pairs_after_filter": len(filtered),
        "output_edges": len(filtered),
        "min_instance_count": min_instance_count,
        "min_confidence": min_confidence,
        "confidence_mean": (round(statistics.mean(confidences), 4) if confidences else 0.0),
        "confidence_median": (round(statistics.median(confidences), 4) if confidences else 0.0),
        "confidence_max": (round(max(confidences), 4) if confidences else 0.0),
        "predicates_distribution": dict(pred_dist.most_common()),
        "top_class_pairs": top_pairs,
    }
    return filtered, stats
