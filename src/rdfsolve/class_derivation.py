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
    "derive_class_mappings_from_instances",
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


def derive_class_mappings_from_instances(
    input_paths: list[str],
    output_path: str,
    *,
    endpoint_url: str = "",
    ports_json_path: str | None = None,
    timeout: float = 60.0,
    batch_size: int = 50,
    min_instance_count: int = 1,
    min_confidence: float = 0.0,
    cache_index: bool = False,
    index_cache_path: str | None = None,
    enrich_in_place: bool = False,
    source_name: str | None = None,
) -> dict[str, Any]:
    """Orchestrate the full instance-to-class derivation pipeline.

    1. Load all instance-mapping JSON-LD files from *input_paths*.
    2. Collect unique entity IRIs.
    3. Build (or load) a :class:`~rdfsolve.class_index.ClassIndex`.
    4. Optionally enrich each input file in-place with class annotations.
    5. Call :func:`derive_class_mappings`.
    6. Serialise the resulting ``ClassDerivedMapping`` to *output_path*.
    7. Write a session-report JSON next to the output file.

    Args:
        input_paths: Paths to instance-mapping JSON-LD files.
        output_path: Destination path for the class-derived JSON-LD.
        endpoint_url: Single QLever / SPARQL 1.1 endpoint for class
            lookup (used when *ports_json_path* is not given).
        ports_json_path: Path to ``ports.json`` mapping
            ``{dataset: port}``.  When provided, every per-dataset
            QLever instance is queried and the dataset name is used as
            the graph identifier.
        timeout: Per-request timeout in seconds.
        batch_size: IRIs per VALUES query.
        min_instance_count: Minimum evidence pairs to retain a class pair.
        min_confidence: Minimum confidence score threshold.
        cache_index: Persist the class index to disk and reuse it.
        index_cache_path: Explicit path for the cached index JSON.
        enrich_in_place: Write enriched copies of all input files.
        source_name: Human-readable name for the session report.

    Returns:
        Session-report dict with keys ``source_name``, ``timestamp``,
        ``source_mapping_type``, ``endpoint_url``, ``cost``,
        ``enrichment``, ``derivation``, ``elapsed_s``.
    """
    import json as _json
    import time as _time
    from datetime import datetime as _dt
    from pathlib import Path as _Path

    from rdfsolve.class_index import (
        build_class_index_from_endpoints,
        build_class_index_from_ports,
        enrich_jsonld_with_classes,
    )
    from rdfsolve.mapping_models import MappingEdge
    from rdfsolve.mapping_models.class_derived import ClassDerivedMapping
    from rdfsolve.schema_models.core import AboutMetadata

    t0 = _time.monotonic()

    # 1. Load input files
    input_docs: list[dict[str, Any]] = []
    source_files: list[str] = []
    source_types: set[str] = set()
    for p_str in input_paths:
        raw = _json.loads(_Path(p_str).read_text(encoding="utf-8"))
        input_docs.append(raw)
        source_files.append(p_str)
        about = raw.get("@about", {})
        mt = about.get("mapping_type") or about.get("strategy", "unknown")
        source_types.add(mt)

    # 2. Collect unique entity IRIs
    entity_iris_set: set[str] = set()
    all_instance_edges: list[MappingEdge] = []
    for raw in input_docs:
        for e in raw.get("@graph", []):
            src_iri = e.get("subject_source_iri") or (e.get("subject_source") or {}).get("@id")
            tgt_iri = e.get("object_source_iri") or (e.get("object_source") or {}).get("@id")
            if src_iri:
                entity_iris_set.add(src_iri)
            if tgt_iri:
                entity_iris_set.add(tgt_iri)
            edge_data = {k: v for k, v in e.items() if k in MappingEdge.model_fields}
            all_instance_edges.append(MappingEdge(**edge_data))

    # 3. Build class index
    _cache_path: str | None = None
    if cache_index:
        _cache_path = index_cache_path or str(
            _Path(output_path).with_suffix(".class_index_cache.json")
        )

    if ports_json_path:
        class_index, cost_stats = build_class_index_from_ports(
            sorted(entity_iris_set),
            ports_json_path,
            batch_size=batch_size,
            timeout=timeout,
            cache_path=_cache_path,
        )
    else:
        if not endpoint_url:
            raise ValueError("Either --endpoint or --ports-json must be provided")
        class_index, cost_stats = build_class_index_from_endpoints(
            sorted(entity_iris_set),
            endpoint_url,
            batch_size=batch_size,
            timeout=timeout,
            cache_path=_cache_path,
        )

    # 4. Optionally enrich input files in-place
    enrichment_stats_total: dict[str, Any] = {
        "entities_total": 0,
        "entities_enriched": 0,
        "entities_not_found": 0,
        "classes_added": 0,
        "elapsed_s": 0.0,
    }
    if enrich_in_place:
        for p_str, raw in zip(input_paths, input_docs, strict=False):
            _, e_stats = enrich_jsonld_with_classes(raw, class_index)
            for k in ("entities_total", "entities_enriched", "entities_not_found", "classes_added"):
                enrichment_stats_total[k] = enrichment_stats_total.get(k, 0) + e_stats.get(k, 0)
            enrichment_stats_total["elapsed_s"] += e_stats.get("elapsed_s", 0.0)
            _Path(p_str).with_suffix(".enriched.jsonld").write_text(
                _json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8"
            )

    # 5. Derive class mappings
    class_pairs, derivation_stats = derive_class_mappings(
        all_instance_edges,
        class_index,
        min_instance_count=min_instance_count,
        min_confidence=min_confidence,
    )

    # 6. Serialise ClassDerivedMapping
    out_p = _Path(output_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    _src_name = source_name or out_p.stem
    _src_type = next(iter(source_types), "unknown")
    class_edges = [pair.to_mapping_edge() for pair in class_pairs]
    about = AboutMetadata.build(
        dataset_name=_src_name,
        pattern_count=len(class_edges),
        strategy="class_derived",
    )
    mapping = ClassDerivedMapping(
        edges=class_edges,
        about=about,
        source_mapping_type=_src_type,
        source_mapping_files=source_files,
        derivation_stats=derivation_stats,
        enrichment_stats=enrichment_stats_total,
        class_index_endpoint=endpoint_url,
    )
    out_p.write_text(
        _json.dumps(mapping.to_jsonld(), indent=2, ensure_ascii=False), encoding="utf-8"
    )

    elapsed = _time.monotonic() - t0

    # 7. Session report
    report: dict[str, Any] = {
        "source_name": _src_name,
        "timestamp": _dt.utcnow().isoformat() + "Z",
        "source_mapping_type": _src_type,
        "endpoint_url": endpoint_url,
        "cost": cost_stats,
        "enrichment": enrichment_stats_total,
        "derivation": derivation_stats,
        "elapsed_s": elapsed,
    }
    report_dir = _Path("docker/mappings/class_derived/.session_reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    ts = _dt.utcnow().strftime("%Y%m%dT%H%M%SZ")
    (report_dir / f"{_src_name}_{ts}.json").write_text(
        _json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return report
