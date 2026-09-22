"""Aggregate entity correspondence into dataset-scoped class associations."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from rdfsolve.mappings.index import ClassIndex
from rdfsolve.mappings.models.core import MappingEdge


@dataclass(frozen=True)
class EntityLink:
    """Two entities in two datasets, related by an asserted predicate or by shared identity.

    predicate is None when no relation was asserted, for example when the same
    IRI occurs in both datasets.
    """

    source_entity: str
    target_entity: str
    source_dataset: str
    target_dataset: str
    predicate: str | None = None

    @classmethod
    def from_edge(cls, edge: MappingEdge) -> EntityLink:
        """Read an entity-level mapping assertion."""
        return cls(
            edge.source_class,
            edge.target_class,
            edge.source_dataset,
            edge.target_dataset,
            edge.predicate,
        )


@dataclass
class ClassPair:
    """One dataset-scoped class pair with its supporting entity evidence.

    instance_count counts distinct supporting entity pairs over all predicates.
    supporting_entity_predicates counts them per asserted entity predicate; one
    entity pair can count under several predicates. Shared identity asserts no
    predicate and adds no entry. These predicates relate the
    entities, not the classes. class_relation stays None unless a class-level
    assertion or a documented rule supplies it. Counts and coverage are support,
    not confidence. Coverage uses the supplied index.
    """

    source_class: str
    target_class: str
    source_dataset: str
    target_dataset: str
    instance_count: int = 0
    supporting_entity_predicates: dict[str, int] = field(default_factory=dict)
    source_entities: set[str] = field(default_factory=set)
    target_entities: set[str] = field(default_factory=set)
    source_coverage: float = 0.0
    target_coverage: float = 0.0
    class_relation: str | None = None
    derivation_method: str = "mapped_instance_types"


def derive_class_mappings(
    instance_edges: Sequence[MappingEdge | EntityLink],
    class_index: ClassIndex | dict[str, ClassIndex],
    *,
    min_instance_count: int = 1,
    derivation_method: str = "mapped_instance_types",
    include_witnesses: bool = False,
) -> tuple[list[ClassPair], dict[str, Any]]:
    """Aggregate distinct entity pairs using each dataset's own type evidence.

    Supply one index per dataset, or configure dataset_graphs on a combined index.
    These associations support retrieval and connectivity analysis; they are not
    class equivalence assertions or calibrated mapping confidence estimates.
    """
    if min_instance_count < 1:
        raise ValueError("min_instance_count must be positive")

    def classes(entity: str, dataset: str) -> set[str]:
        """Return the indexed classes of an entity in one dataset."""
        if isinstance(class_index, dict):
            index = class_index.get(dataset)
            return index.get_classes(entity) if index else set()
        return class_index.get_classes(entity, dataset)

    Key = tuple[str, str, str, str]
    pairs: dict[Key, ClassPair] = {}
    witnesses: dict[Key, set[tuple[str, str]]] = {}
    by_predicate: dict[Key, dict[str, set[tuple[str, str]]]] = {}
    skipped_source = skipped_target = processed = 0
    for item in instance_edges:
        edge = EntityLink.from_edge(item) if isinstance(item, MappingEdge) else item
        left = classes(edge.source_entity, edge.source_dataset)
        right = classes(edge.target_entity, edge.target_dataset)
        if not left:
            skipped_source += 1
            continue
        if not right:
            skipped_target += 1
            continue
        processed += 1
        for source in left:
            for target in right:
                key = (source, target, edge.source_dataset, edge.target_dataset)
                pair = pairs.setdefault(key, ClassPair(*key, derivation_method=derivation_method))
                witness = (edge.source_entity, edge.target_entity)
                witnesses.setdefault(key, set()).add(witness)
                predicates = by_predicate.setdefault(key, {})
                if edge.predicate is not None:
                    predicates.setdefault(edge.predicate, set()).add(witness)
                pair.source_entities.add(edge.source_entity)
                pair.target_entities.add(edge.target_entity)
    sizes: Counter[tuple[str, str]] = Counter()
    datasets = {p.source_dataset for p in pairs.values()} | {
        p.target_dataset for p in pairs.values()
    }
    for dataset in datasets:
        index = class_index[dataset] if isinstance(class_index, dict) else class_index
        for entity in index.entities:
            sizes.update((dataset, cls) for cls in classes(entity, dataset))
    for key, pair in pairs.items():
        pair.instance_count = len(witnesses[key])
        pair.supporting_entity_predicates = {
            predicate: len(found) for predicate, found in sorted(by_predicate[key].items())
        }
        pair.source_coverage = (
            len(pair.source_entities) / sizes[(pair.source_dataset, pair.source_class)]
        )
        pair.target_coverage = (
            len(pair.target_entities) / sizes[(pair.target_dataset, pair.target_class)]
        )
    result = sorted(
        (p for p in pairs.values() if p.instance_count >= min_instance_count),
        key=lambda p: (
            -p.instance_count,
            p.source_dataset,
            p.source_class,
            p.target_dataset,
            p.target_class,
        ),
    )
    report: dict[str, Any] = {
        "input_edges": len(instance_edges),
        "processed_edges": processed,
        "skipped_no_source_class": skipped_source,
        "skipped_no_target_class": skipped_target,
        "class_pairs_found": len(pairs),
        "output_edges": len(result),
        "supporting_entity_predicates": dict(
            Counter(predicate for p in result for predicate in p.supporting_entity_predicates)
        ),
        "coverage_basis": "entities in the supplied class index",
    }
    if include_witnesses:
        kept = {
            (pair.source_class, pair.target_class, pair.source_dataset, pair.target_dataset)
            for pair in result
        }
        rows: list[dict[str, Any]] = []
        for key in sorted(kept):
            predicate_sets = by_predicate.get(key, {})
            for source_entity, target_entity in sorted(witnesses.get(key, set())):
                supporting = sorted(
                    predicate
                    for predicate, found in predicate_sets.items()
                    if (source_entity, target_entity) in found
                )
                rows.append(
                    {
                        "source_class": key[0],
                        "target_class": key[1],
                        "source_dataset": key[2],
                        "target_dataset": key[3],
                        "source_entity": source_entity,
                        "target_entity": target_entity,
                        "supporting_entity_predicates": supporting,
                    }
                )
        report["witnesses"] = rows
    return result, report


def shared_entity_links(
    indices: dict[str, ClassIndex],
    *,
    min_instance_count: int = 1,
    include_witnesses: bool = False,
) -> tuple[list[ClassPair], dict[str, Any]]:
    """Find class associations supported by the same IRI in two datasets.

    A shared IRI is identity evidence, not an asserted relation. The links carry
    no predicate.
    """
    from itertools import combinations

    edges = [
        EntityLink(iri, iri, left, right)
        for left, right in combinations(sorted(indices), 2)
        for iri in sorted(indices[left].entities.keys() & indices[right].entities.keys())
    ]
    return derive_class_mappings(
        edges,
        indices,
        min_instance_count=min_instance_count,
        derivation_method="shared_entity_iri",
        include_witnesses=include_witnesses,
    )
