"""Compare dataset-scoped class graphs while retaining each evidence kind."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from itertools import combinations
from typing import TYPE_CHECKING, Any

from rdfsolve.analysis.overlap import jaccard_similarity
from rdfsolve.analysis.schema import extract_class_set, extract_predicate_set

if TYPE_CHECKING:
    from rdfsolve.mappings.derivation import ClassPair
    from rdfsolve.mappings.models.core import MappingEdge
    from rdfsolve.schema_models.core import MinedSchema


def compare_schemas(schemas: Mapping[str, MinedSchema]) -> list[dict[str, Any]]:
    """Return vocabulary overlap for every dataset pair, including zero overlap."""
    classes = {name: extract_class_set(s) for name, s in schemas.items()}
    predicates = {name: extract_predicate_set(s) for name, s in schemas.items()}
    return [
        {
            "source": left,
            "target": right,
            "shared_classes": len(classes[left] & classes[right]),
            "shared_predicates": len(predicates[left] & predicates[right]),
            "class_jaccard": jaccard_similarity(classes[left], classes[right]),
            "property_jaccard": jaccard_similarity(predicates[left], predicates[right]),
        }
        for left, right in combinations(sorted(schemas), 2)
    ]


def build_connectivity(
    schemas: Mapping[str, MinedSchema],
    *,
    class_mappings: Sequence[MappingEdge] = (),
    associations: Sequence[ClassPair] = (),
) -> Any:
    """Build a directed multigraph with dataset-qualified class nodes.

    Shared vocabulary, observed predicates, explicit class mappings and entity
    associations remain distinct edges. No transitive mapping inference is run.
    """
    import networkx as nx

    graph: Any = nx.MultiDiGraph()
    occurrences: defaultdict[str, list[str]] = defaultdict(list)
    for dataset, schema in sorted(schemas.items()):
        for cls in sorted(extract_class_set(schema)):
            graph.add_node((dataset, cls), dataset=dataset, iri=cls)
            occurrences[cls].append(dataset)
        for pattern in schema.patterns:
            left, right = (dataset, pattern.subject_class), (dataset, pattern.object_class)
            if left in graph and right in graph:
                graph.add_edge(
                    left, right, kind="schema", predicate=pattern.property_uri, count=pattern.count
                )
    for cls, datasets in sorted(occurrences.items()):
        for first, second in combinations(datasets, 2):
            graph.add_edge(
                (first, cls), (second, cls), kind="shared_class", predicate=None, directed=False
            )

    def add_evidence(
        kind: str, edge: MappingEdge | ClassPair, predicate: str | None, evidence: dict[str, Any]
    ) -> None:
        """Add one evidence edge between two schema class nodes."""
        left = (edge.source_dataset, edge.source_class)
        right = (edge.target_dataset, edge.target_class)
        if left not in graph or right not in graph:
            raise ValueError(
                f"{kind} endpoints are absent from the supplied schemas: {left}, {right}"
            )
        graph.add_edge(left, right, kind=kind, predicate=predicate, **evidence)

    for mapping in class_mappings:
        add_evidence(
            "explicit_mapping",
            mapping,
            mapping.predicate,
            {
                "confidence": mapping.confidence,
                "justification": mapping.mapping_justification,
                "mapping_source": mapping.mapping_source,
            },
        )
    for pair in associations:
        add_evidence(
            "entity_association",
            pair,
            pair.class_relation,
            {
                "supporting_entity_predicates": pair.supporting_entity_predicates,
                "derivation_method": pair.derivation_method,
                "instance_count": pair.instance_count,
                "source_coverage": pair.source_coverage,
                "target_coverage": pair.target_coverage,
                "coverage_basis": "indexed entities",
            },
        )
    return graph
