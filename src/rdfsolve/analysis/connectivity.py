"""Compare dataset-scoped class graphs while retaining each evidence kind."""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations

from rdfsolve.analysis.overlap import jaccard_similarity
from rdfsolve.analysis.schema import extract_class_set, extract_predicate_set


def compare_schemas(schemas):
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


def build_connectivity(schemas, *, class_mappings=(), associations=()):
    """Build a directed multigraph with dataset-qualified class nodes.

    Shared vocabulary, observed predicates, explicit class mappings and entity
    associations remain distinct edges. No transitive mapping inference is run.
    """
    import networkx as nx

    graph = nx.MultiDiGraph()
    occurrences = defaultdict(list)
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
        for left, right in combinations(datasets, 2):
            graph.add_edge(
                (left, cls), (right, cls), kind="shared_class", predicate=None, directed=False
            )
    for kind, edges in (("explicit_mapping", class_mappings), ("entity_association", associations)):
        for edge in edges:
            left = (edge.source_dataset, edge.source_class)
            right = (edge.target_dataset, edge.target_class)
            if left not in graph or right not in graph:
                raise ValueError(
                    f"{kind} endpoints are absent from the supplied schemas: {left}, {right}"
                )
            if kind == "explicit_mapping":
                evidence = {
                    "confidence": edge.confidence,
                    "justification": edge.mapping_justification,
                    "mapping_source": edge.mapping_source,
                }
            else:
                evidence = {
                    "instance_count": edge.instance_count,
                    "source_coverage": edge.source_coverage,
                    "target_coverage": edge.target_coverage,
                    "coverage_basis": "indexed entities",
                }
            graph.add_edge(left, right, kind=kind, predicate=edge.predicate, **evidence)
    return graph
