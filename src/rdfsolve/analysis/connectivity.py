"""Compare dataset-scoped class graphs while retaining each evidence kind."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from itertools import combinations
from typing import TYPE_CHECKING, Any

from rdfsolve.analysis.overlap import jaccard_similarity
from rdfsolve.analysis.schema import extract_class_set, extract_predicate_set

if TYPE_CHECKING:
    from rdfsolve.mappings.models.core import MappingEdge
    from rdfsolve.mappings.signatures import LinkEvidence
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
    links: Sequence[LinkEvidence] = (),
) -> Any:
    """Build a directed multigraph with dataset-qualified class nodes.

    Shared vocabulary, observed predicates, explicit class mappings and verified links
    (rdfsolve.mappings.signatures.verify) remain distinct edges. No transitive mapping inference is run.
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
        kind: str,
        left: tuple[str, str | None],
        right: tuple[str, str | None],
        predicate: str | None,
        evidence: dict[str, Any],
    ) -> None:
        """Add one evidence edge between two schema class nodes."""
        if left not in graph or right not in graph:
            raise ValueError(
                f"{kind} endpoints are absent from the supplied schemas: {left}, {right}"
            )
        graph.add_edge(left, right, kind=kind, predicate=predicate, **evidence)

    for mapping in class_mappings:
        add_evidence(
            "explicit_mapping",
            (mapping.source_dataset, mapping.source_class),
            (mapping.target_dataset, mapping.target_class),
            mapping.predicate,
            {
                "confidence": mapping.confidence,
                "justification": mapping.mapping_justification,
                "mapping_source": mapping.mapping_source,
            },
        )
    for evidence in links:
        link = evidence.link
        add_evidence(
            "verified_link",
            (link.source, link.source_class),
            (link.target, link.target_class),
            link.property,
            {
                "link_kind": link.kind,
                "identifier_type": link.identifier_type,
                "target_property": link.target_property,
                "sampled": evidence.sampled,
                "found": evidence.found,
                "share": evidence.share,
                "target_forms": evidence.target_forms,
            },
        )
    return graph
