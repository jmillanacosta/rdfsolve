"""Compare dataset-scoped class graphs while retaining each evidence kind."""

from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Mapping, Sequence
from itertools import combinations
from typing import TYPE_CHECKING, Any

from rdfsolve.analysis.overlap import jaccard_similarity
from rdfsolve.analysis.schema import extract_class_set, extract_predicate_set

if TYPE_CHECKING:
    from rdfsolve.mappings.models.core import MappingEdge
    from rdfsolve.mappings.routes import RouteEvidence
    from rdfsolve.mappings.signatures import Link, LinkEvidence
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


# Levels of evidence of an edge, from the weakest. confirmed: seen in full on the data (a pattern
# with its count, a path that instances follow, a link of which every value was read and the
# share is at least the threshold). tested: checked on part of the data or with a weak result
# (a sampled link, or a link read in full with a smaller share). plausible: stated or composed,
# not tested on the data (the same class in two datasets, an external mapping, a proposed link,
# a path composed from the schema).
LEVELS = ("plausible", "tested", "confirmed")


def build_connectivity(
    schemas: Mapping[str, MinedSchema],
    *,
    class_mappings: Sequence[MappingEdge] = (),
    links: Sequence[LinkEvidence] = (),
    candidates: Sequence[Link] = (),
    routes: Sequence[RouteEvidence] = (),
    min_share: float = 0.5,
) -> Any:
    """Build a directed multigraph with dataset-qualified class nodes.

    Shared vocabulary, observed predicates, paths over several steps, explicit class mappings,
    verified links (rdfsolve.mappings.signatures.verify) and proposed links (*candidates*)
    remain distinct edges, each with its level of evidence (LEVELS). A link of which no value
    was found is left out. A route across two datasets that was tested on the data (*routes*,
    rdfsolve.mappings.routes) is one edge. No transitive mapping inference is run.
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
                    left,
                    right,
                    kind="schema",
                    predicate=pattern.property_uri,
                    count=pattern.count,
                    evidence="confirmed",
                )
        _add_paths(graph, dataset, schema)
    for cls, datasets in sorted(occurrences.items()):
        for first, second in combinations(datasets, 2):
            graph.add_edge(
                (first, cls),
                (second, cls),
                kind="shared_class",
                predicate=None,
                directed=False,
                evidence="plausible",
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
                "evidence": "plausible",
            },
        )
    for evidence in links:
        link = evidence.link
        if not evidence.found:
            continue
        share = evidence.share or 0.0
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
                "complete": evidence.complete,
                "flags": evidence.flags,
                "evidence": "confirmed" if evidence.complete and share >= min_share else "tested",
            },
        )
    for link in candidates:
        add_evidence(
            "candidate_link",
            (link.source, link.source_class),
            (link.target, link.target_class),
            link.property,
            {
                "link_kind": link.kind,
                "identifier_type": link.identifier_type,
                "target_property": link.target_property,
                "evidence": "plausible",
            },
        )
    for tested in routes:
        route = tested.route
        left, right = (route.link.source, route.start_class), (route.link.target, route.end_class)
        if not tested.matched or left not in graph or right not in graph:
            continue
        graph.add_edge(
            left,
            right,
            kind="route",
            predicate=route.link.property,
            before=[step.property_uri for step in route.before],
            after=[step.property_uri for step in route.after],
            identifier_type=route.link.identifier_type,
            starts=tested.starts,
            matched=tested.matched,
            complete=tested.complete,
            resolved_construct=route.resolved,
            flags=tested.flags,
            evidence=tested.level,
        )
    return graph


def _add_paths(graph: Any, dataset: str, schema: MinedSchema) -> None:
    """Add the paths over several steps of a schema that end in a class, each as one edge.

    A path that instances follow is confirmed; a path composed from the schema and not tested
    is plausible; a tested path that no instance follows is left out.
    """
    if schema.navigation is None:
        return
    for route in schema.navigation.paths:
        start, end = (
            (dataset, route.steps[0].subject_class),
            (dataset, route.steps[-1].object_class),
        )
        if route.instance_support in {"no_match", "no_sources"} or end not in graph:
            continue
        graph.add_edge(
            start,
            end,
            kind="path",
            predicate=None,
            steps=[step.property_uri for step in route.steps],
            via=[step.object_class for step in route.steps[:-1]],
            matched_sources=route.matched_sources,
            source_count=route.source_count,
            evidence="confirmed" if route.instance_support == "matched" else "plausible",
        )


def best_route(graph: Any, source: Any, target: Any) -> dict[str, Any] | None:
    """Return the route from *source* to *target* with the strongest edges, then the fewest.

    The route is looked for with confirmed edges only, then with tested edges too, then with
    all edges; among the routes of a level the one with the fewest edges is taken. A route of
    one edge has the level of that edge. A route of several edges is a composition: no instance
    is known to follow all of it, so its evidence is plausible, and weakest_segment gives the
    level of its weakest edge. An edge can carry the flags of a link over an identity property
    that failed a check (a gene stated to be the same as a protein); at each level a route
    without flagged edges is taken first, and flagged says whether the route has such an edge.
    None when there is no route.
    """
    import networkx as nx

    if source not in graph or target not in graph:
        return None
    # At each level, a route without flagged edges is looked for first.
    searches = [(f, a) for f in reversed(range(len(LEVELS))) for a in (False, True)]
    for floor, with_flagged in searches:
        simple: Any = nx.DiGraph()
        simple.add_nodes_from((source, target))
        for left, right, data in graph.edges(data=True):
            if LEVELS.index(data["evidence"]) < floor or (data.get("flags") and not with_flagged):
                continue
            ends = (
                ((left, right), (right, left))
                if data.get("directed") is False
                else ((left, right),)
            )
            for a, b in ends:
                kept = simple.get_edge_data(a, b)
                if kept is None or LEVELS.index(data["evidence"]) > LEVELS.index(
                    kept["edge"]["evidence"]
                ):
                    simple.add_edge(a, b, edge=data)
        try:
            nodes = nx.shortest_path(simple, source, target)
        except nx.NetworkXNoPath:
            continue
        edges = [simple[a][b]["edge"] for a, b in itertools.pairwise(nodes)]
        if not edges:
            return None
        weakest = min((e["evidence"] for e in edges), key=LEVELS.index)
        composed = len(edges) > 1
        return {
            "nodes": nodes,
            "edges": edges,
            "composed": composed,
            "weakest_segment": weakest,
            "evidence": "plausible" if composed else weakest,
            "flagged": any(e.get("flags") for e in edges),
        }
    return None
