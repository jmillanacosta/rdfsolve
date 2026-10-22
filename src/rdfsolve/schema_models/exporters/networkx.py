"""Export typed class relationships as a NetworkX graph."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS, UNTYPED_SUBJECTS_LABEL

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema


def to_networkx(schema: MinedSchema) -> Any:
    """Export as a typed-object ``nx.MultiDiGraph``.

    Nodes are class URIs.  Each typed-object pattern becomes a
    directed edge.  Literal/Resource sentinels are excluded. The subjects without a type
    (untyped patterns) are one node, "untyped subjects" (attribute untyped=True), not a class.
    """
    try:
        import networkx as _nx
    except ImportError as exc:
        raise ImportError(
            "networkx is required for to_networkx(); install it with: pip install networkx",
        ) from exc

    graph: Any = _nx.MultiDiGraph()
    dataset = schema.about.dataset_name or ""

    for pat in schema.patterns:
        if pat.object_class in _SENTINEL_OBJECTS:
            continue
        subject = UNTYPED_SUBJECTS_LABEL if pat.untyped_subject else pat.subject_class
        if pat.untyped_subject and subject not in graph:
            graph.add_node(subject, dataset=dataset, label=subject, untyped=True)
        for uri, label in (
            (subject, pat.subject_label),
            (pat.object_class, pat.object_label),
        ):
            if uri not in graph:
                graph.add_node(
                    uri,
                    dataset=dataset,
                    label=label or "",
                )
        graph.add_edge(
            subject,
            pat.object_class,
            predicate=pat.property_uri,
            dataset=dataset,
            count=pat.count,
        )
    return graph


# JSON-LD export
