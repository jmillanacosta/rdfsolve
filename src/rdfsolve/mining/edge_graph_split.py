"""Per-dataset schemas from one schema mined across several named graphs.

Providers such as PubChem split one knowledge graph into many downloadable
datasets whose entities point at each other: a substance links to a compound
typed in the compound dataset. Mining each dataset graph alone loses those
object types. The grouped pipeline therefore mines all graphs of a provider
together, which resolves types over their RDF merge while the counts phase
attributes every edge to the graph that holds it. This module cuts that group
schema into one schema per dataset graph.
"""

from __future__ import annotations

from collections.abc import Mapping
from uuid import uuid4

from rdfsolve._outcomes import QueryState
from rdfsolve.config import mint
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern

__all__ = ["split_by_edge_graph", "unattributed_patterns"]


def unattributed_patterns(schema: MinedSchema) -> list[SchemaPattern]:
    """Return patterns that carry no per-graph counts and so belong to no dataset."""
    return [
        pattern
        for pattern in [*schema.patterns, *(schema.term_patterns or [])]
        if not pattern.graphs
    ]


def split_by_edge_graph(
    schema: MinedSchema,
    graph_uri: str | list[str],
    dataset_name: str,
    *,
    declared_classes: frozenset[str] = frozenset(),
    class_entity_counts: Mapping[str, int] | None = None,
    class_entity_count_states: Mapping[str, QueryState] | None = None,
) -> MinedSchema:
    """Keep the patterns whose edges lie in the selected graphs, with their edge counts.

    Subject and object classes stay as resolved over all mined graphs. Group
    level statistics, navigation and retained source metadata describe the
    group, not this dataset, and are left out. The dataset gets its own
    snapshot identity; entity counts must be measured in its graph by the
    caller.
    """
    graphs = [graph_uri] if isinstance(graph_uri, str) else list(dict.fromkeys(graph_uri))
    patterns = _select_graphs(schema.patterns, graphs)
    raw_patterns = (
        _select_graphs(schema.raw_patterns, graphs) if schema.raw_patterns is not None else None
    )
    classes = {p.subject_class for p in patterns} | {
        p.object_class for p in patterns if p.object_class not in _SENTINEL_OBJECTS
    }
    properties = {p.property_uri for p in patterns}
    about = schema.about
    retrieved = about.retrieved_at or about.started_at or about.generated_at
    about = about.model_copy(
        update={
            "schema_id": str(uuid4()),
            "dataset_name": dataset_name,
            "graph_uris": graphs,
            "type_graph_uris": about.type_graph_uris or about.graph_uris,
            "snapshot_id": mint("snapshot", dataset_name, retrieved) if retrieved else None,
            "snapshot_identity_basis": "retrieval_record" if retrieved else None,
            # A content hash of the group input does not identify this dataset.
            "content_sha256": None,
            "schema_uri": mint("schema", dataset_name),
            "void_uri": mint("void", dataset_name),
            "report_uri": mint("report", dataset_name),
            "linkml_uri": mint("linkml", dataset_name),
            "pattern_count": len(patterns),
            "class_count": len(classes),
            "declared_class_count": len(classes & declared_classes),
            "used_type_count": len(classes - declared_classes),
            "property_count": len(properties),
            "triple_count_estimate": None,
            "distinct_subject_count": None,
            "distinct_predicate_count": None,
            "class_entity_counts": dict(class_entity_counts or {}),
            "class_entity_count_states": dict(class_entity_count_states or {}),
        }
    )
    return schema.model_copy(
        update={
            "patterns": patterns,
            "raw_patterns": raw_patterns,
            "term_patterns": _select_graphs(schema.term_patterns, graphs)
            if schema.term_patterns is not None
            else None,
            "about": about,
            "structural_patterns": [p for p in schema.structural_patterns if p.graph_uri in graphs]
            if schema.structural_patterns is not None
            else None,
            "collections": [p for p in schema.collections if p.graph_uri in graphs]
            if schema.collections is not None
            else None,
            "navigation": None,
            "source_metadata": None,
        }
    )


def _select_graphs(source: list[SchemaPattern], graphs: list[str]) -> list[SchemaPattern]:
    """Select edge counts for the given graphs."""
    patterns: list[SchemaPattern] = []
    for pattern in source:
        attributed = {g: n for g, n in (pattern.graphs or {}).items() if g in graphs}
        count = sum(attributed.values())
        if not count:
            continue
        only_here = len(graphs) == 1 and set(pattern.graphs or {}) == set(graphs)
        patterns.append(
            pattern.model_copy(
                update={
                    "count": count,
                    "count_semantics": "upper_bound"
                    if pattern.count_semantics == "upper_bound"
                    else "triples_in_graph"
                    if len(graphs) == 1
                    else "quad_occurrences",
                    "graphs": attributed,
                    "distinct_subjects": pattern.distinct_subjects if only_here else None,
                    "distinct_objects": pattern.distinct_objects if only_here else None,
                }
            )
        )
    return patterns
