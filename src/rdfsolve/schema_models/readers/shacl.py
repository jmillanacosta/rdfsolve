"""Read SHACL profiles without treating constraints as observations."""

from __future__ import annotations

import logging

from rdflib import RDF, Graph

from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import SchemaEnrichment
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclShapesGraph

logger = logging.getLogger(__name__)


def shacl_to_minedschema(shacl_ttl: str) -> MinedSchema:
    """Keep source shapes, including paths and cardinalities.

    Only simple value constraints also become patterns. Cardinalities
    count values per focus node, not triples in the dataset.
    """
    from rdfsolve.schema_models.readers.void import (
        VOID,
        void_graph_to_minedschema,
        warn_untyped_partitions,
    )

    graph = Graph().parse(data=shacl_ttl, format="turtle")
    shapes = ShaclShapesGraph.from_rdf(graph)
    schema = (
        void_graph_to_minedschema(graph, report_untyped=False)
        if (None, RDF.type, VOID.Dataset) in graph
        else MinedSchema(about=AboutMetadata.build())
    )
    schema.shapes = shapes
    keys = {(p.subject_class, p.property_uri, p.object_class, p.datatype) for p in schema.patterns}
    unrepresented = 0
    kinds = {
        "IRI": ("Resource",),
        "Literal": ("Literal",),
        "BlankNode": ("BlankNode",),
        "BlankNodeOrIRI": ("BlankNode", "Resource"),
        "BlankNodeOrLiteral": ("BlankNode", "Literal"),
        "IRIOrLiteral": ("Resource", "Literal"),
    }
    for shape in shapes.node_shapes:
        if not shape.target_class:
            continue
        for prop in shape.property_shapes:
            if not prop.path:
                continue
            if not isinstance(prop.path, str) or (
                prop.alternatives and (prop.class_constraint or prop.datatype or prop.node_kind)
            ):
                unrepresented += 1
                continue
            for option in prop.alternatives or [prop]:
                if option.alternatives:
                    unrepresented += 1
                    continue
                datatype = option.datatype
                object_classes: tuple[str, ...]
                if option.class_constraint:
                    object_classes = (option.class_constraint,)
                elif datatype:
                    object_classes = ("Literal",)
                elif option.node_kind:
                    object_classes = kinds[option.node_kind]
                else:
                    unrepresented += 1
                    continue
                for object_class in object_classes:
                    key = (shape.target_class, prop.path, object_class, datatype)
                    if key in keys:
                        continue
                    keys.add(key)
                    schema.patterns.append(
                        SchemaPattern(
                            subject_class=shape.target_class,
                            property_uri=prop.path,
                            object_class=object_class,
                            datatype=datatype,
                            subject_label=shape.name,
                            property_label=prop.name,
                            evidence_source="shacl",
                        )
                    )
    if unrepresented:
        logger.warning(
            "Retain %d SHACL branches in schema.shapes, not as triple patterns "
            "(compound paths, missing value constraints, or intersections).",
            unrepresented,
        )
    warn_untyped_partitions(graph, schema.patterns)
    schema.about.pattern_count = len(schema.patterns)
    schema.about.class_count = len(schema.get_classes())
    schema.about.property_count = len(schema.get_properties())
    schema.enrichment = SchemaEnrichment.from_rdf_graph(
        graph, schema.get_classes(), schema.get_properties()
    )
    return schema
