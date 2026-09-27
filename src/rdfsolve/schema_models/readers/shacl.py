"""Read SHACL profiles without treating constraints as observations."""

from __future__ import annotations

import logging

from rdflib import RDF, RDFS, Graph, URIRef

from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.collections import CollectionProfile
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import SchemaEnrichment
from rdfsolve.schema_models.metadata import RetainedMetadata
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclPropertyShape, ShaclShapesGraph

logger = logging.getLogger(__name__)


def _list_members(option: ShaclPropertyShape) -> list[ShaclPropertyShape] | None:
    """Return the member constraints of an RDF list option, or None for another option.

    A list option is a blank node whose one nested property has the path rdf:rest*/rdf:first.
    """
    from rdflib import RDF

    from rdfsolve.schema_models.paths import PropertyPath

    members = PropertyPath.from_sparql("rdf:rest*/rdf:first", {"rdf": str(RDF)})
    if option.node_kind != "BlankNode" or len(option.properties) != 1:
        return None
    (nested,) = option.properties
    if nested.path != members:
        return None
    return nested.alternatives or [nested]


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
    schema.source_metadata = RetainedMetadata(
        rdf=graph.serialize(format="turtle"),
        format="turtle",
        scope="supplied SHACL document",
    )
    schema.shapes = shapes
    schema.prefixes = {
        prefix: str(namespace) for prefix, namespace in shapes.to_rdf(graph).namespaces()
    }
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
    lists: list[CollectionProfile] = []
    for shape in shapes.node_shapes:
        if (
            not shape.target_class
            and shape.uri
            and not shape.uri.startswith("_:")
            and any(
                RDFS.Class in graph.transitive_objects(kind, RDFS.subClassOf)
                for kind in graph.objects(URIRef(shape.uri), RDF.type)
            )
        ):
            shape.target_class = shape.uri
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
                listed = _list_members(option)
                if listed is not None:
                    lists.append(
                        CollectionProfile(
                            subject_class=shape.target_class,
                            property_uri=prop.path,
                            member_types=sorted(
                                {m.class_constraint for m in listed if m.class_constraint}
                            ),
                            member_kinds=sorted(
                                {"IRI", "BlankNode"}
                                if any(m.class_constraint for m in listed)
                                else set()
                                | (
                                    {"Literal"}
                                    if any(m.node_kind == "Literal" for m in listed)
                                    else set()
                                )
                            ),
                            member_datatypes=sorted({m.datatype for m in listed if m.datatype}),
                            evidence_source="shacl",
                        )
                    )
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
    if lists:
        schema.collections = [*(schema.collections or []), *lists]
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
