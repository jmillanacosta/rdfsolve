"""Read simple class, datatype, and node-kind SHACL constraints."""

from __future__ import annotations

import warnings

from rdflib import Graph

from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclShapesGraph


def shacl_to_minedschema(shacl_ttl: str) -> MinedSchema:
    """Read supported shape constraints.

    Missing object constraints do not imply IRI values. Complex paths,
    nested alternatives, and intersections around alternatives are not
    supported. Counts cannot be recovered from shapes.
    """
    graph = Graph().parse(data=shacl_ttl, format="turtle")
    shapes = ShaclShapesGraph.from_rdf(graph)
    patterns = []
    ambiguous = 0
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
            if prop.alternatives and (prop.class_constraint or prop.datatype or prop.node_kind):
                raise ValueError(
                    "SHACL intersections around sh:or cannot be represented as patterns"
                )
            for option in prop.alternatives or [prop]:
                if option.alternatives:
                    raise ValueError("Nested sh:or cannot be represented as patterns")
                datatype = option.datatype
                object_classes: tuple[str, ...]
                if option.class_constraint:
                    object_classes = (option.class_constraint,)
                elif datatype:
                    object_classes = ("Literal",)
                elif option.node_kind:
                    object_classes = kinds[option.node_kind]
                else:
                    ambiguous += 1
                    continue
                for object_class in object_classes:
                    patterns.append(
                        SchemaPattern(
                            subject_class=shape.target_class,
                            property_uri=prop.path,
                            object_class=object_class,
                            datatype=datatype,
                            subject_label=shape.name,
                            property_label=prop.name,
                        )
                    )
    if ambiguous:
        warnings.warn(
            f"SHACL omits object constraints in {ambiguous} branches; "
            "these are not converted to patterns.",
            UserWarning,
            stacklevel=2,
        )
    schema = MinedSchema(patterns=patterns, about=AboutMetadata.build())
    schema.about.pattern_count = len(patterns)
    schema.about.class_count = len(schema.get_classes())
    schema.about.property_count = len(schema.get_properties())
    return schema
