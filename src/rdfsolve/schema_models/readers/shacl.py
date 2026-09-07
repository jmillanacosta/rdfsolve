"""SHACL to MinedSchema conversion functions."""

from __future__ import annotations

from rdflib import Graph

from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclNodeShape, ShaclPropertyShape, ShaclShapesGraph


def shacl_to_minedschema(shacl_ttl: str) -> MinedSchema:
    """Parse SHACL Turtle into MinedSchema.

    Args:
        shacl_ttl: SHACL shapes in Turtle format

    Returns:
        MinedSchema with patterns reconstructed from shapes

    Example:
        >>> shacl_ttl = Path("shapes.ttl").read_text()
        >>> schema = shacl_to_minedschema(shacl_ttl)
        >>> print(len(schema.patterns))
        10
    """
    g = Graph()
    g.parse(data=shacl_ttl, format="turtle")

    shapes = ShaclShapesGraph.from_rdf(g)
    patterns = []

    for ns in shapes.node_shapes:
        subject_class = ns.target_class
        if not subject_class:
            continue

        property_shapes = [
            option.model_copy(update={"path": shape.path})
            for shape in ns.property_shapes
            for option in (shape.alternatives or [shape])
        ]
        for ps in property_shapes:
            if not ps.path:
                continue

            # Determine object class
            if ps.class_constraint:
                object_class = ps.class_constraint
                datatype = None
            elif ps.datatype:
                object_class = "Literal"
                datatype = ps.datatype
            elif ps.node_kind == "Literal":
                object_class = "Literal"
                datatype = None
            elif ps.node_kind == "BlankNode":
                object_class = "BlankNode"
                datatype = None
            else:
                object_class = "Resource"
                datatype = None

            patterns.append(
                SchemaPattern(
                    subject_class=subject_class,
                    property_uri=ps.path,
                    object_class=object_class,
                    datatype=datatype,
                )
            )

    return MinedSchema(
        patterns=patterns,
        about=AboutMetadata.build(
            pattern_count=len(patterns),
            class_count=len(shapes.node_shapes),
        ),
    )
