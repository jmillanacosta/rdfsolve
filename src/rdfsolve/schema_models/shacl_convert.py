"""SHACL to MinedSchema conversion functions."""

from __future__ import annotations

from rdflib import Graph

from rdfsolve.schema_models.core import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclNodeShape, ShaclPropertyShape, ShaclShapesGraph


def minedschema_to_shacl(
    schema: MinedSchema,
    base_uri: str = "http://example.org/shapes/",
) -> ShaclShapesGraph:
    """Convert MinedSchema to SHACL shapes.

    Creates one NodeShape per subject class with PropertyShapes for each pattern.

    Args:
        schema: MinedSchema to convert
        base_uri: Base URI for shape URIs

    Returns:
        ShaclShapesGraph with NodeShape per class
    """
    from collections import defaultdict
    from hashlib import md5

    # Group patterns by subject class
    by_subject: dict[str, list[SchemaPattern]] = defaultdict(list)
    for pat in schema.patterns:
        by_subject[pat.subject_class].append(pat)

    node_shapes = []
    for subject_class in sorted(by_subject.keys()):
        cls_hash = md5(subject_class.encode(), usedforsecurity=False).hexdigest()[:8]

        property_shapes = []
        for pat in by_subject[subject_class]:
            prop_hash = md5(pat.property_uri.encode(), usedforsecurity=False).hexdigest()[:8]

            ps = ShaclPropertyShape(
                uri=f"{base_uri}ps-{cls_hash}-{prop_hash}",
                path=pat.property_uri,
                name=pat.property_label,
                description=schema.enrichment.description(pat.property_uri),
            )

            if pat.object_class == "Literal":
                ps.node_kind = "Literal"
                if pat.datatype:
                    ps.datatype = pat.datatype
            elif pat.object_class == "Resource":
                ps.node_kind = "IRI"
            elif pat.object_class == "BlankNode":
                ps.node_kind = "BlankNode"
            else:
                ps.node_kind = "IRI"
                ps.class_constraint = pat.object_class

            property_shapes.append(ps)

        node_shapes.append(
            ShaclNodeShape(
                uri=f"{base_uri}ns-{cls_hash}",
                target_class=subject_class,
                name=by_subject[subject_class][0].subject_label,
                description=schema.enrichment.description(subject_class),
                property_shapes=property_shapes,
            )
        )

    return ShaclShapesGraph(node_shapes=node_shapes, base_uri=base_uri)


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

        for ps in ns.property_shapes:
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
