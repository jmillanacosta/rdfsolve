"""Export observed patterns or a retained source SHACL profile."""

from __future__ import annotations

from rdflib import Graph

from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern
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
    import logging

    lost_counts = sum(
        pattern.count is not None
        and (
            pattern.object_class in ("Resource", "BlankNode")
            or (pattern.object_class == "Literal" and not pattern.datatype)
        )
        for pattern in schema.patterns
    )
    if lost_counts:
        logging.getLogger(__name__).warning(
            "SHACL plus VoID metadata cannot retain %d pattern counts with unspecified "
            "classes or datatypes. Keep canonical JSON; these are not sh:minCount values.",
            lost_counts,
        )
    if schema.shapes is not None:
        return _add_navigation(schema, schema.shapes.model_copy(deep=True), base_uri)

    from collections import defaultdict
    from hashlib import md5

    # Group patterns by subject class
    by_subject: dict[str, list[SchemaPattern]] = defaultdict(list)
    for pat in schema.patterns:
        by_subject[pat.subject_class].append(pat)

    node_shapes = []
    for subject_class in sorted(by_subject.keys()):
        cls_hash = md5(subject_class.encode(), usedforsecurity=False).hexdigest()[:8]

        by_property: dict[str, list[SchemaPattern]] = defaultdict(list)
        for pattern in by_subject[subject_class]:
            by_property[pattern.property_uri].append(pattern)
        property_shapes = []
        for prop, patterns in sorted(by_property.items()):
            prop_hash = md5(prop.encode(), usedforsecurity=False).hexdigest()[:8]
            options: dict[tuple[str, str | None], ShaclPropertyShape] = {}
            for pattern in patterns:
                option = ShaclPropertyShape(path="")
                if pattern.object_class == "Literal":
                    option.node_kind = "Literal"
                    option.datatype = pattern.datatype
                elif pattern.object_class == "Resource":
                    option.node_kind = "IRI"
                elif pattern.object_class == "BlankNode":
                    option.node_kind = "BlankNode"
                else:
                    option.node_kind = "BlankNodeOrIRI"
                    option.class_constraint = pattern.object_class
                options[(pattern.object_class, pattern.datatype)] = option
            alternatives = list(options.values())
            shape = (
                alternatives[0]
                if len(alternatives) == 1
                else ShaclPropertyShape(path="", alternatives=alternatives)
            )
            shape.path = prop
            shape.uri = f"{base_uri}ps-{cls_hash}-{prop_hash}"
            shape.name = patterns[0].property_label
            shape.description = schema.enrichment.description(prop)
            property_shapes.append(shape)
        node_shapes.append(
            ShaclNodeShape(
                uri=f"{base_uri}ns-{cls_hash}",
                target_class=subject_class,
                name=by_subject[subject_class][0].subject_label,
                description=schema.enrichment.description(subject_class),
                property_shapes=property_shapes,
            )
        )

    return _add_navigation(
        schema, ShaclShapesGraph(node_shapes=node_shapes, base_uri=base_uri), base_uri
    )


def _add_navigation(
    schema: MinedSchema, shapes: ShaclShapesGraph, base_uri: str
) -> ShaclShapesGraph:
    """Add nonrestrictive path hints. Zero is a lower bound, not an observed count."""
    import logging
    from collections import defaultdict
    from hashlib import sha256

    if schema.navigation is None or not schema.navigation.paths:
        return shapes
    by_class: dict[str, list[ShaclPropertyShape]] = defaultdict(list)
    for route in schema.navigation.paths:
        end = route.steps[-1]
        hint = ShaclPropertyShape(path="")
        if end.object_class == "Literal":
            hint.node_kind = "Literal"
            hint.datatype = end.datatype
        elif end.object_class in ("Resource", "BlankNode"):
            hint.node_kind = "IRI" if end.object_class == "Resource" else "BlankNode"
        else:
            hint.class_constraint = end.object_class
        by_class[route.steps[0].subject_class].append(
            ShaclPropertyShape(
                path=route.property_path(),
                name=" / ".join(step.property_label or step.property_uri for step in route.steps),
                description=(
                    "Schema-composed navigation candidate. Instance joins were not checked. "
                    "The zero qualified lower bound does not assert a path occurrence. "
                    "Intermediate class filters and step counts remain in canonical JSON."
                ),
                qualified_shape=hint,
                qualified_min_count=0,
            )
        )
    for class_iri, properties in sorted(by_class.items()):
        identifier = sha256(class_iri.encode()).hexdigest()[:16]
        shapes.node_shapes.append(
            ShaclNodeShape(
                uri=f"{base_uri}navigation-{identifier}",
                target_class=class_iri,
                property_shapes=properties,
            )
        )
    logging.getLogger(__name__).warning(
        "SHACL navigation keeps predicate sequences and final-value type hints, not intermediate "
        "class filters, step statistics, or schema-walk totals. Keep canonical JSON for those."
    )
    return shapes
