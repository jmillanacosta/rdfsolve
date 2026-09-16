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
    *,
    activate_observed: bool = False,
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
        return _complete_shapes(schema, schema.shapes.model_copy(deep=True), base_uri)

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
                deactivated=not activate_observed,
                name=by_subject[subject_class][0].subject_label,
                description=" ".join(
                    filter(
                        None,
                        [
                            schema.enrichment.description(subject_class),
                            (
                                "Observed value-type template, not a source constraint. "
                                "No required fields or per-entity cardinalities were inferred."
                            ),
                        ],
                    )
                ),
                property_shapes=property_shapes,
            )
        )

    return _complete_shapes(
        schema, ShaclShapesGraph(node_shapes=node_shapes, base_uri=base_uri), base_uri
    )


def _complete_shapes(
    schema: MinedSchema, shapes: ShaclShapesGraph, base_uri: str
) -> ShaclShapesGraph:
    """Add namespace declarations and candidate navigation paths."""
    import logging
    from collections import defaultdict
    from hashlib import sha256

    from rdfsolve.schema_models.navigation import NavigationPath

    shapes.declare_prefixes(schema.get_prefixes(), resource=base_uri)
    if schema.navigation is None or not schema.navigation.paths:
        return shapes
    grouped: dict[tuple[str, tuple[str, ...]], list[NavigationPath]] = defaultdict(list)
    for route in schema.navigation.paths:
        grouped[
            (
                route.steps[0].subject_class,
                tuple(step.property_uri for step in route.steps),
            )
        ].append(route)

    by_class: dict[str, list[ShaclPropertyShape]] = defaultdict(list)
    for (start, predicates), routes in sorted(grouped.items()):
        descriptions = []
        for route in routes:
            steps = []
            for step in route.steps:
                value = step.datatype or step.object_class
                count = str(step.count) if step.count is not None else "unknown"
                steps.append(
                    f"{step.subject_label or step.subject_class} --{step.property_label or step.property_uri}--> {step.object_label or value} (edge triples: {count})"
                )
            descriptions.append("; ".join(steps))
        identifier = sha256(repr((start, predicates)).encode()).hexdigest()[:20]
        by_class[start].append(
            ShaclPropertyShape(
                uri=f"{base_uri}route-{identifier}",
                path=routes[0].property_path(),
                name="; ".join(sorted({route.label() for route in routes})),
                description="Candidate class-qualified routes: "
                + " | ".join(sorted(set(descriptions))),
            )
        )

    for class_iri, properties in sorted(by_class.items()):
        identifier = sha256(class_iri.encode()).hexdigest()[:16]
        uri = f"{base_uri}navigation-{identifier}"
        omitted = "; ".join(
            f"{hops} hops: {counts.get(class_iri, 0)} omitted"
            for hops, counts in sorted(schema.navigation.omitted_by_class.items())
        )
        shapes.node_shapes = [shape for shape in shapes.node_shapes if shape.uri != uri]
        shapes.node_shapes.append(
            ShaclNodeShape(
                uri=uri,
                target_class=class_iri,
                name="Paths from "
                + next(
                    (r.steps[0].subject_label or class_iri)
                    for r in schema.navigation.paths
                    if r.steps[0].subject_class == class_iri
                ),
                description=(
                    "Schema-composed paths. Instance support and coverage are unknown. "
                    "Edge triple counts are not joined counts or per-entity cardinalities. "
                    "The path omits intermediate class filters listed in each description. "
                    "Value types are candidate endpoints, not enforced constraints. " + omitted
                ),
                property_shapes=properties,
            )
        )
    for route in schema.navigation.paths:
        if route.instance_support != "matched":
            continue
        child = None
        for step in reversed(route.steps):
            value = ShaclPropertyShape(path="")
            if step.object_class == "Literal":
                value.node_kind, value.datatype = "Literal", step.datatype
            elif step.object_class in {"Resource", "BlankNode"}:
                value.node_kind = "IRI" if step.object_class == "Resource" else "BlankNode"
            else:
                value.class_constraint = step.object_class
            if child is not None:
                value.properties = [child]
            child = ShaclPropertyShape(
                path=step.property_uri,
                qualified_shape=value,
                qualified_min_count=1,
                name=step.property_label or step.property_uri,
            )
        identifier = sha256(repr(route.signature()).encode()).hexdigest()[:20]
        shapes.node_shapes.append(
            ShaclNodeShape(
                uri=f"{base_uri}observed-route-{identifier}",
                target_class=route.steps[0].subject_class,
                deactivated=True,
                property_shapes=[child],
                name=route.label(),
                description=f"Candidate query profile; {route.matched_sources}/{route.source_count} focus entries matched. "
                f"Observed endpoint degree {route.min_count}..{route.max_count}; {route.observed_at}. "
                "Qualified existence describes this route. It is not a dataset-wide requirement.",
            )
        )
    logging.getLogger(__name__).warning(
        "SHACL exports candidate paths and observed nested profiles. "
        "Keep canonical JSON for machine-readable filters, statistics, and route provenance."
    )
    return shapes
