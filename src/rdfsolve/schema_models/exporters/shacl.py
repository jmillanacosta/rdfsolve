"""Export observed patterns or a retained source SHACL profile."""

from __future__ import annotations

from collections import defaultdict

from rdfsolve.schema_models.collections import CollectionProfile
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclNodeShape, ShaclPropertyShape, ShaclShapesGraph

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

ANY_LITERAL = "http://www.w3.org/2000/01/rdf-schema#Literal"


def shape_iri(dataset: str, cls: str, prop: str | None = None) -> str:
    """Return the IRI of the node shape of *cls* in *dataset*, or of its property shape of *prop*.

    The IRIs are hashes of the class and property IRIs, so a shape keeps its IRI across releases
    while its class and property persist (mappings to it, such as SSSOM rows, keep holding).
    """
    from hashlib import md5

    from rdfsolve.config import mint

    def short(iri: str) -> str:
        """Return the short hash of an IRI."""
        return md5(iri.encode(), usedforsecurity=False).hexdigest()[:8]

    base = mint("dataset", dataset or "unnamed") + "/shapes/"
    return f"{base}ns-{short(cls)}" if prop is None else f"{base}ps-{short(cls)}-{short(prop)}"


def minedschema_to_shacl(
    schema: MinedSchema,
    base_uri: str | None = None,
    *,
    activate_observed: bool = False,
) -> ShaclShapesGraph:
    """Convert MinedSchema to SHACL shapes.

    Creates one NodeShape per subject class with PropertyShapes for each pattern.

    Args:
        schema: MinedSchema to convert
        base_uri: Base IRI for shape IRIs; defaults to the dataset IRI

    Returns:
        ShaclShapesGraph with NodeShape per class
    """
    import logging

    from rdfsolve.config import mint

    base_uri = base_uri or mint("dataset", schema.about.dataset_name or "unnamed") + "/shapes/"
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
            "%d pattern rows describe objects without a class (IRIs without a type, blank "
            "nodes, literals without a datatype). Their triples and distinct subjects, the "
            "counts of each such kind for its class and property, are kept only in the JSON "
            "schema; VoID gives the distinct blank-node and literal objects of each class and "
            "property, and its distinct IRIs and triples only where no object has a class. "
            "These counts are not sh:minCount values.",
            lost_counts,
        )
    if schema.shapes is not None:
        return _complete_shapes(schema, schema.shapes.model_copy(deep=True), base_uri)

    from hashlib import md5

    # Group patterns by subject class; the patterns of untyped subjects by property.
    by_subject: dict[str, list[SchemaPattern]] = defaultdict(list)
    untyped: dict[str, list[SchemaPattern]] = defaultdict(list)
    for pat in schema.patterns:
        if pat.untyped_subject:
            untyped[pat.property_uri].append(pat)
        else:
            by_subject[pat.subject_class].append(pat)

    lists: dict[tuple[str, str], list[CollectionProfile]] = defaultdict(list)
    for profile in schema.collections or []:
        lists[(profile.subject_class, profile.property_uri)].append(profile)
    node_shapes = []
    for subject_class in sorted(by_subject.keys()):
        cls_hash = md5(subject_class.encode(), usedforsecurity=False).hexdigest()[:8]

        by_property: dict[str, list[SchemaPattern]] = defaultdict(list)
        for pattern in by_subject[subject_class]:
            by_property[pattern.property_uri].append(pattern)
        property_shapes = []
        for prop, patterns in sorted(by_property.items()):
            # No constraint on rdf:type: the schema keeps the rows of type values that are
            # declared classes and leaves out the others, so its rows do not give every type.
            if prop == RDF_TYPE:
                continue
            prop_hash = md5(prop.encode(), usedforsecurity=False).hexdigest()[:8]
            shape = _value_shape(patterns, lists.get((subject_class, prop), []))
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

    node_shapes += _untyped_shapes(schema, untyped, base_uri, activate_observed)
    return _complete_shapes(
        schema, ShaclShapesGraph(node_shapes=node_shapes, base_uri=base_uri), base_uri
    )


def _value_shape(
    patterns: list[SchemaPattern], profiles: list[CollectionProfile]
) -> ShaclPropertyShape:
    """Return the value constraint of the patterns of one property: one option, or sh:or."""
    options: dict[tuple[str, str | None], ShaclPropertyShape] = {}
    for pattern in patterns:
        option = ShaclPropertyShape(path="")
        if pattern.object_class == "Literal":
            option.node_kind = "Literal"
            option.datatype = None if pattern.datatype == ANY_LITERAL else pattern.datatype
        elif pattern.object_class == "Resource":
            option.node_kind = "IRI"
        elif pattern.object_class == "BlankNode":
            option.node_kind = "BlankNode"
        else:
            option.node_kind = "BlankNodeOrIRI"
            option.class_constraint = pattern.object_class
        options[(pattern.object_class, pattern.datatype)] = option
    for profile in profiles:
        options[("List", None)] = _list_option(profile)
    alternatives = list(options.values())
    return (
        alternatives[0]
        if len(alternatives) == 1
        else ShaclPropertyShape(path="", alternatives=alternatives)
    )


def _membership_path(schema: MinedSchema) -> str | PropertyPath:
    """Return the path of class membership: rdf:type, or the membership properties."""
    membership = schema.about.membership_property
    if not membership:
        return RDF_TYPE
    if isinstance(membership, str):
        return membership
    if len(membership) == 1:
        return membership[0]
    return PropertyPath(
        operator="alternative",
        items=[PropertyPath(operator="predicate", iri=p) for p in membership],
    )


def _untyped_shapes(
    schema: MinedSchema,
    untyped: dict[str, list[SchemaPattern]],
    base_uri: str,
    activate_observed: bool,
) -> list[ShaclNodeShape]:
    """Return a node shape for the untyped subjects of each property.

    SHACL has no target for "the nodes without a type": the shape targets the subjects of the
    property (sh:targetSubjectsOf) and holds when the node has a type (its class shapes
    describe it) or when its values have the observed kinds: sh:or ([sh:path rdf:type;
    sh:minCount 1] [sh:path p; values]). A typed subject is never held to the untyped values.
    """
    from hashlib import md5

    shapes = []
    typed = ShaclPropertyShape(path=_membership_path(schema), min_count=1)
    for prop, patterns in sorted(untyped.items()):
        short = md5(prop.encode(), usedforsecurity=False).hexdigest()[:8]
        values = _value_shape(patterns, [])
        values.path = prop
        values.uri = f"{base_uri}ps-untyped-{short}"
        values.name = patterns[0].property_label
        values.description = schema.enrichment.description(prop)
        label = patterns[0].property_label or prop
        shapes.append(
            ShaclNodeShape(
                uri=f"{base_uri}ns-untyped-{short}",
                target_subjects_of=[prop],
                alternatives=[typed.model_copy(deep=True), values],
                deactivated=not activate_observed,
                name=f"Subjects of {label} without a type",
                description=(
                    "Observed values of the IRI subjects of this property that have no type "
                    "(rdfsolve patterns with subject_binding 'untyped'). A subject with a type "
                    "conforms through the first alternative; its class shapes describe it. "
                    "Observed value-type template, not a source constraint. No required fields "
                    "or per-entity cardinalities were inferred."
                ),
            )
        )
    return shapes


LIST_MEMBERS = "rdf:rest*/rdf:first"


def _list_option(profile: CollectionProfile) -> ShaclPropertyShape:
    """Return the SHACL option for RDF list values: a list node whose members have the types.

    The members are reached with the path ([sh:zeroOrMorePath rdf:rest] rdf:first).
    """
    from rdflib import RDF

    from rdfsolve.schema_models.paths import PropertyPath

    members: list[ShaclPropertyShape] = [
        ShaclPropertyShape(path="", class_constraint=member) for member in profile.member_types
    ]
    members += [
        ShaclPropertyShape(path="", node_kind="Literal", datatype=datatype)
        for datatype in profile.member_datatypes
    ]
    if "Literal" in profile.member_kinds and not profile.member_datatypes:
        members.append(ShaclPropertyShape(path="", node_kind="Literal"))
    path = PropertyPath.from_sparql(LIST_MEMBERS, {"rdf": str(RDF)})
    member = members[0] if len(members) == 1 else ShaclPropertyShape(path="", alternatives=members)
    member.path = path
    return ShaclPropertyShape(
        path="", node_kind="BlankNode", properties=[member] if members else []
    )


def _complete_shapes(
    schema: MinedSchema, shapes: ShaclShapesGraph, base_uri: str
) -> ShaclShapesGraph:
    """Add namespace declarations and candidate navigation paths."""
    import logging
    from collections import defaultdict
    from hashlib import sha256

    from rdfsolve.schema_models.navigation import NavigationPath

    inactive = sum(shape.deactivated for shape in shapes.node_shapes)
    if inactive:
        logging.getLogger(__name__).warning(
            "SHACL export contains %d deactivated node shapes. These shapes do not "
            "validate data. Use activate_observed=True to enforce generated one-hop templates; "
            "retained source constraints keep their activation state.",
            inactive,
        )
    shapes.declare_prefixes(schema.get_prefixes(), resource=base_uri)
    if schema.navigation is None or not schema.navigation.paths:
        return shapes
    tested = schema.navigation.strategy == "tested"
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
            text = "; ".join(steps)
            if tested:
                text += f" ({route.matched_sources} of {route.source_count} start instances)"
            descriptions.append(text)
        identifier = sha256(repr((start, predicates)).encode()).hexdigest()[:20]
        by_class[start].append(
            ShaclPropertyShape(
                uri=f"{base_uri}route-{identifier}",
                path=routes[0].property_path(),
                name="; ".join(sorted({route.label() for route in routes})),
                description=(
                    "Class-qualified paths followed by instances of the data: "
                    if tested
                    else "Candidate class-qualified routes: "
                )
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
                    "Paths tested on the data; each was followed by at least one instance. "
                    "Edge triple counts are not joined counts or per-entity cardinalities. "
                    "The path omits intermediate class filters listed in each description."
                    if tested
                    else "Schema-composed paths. Instance support and coverage are unknown. "
                    "Edge triple counts are not joined counts or per-entity cardinalities. "
                    "The path omits intermediate class filters listed in each description. "
                    "Value types are candidate endpoints, not enforced constraints. " + omitted
                ),
                property_shapes=properties,
            )
        )
    for route in schema.navigation.paths:
        # A tested path is given above with its counts; a nested profile for each of thousands
        # of paths made the file too large (AOP-Wiki: 53 MB).
        if tested or route.instance_support != "matched":
            continue
        child = None
        for step in reversed(route.steps):
            constraint = ShaclPropertyShape(path="")
            if step.object_class == "Literal":
                constraint.node_kind, constraint.datatype = "Literal", step.datatype
            elif step.object_class in {"Resource", "BlankNode"}:
                constraint.node_kind = "IRI" if step.object_class == "Resource" else "BlankNode"
            else:
                constraint.class_constraint = step.object_class
            if child is not None:
                constraint.properties = [child]
            child = ShaclPropertyShape(
                path=step.property_uri,
                qualified_shape=constraint,
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
                description=(
                    f"Tested path; {route.matched_sources} of {route.source_count} start "
                    f"instances follow it; {route.observed_at}. "
                    if tested
                    else f"Candidate query profile; {route.matched_sources}/{route.source_count} focus entries matched. "
                    f"Observed endpoint degree {route.min_count}..{route.max_count}; {route.observed_at}. "
                )
                + "Qualified existence describes this route. It is not a dataset-wide requirement.",
            )
        )
    logging.getLogger(__name__).warning(
        "SHACL exports candidate paths and observed nested profiles. "
        "Keep canonical JSON for machine-readable filters, statistics, and route provenance."
    )
    return shapes
