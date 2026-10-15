"""Export observed patterns or a retained source SHACL profile."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from rdflib import Graph, URIRef

from rdfsolve.schema_models.collections import CollectionProfile
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclNodeShape, ShaclPropertyShape, ShaclShapesGraph

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

ANY_LITERAL = "http://www.w3.org/2000/01/rdf-schema#Literal"
# A SPARQL prefix name (PN_PREFIX, ASCII): a letter first, no dot last.
_PREFIX_NAME = re.compile(r"^[A-Za-z]([A-Za-z0-9_.-]*[A-Za-z0-9_-])?$")


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


def untyped_shape_iri(dataset: str, prop: str) -> str:
    """Return the IRI of the node shape of the subjects of *prop* without a type in *dataset*
    (with the default base IRI of minedschema_to_shacl).
    """
    from hashlib import md5

    from rdfsolve.config import mint

    short = md5(prop.encode(), usedforsecurity=False).hexdigest()[:8]
    return mint("dataset", dataset or "unnamed") + f"/shapes/ns-untyped-{short}"


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
            _mark_sampled(shape, patterns)
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


def _mark_sampled(shape: ShaclPropertyShape, patterns: list[SchemaPattern]) -> bool:
    """Deactivate a value shape whose kinds come in part from a sample of a refused query.

    The sample shows kinds that occur, not every kind: an active shape would flag values of a
    kind the sample missed. Counts of samples are never sh:minCount or sh:maxCount (no observed
    shape states them). Return whether the shape was deactivated.
    """
    samples = [p.sampled for p in patterns if p.sampled and "patterns" in p.sampled.covers]
    if not samples:
        return False
    first = samples[0]
    shape.deactivated = True
    note = (
        f"Value kinds observed in a sample of {first.size} {first.unit} of a query that the "
        "endpoint refused; other kinds may occur, so the shape is deactivated."
    )
    shape.description = f"{shape.description} {note}" if shape.description else note
    return True


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
        incomplete = _mark_sampled(values, patterns)
        shapes.append(
            ShaclNodeShape(
                uri=f"{base_uri}ns-untyped-{short}",
                target_subjects_of=[prop],
                alternatives=[typed.model_copy(deep=True), values],
                deactivated=not activate_observed or incomplete,
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


def _add_iri_patterns(schema: MinedSchema, shapes: ShaclShapesGraph, base_uri: str) -> None:
    """Give each node shape the ways its members are identified: the parts of its class counted
    in the data (schema.class_parts) as named shapes combined with sh:or, else the namespaces of
    its class's example IRIs as sh:pattern.

    The namespaces of registered identifiers are also declared with their prefixes (sh:declare)
    on a declarations resource of that class, so that a reader can tell which registered identifiers
    the class's IRIs are, also where a prefix names another namespace elsewhere in the graph.
    """
    from hashlib import sha256

    from rdfsolve._uri import uri_to_curie
    from rdfsolve.identifiers import parse

    examples: dict[str, set[str]] = defaultdict(set)
    for class_iri, terms in schema.enrichment.class_examples.items():
        examples[class_iri].update(t.value for t in terms if t.kind == "uri")
    for example in schema.enrichment.examples:
        if not example.untyped_subject and example.subject.kind == "uri":
            examples[example.subject_class].add(example.subject.value)
    namespaces: dict[str, dict[str, str]] = {}  # class -> namespace -> prefix
    for class_iri, iris in examples.items():
        for iri in iris:
            found = parse(iri)  # a registered identifier: its namespace as written in the IRI
            if found is not None and iri.endswith(found.local):
                prefix, namespace = found.prefix, iri[: len(iri) - len(found.local)]
            else:  # not a registered identifier: the namespace alone, with no prefix to declare
                prefix, namespace = "", uri_to_curie(iri)[2]
            if namespace and namespace != iri:
                namespaces.setdefault(class_iri, {})[namespace] = prefix
    special = set("\\.?*+(){}|[]^$")
    for class_iri, found_namespaces in sorted(namespaces.items()):
        by_prefix: dict[str, str] = {}
        for namespace, prefix in sorted(found_namespaces.items()):
            # A prefix that is no SPARQL prefix name cannot be declared; its namespace stays in
            # the class's sh:pattern.
            if prefix and _PREFIX_NAME.match(prefix):
                by_prefix.setdefault(prefix, namespace)
        if by_prefix:
            identity = sha256(class_iri.encode()).hexdigest()[:16]
            shapes.declare_prefixes(by_prefix, resource=f"{base_uri}identifiers-{identity}")
    counted = schema.class_parts.by_class if schema.class_parts is not None else {}
    for shape in list(shapes.node_shapes):
        parts = counted.get(shape.target_class or "")
        if parts and shape.pattern is None:
            # The parts counted over every member: named shapes, one of which each member meets.
            from rdfsolve.schema_models.class_parts import part_shapes

            label = shape.name or (shape.target_class or "").rsplit("/", 1)[-1].rsplit("#", 1)[-1]
            named = part_shapes(shape.uri, label, parts)
            shape.parts = [part.uri for part in named]
            shapes.node_shapes += named
            continue
        if shape.pattern is None and shape.target_class in namespaces:
            escaped = (
                "".join("\\" + c if c in special else c for c in namespace)
                for namespace in sorted(namespaces[shape.target_class])
            )
            shape.pattern = "^(" + "|".join(escaped) + ")"


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
    _add_iri_patterns(schema, shapes, base_uri)
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
        # of paths makes the file too large.
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


def merge_profile(graph: Graph, profile: str) -> None:
    """Merge the shapes of a SHACL application profile (Turtle) into generated shapes.

    A profile node shape with a sh:targetClass that a generated node shape has is merged into
    it; a profile property shape whose path is an IRI that a generated property shape of that
    class has is merged into it: each predicate the profile shape states replaces the generated
    values of that predicate (constraints, order, group, messages, DASH annotations), the other
    generated values stay. A profile property shape without a generated one is added to the
    class's shape; a node shape of a class with no generated shape, and every other statement
    of the profile (property groups, declarations), are added as they are.
    """
    from rdflib import RDF, Namespace

    sh = Namespace("http://www.w3.org/ns/shacl#")
    given = Graph().parse(data=profile, format="turtle")
    generated: dict[str, Any] = {}
    for node, target_class in graph.subject_objects(sh.targetClass):
        generated.setdefault(str(target_class), node)
    by_path: dict[tuple[str, str], Any] = {}
    for class_iri, node in generated.items():
        for shape in graph.objects(node, sh.property):
            path = graph.value(shape, sh.path)
            if isinstance(path, URIRef):
                by_path.setdefault((class_iri, str(path)), shape)

    def merge_into(target: Any, source: Any, keep: set[Any]) -> None:
        """Replace the values of *target* for each predicate *source* states, but *keep*."""
        for predicate in set(given.predicates(source)) - keep:
            graph.remove((target, predicate, None))
        for _, predicate, value in given.triples((source, None, None)):
            if predicate not in keep:
                graph.add((target, predicate, value))

    merged: set[Any] = set()
    for node, cls in given.subject_objects(sh.targetClass):
        target = generated.get(str(cls))
        if target is None:
            continue
        merged.add(node)
        merge_into(target, node, {RDF.type, sh.targetClass, sh.property})
        for shape in given.objects(node, sh.property):
            path = given.value(shape, sh.path)
            existing = by_path.get((str(cls), str(path))) if isinstance(path, URIRef) else None
            if existing is None:
                graph.add((target, sh.property, shape))
                continue
            merged.add(shape)
            merge_into(existing, shape, {RDF.type, sh.path})
    for triple in given:
        if triple[0] not in merged:
            graph.add(triple)
    for prefix, namespace in given.namespaces():
        graph.bind(prefix, namespace, override=False)
