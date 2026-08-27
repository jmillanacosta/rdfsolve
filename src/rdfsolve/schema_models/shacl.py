"""SHACL shape generation from mined patterns.

Direct SHACL generation using rdflib, with support for:
- Cardinality constraints (sh:minCount, sh:maxCount)
- Datatype constraints (sh:datatype)
- Node kind constraints (sh:nodeKind)
- Class constraints (sh:class)
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, cast

from linkml.generators.shaclgen import ShaclGenerator
from linkml.generators.yamlgen import YAMLGenerator
from rdflib import XSD, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, SH

from rdfsolve.schema_models.linkml import mined_schema_to_linkml, to_linkml

__all__ = ["mined_schema_to_shacl", "to_shacl", "mined_schema_to_shacl_direct"]


def to_shacl(
    jsonld: dict[str, Any],
    *,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
    closed: bool = True,
    suffix: str | None = None,
    include_annotations: bool = False,
) -> str:
    """Generate SHACL shapes (Turtle) from a JSON-LD schema dict.

    Parameters
    ----------
    jsonld:
        JSON-LD document (``@context``, ``@graph``, …).
    schema_name:
        Name for the underlying LinkML schema.
    schema_description:
        Human-readable description.
    schema_base_uri:
        Base URI for the schema.
    closed:
        If *True*, produce closed SHACL shapes (``sh:closed true``).
    suffix:
        Suffix appended to every shape name
        (e.g. ``"Shape"`` -> ``PersonShape``).
    include_annotations:
        If *True*, carry annotations through to shapes.

    Returns
    -------
    str
        SHACL shapes serialised as Turtle.
    """
    linkml_schema = to_linkml(
        jsonld,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )
    linkml_yaml = YAMLGenerator(linkml_schema).serialize()
    shacl_gen = ShaclGenerator(
        schema=linkml_yaml,
        closed=closed,
        suffix=suffix,
        include_annotations=include_annotations,
    )
    return cast(str, shacl_gen.serialize())


def mined_schema_to_shacl(
    mined_schema: Any,  # MinedSchema, but avoiding circular import
    schema_name: str | None = None,
    schema_description: str | None = None,
    closed: bool = True,
    suffix: str | None = None,
    include_annotations: bool = False,
    use_direct: bool = True,
) -> str:
    """Generate SHACL shapes from MinedSchema patterns.

    By default uses direct rdflib generation with full constraint support.
    Can optionally use LinkML generation for compatibility.

    Shape URIs are generated in a configurable shapes namespace (defaults to
    http://example.com/shapes/{dataset}/) while sh:targetClass uses the
    original class URIs.

    Parameters
    ----------
    mined_schema:
        MinedSchema object with patterns and metadata
    schema_name:
        Name for the underlying schema
    schema_description:
        Human-readable description
    closed:
        If *True*, produce closed SHACL shapes (``sh:closed true``)
    suffix:
        Suffix appended to every shape name (LinkML only)
    include_annotations:
        If *True*, carry annotations through to shapes (LinkML only)
    use_direct:
        If *True*, use direct rdflib generation with full constraints

    Returns
    -------
    str
        SHACL shapes serialised as Turtle
    """
    if use_direct:
        # Use new direct generator with full constraint support
        return mined_schema_to_shacl_direct(
            mined_schema,
            schema_name=schema_name,
            closed=closed,
        )

    # Legacy LinkML-based generation
    from rdfsolve.schema_models.linkml import make_valid_linkml_name

    linkml_schema = mined_schema_to_linkml(
        mined_schema,
        schema_name=schema_name,
        schema_description=schema_description,
    )
    linkml_yaml = YAMLGenerator(linkml_schema).serialize()
    shacl_gen = ShaclGenerator(
        schema=linkml_yaml,
        closed=closed,
        suffix=suffix,
        include_annotations=include_annotations,
    )
    shacl_turtle = cast(str, shacl_gen.serialize())

    # Post-process: Replace shape URIs with configurable namespace URIs
    # LinkML uses class_uri as the shape URI, but we want shapes in a separate namespace
    # Build a mapping from original class URIs to shape URIs
    from rdfsolve.config import get_base_uri

    shape_uri_map = {}
    base_uri = get_base_uri()
    shapes_base = f"{base_uri}/shapes/{linkml_schema.name}/"

    for pattern in mined_schema.patterns:
        # Map subject class
        class_uri = pattern.subject_class
        class_name = make_valid_linkml_name(class_uri)
        if suffix:
            shape_uri = f"{shapes_base}{class_name}{suffix}"
        else:
            shape_uri = f"{shapes_base}{class_name}"
        shape_uri_map[class_uri] = shape_uri

        # Map object class if it's not a sentinel
        if pattern.object_class not in ("Literal", "Resource", "BlankNode"):
            class_uri = pattern.object_class
            class_name = make_valid_linkml_name(class_uri)
            if suffix:
                shape_uri = f"{shapes_base}{class_name}{suffix}"
            else:
                shape_uri = f"{shapes_base}{class_name}"
            shape_uri_map[class_uri] = shape_uri

    # Replace shape URIs in the SHACL Turtle
    # We need to replace shape declarations but keep sh:targetClass unchanged
    from rdflib import Graph, Namespace
    from rdflib.namespace import RDF, SH

    # Parse the SHACL graph
    g = Graph()
    g.parse(data=shacl_turtle, format="turtle")

    # Build new graph with replaced shape URIs
    new_g = Graph()
    for prefix, ns in g.namespaces():
        new_g.bind(prefix, ns)

    # Add shapes namespace binding
    new_g.bind("shapes", Namespace(shapes_base))

    # Copy triples, replacing shape subjects with shape namespace URIs
    for s, p, o in g:
        # Check if subject is a shape (has rdf:type sh:NodeShape)
        is_shape = (s, RDF.type, SH.NodeShape) in g

        if is_shape:
            # Replace shape URI with shape namespace URI
            original_uri = str(s)
            if original_uri in shape_uri_map:
                from rdflib import URIRef

                new_s = URIRef(shape_uri_map[original_uri])
                new_g.add((new_s, p, o))
            else:
                new_g.add((s, p, o))
        else:
            new_g.add((s, p, o))

    # Serialize back to Turtle
    return cast(str, new_g.serialize(format="turtle"))


def mined_schema_to_shacl_direct(
    mined_schema: Any,
    schema_name: str | None = None,
    closed: bool = True,
) -> str:
    """Generate SHACL shapes directly using rdflib from mined patterns.

    Creates SHACL shapes with full constraint support including:
    - sh:minCount, sh:maxCount from pattern cardinality
    - sh:datatype for literal properties
    - sh:nodeKind for distinguishing IRIs from literals
    - sh:class for object property ranges
    - sh:description from pattern descriptions

    Parameters
    ----------
    mined_schema:
        MinedSchema object with patterns and metadata
    schema_name:
        Name for the schema (uses dataset_name if not provided)
    closed:
        If True, produce closed SHACL shapes

    Returns
    -------
    str
        SHACL shapes serialized as Turtle
    """
    from rdfsolve.config import get_base_uri

    g = Graph()
    g.bind("sh", SH)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

    # Group patterns by subject class
    class_patterns: dict[str, list] = defaultdict(list)
    for pat in mined_schema.patterns:
        class_patterns[pat.subject_class].append(pat)

    # Create shape namespace
    base_uri = get_base_uri()
    name = schema_name or mined_schema.about.dataset_name
    shapes_base = f"{base_uri}/shapes/{name}/"

    for class_uri, patterns in class_patterns.items():
        # Create NodeShape
        class_local = _get_local_name(class_uri)
        shape_uri = URIRef(f"{shapes_base}{class_local}Shape")

        g.add((shape_uri, RDF.type, SH.NodeShape))
        g.add((shape_uri, SH.targetClass, URIRef(class_uri)))

        # Add class label and description if available
        for pat in patterns:
            if pat.subject_label:
                g.add((shape_uri, RDFS.label, Literal(f"{pat.subject_label} Shape")))
                break

        for pat in patterns:
            if pat.subject_description:
                g.add((shape_uri, SH.description, Literal(pat.subject_description)))
                break

        # Add closed constraint if requested
        if closed:
            g.add((shape_uri, SH.closed, Literal(True)))
            g.add((shape_uri, SH.ignoredProperties, RDF.type))

        # Create property shapes
        for pat in patterns:
            prop_uri = URIRef(pat.property_uri)
            prop_shape_uri = URIRef(f"{shape_uri}/{_get_local_name(pat.property_uri)}")

            g.add((shape_uri, SH.property, prop_shape_uri))
            g.add((prop_shape_uri, SH.path, prop_uri))

            # Add property label
            if pat.property_label:
                g.add((prop_shape_uri, RDFS.label, Literal(pat.property_label)))

            # Add property description
            if pat.property_description:
                g.add((prop_shape_uri, SH.description, Literal(pat.property_description)))

            # Add cardinality constraints
            if pat.min_count is not None:
                g.add((prop_shape_uri, SH.minCount, Literal(pat.min_count)))

            if pat.max_count is not None:
                g.add((prop_shape_uri, SH.maxCount, Literal(pat.max_count)))

            # Add type constraints based on object class
            if pat.object_class == "Literal":
                # Literal value
                g.add((prop_shape_uri, SH.nodeKind, SH.Literal))

                # Add datatype if available
                if pat.datatype:
                    g.add((prop_shape_uri, SH.datatype, URIRef(pat.datatype)))

            elif pat.object_class in ("Resource", "BlankNode"):
                # URI or blank node
                if pat.object_class == "Resource":
                    g.add((prop_shape_uri, SH.nodeKind, SH.IRI))
                else:
                    g.add((prop_shape_uri, SH.nodeKind, SH.BlankNode))

            else:
                # Object property - reference to a class
                g.add((prop_shape_uri, SH.nodeKind, SH.IRI))
                g.add((prop_shape_uri, SH["class"], URIRef(pat.object_class)))

    return cast(str, g.serialize(format="turtle"))


def _get_local_name(uri: str) -> str:
    """Extract local name from URI."""
    if "#" in uri:
        return uri.split("#")[-1]
    elif "/" in uri:
        return uri.split("/")[-1]
    return uri
