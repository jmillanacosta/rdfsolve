"""SHACL shape generation from JSON-LD.

Converts a rdfsolve JSON-LD schema dict to SHACL Turtle via the
LinkML -> ShaclGenerator pipeline.
"""

from __future__ import annotations

from typing import Any, cast

from linkml.generators.shaclgen import ShaclGenerator
from linkml.generators.yamlgen import YAMLGenerator

from rdfsolve.schema_models.linkml import mined_schema_to_linkml, to_linkml

__all__ = ["mined_schema_to_shacl", "to_shacl"]


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
) -> str:
    """Generate SHACL shapes directly from MinedSchema patterns.

    This builds SHACL via LinkML without going through JSON-LD or VoID,
    ensuring the shapes represent the mined patterns directly.

    Shape URIs are generated in a configurable shapes namespace (defaults to
    http://example.com/shapes/{dataset}/) while sh:targetClass uses the
    original class URIs.

    Parameters
    ----------
    mined_schema:
        MinedSchema object with patterns and metadata
    schema_name:
        Name for the underlying LinkML schema
    schema_description:
        Human-readable description
    closed:
        If *True*, produce closed SHACL shapes (``sh:closed true``)
    suffix:
        Suffix appended to every shape name
    include_annotations:
        If *True*, carry annotations through to shapes

    Returns
    -------
    str
        SHACL shapes serialised as Turtle
    """
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
