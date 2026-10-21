"""Read a JSON Schema as a SHACL node shape, with the property addresses of a JSON-LD context.

Only a subset of JSON Schema is read: type, format, pattern, properties, required, items, anyOf,
oneOf, $ref and definitions. Every other keyword of JSON Schema is refused, so that no constraint
is lost unseen. Keys that are not keywords of JSON Schema are read as annotations and left out, and
so are properties named by a JSON-LD keyword.
"""

from __future__ import annotations

import json
from typing import Any

from rdflib import RDF, RDFS, Graph
from rdflib.namespace import XSD

from rdfsolve.schema_models.shacl_model import Severity, ShaclNodeShape, ShaclPropertyShape

REFUSED = frozenset(
    {
        "allOf", "not", "if", "then", "else", "enum", "const", "patternProperties",
        "additionalProperties", "dependentRequired", "dependentSchemas", "dependencies",
        "unevaluatedProperties", "unevaluatedItems", "prefixItems", "contains", "minContains",
        "maxContains", "propertyNames", "minProperties", "maxProperties", "minItems", "maxItems",
        "uniqueItems", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
        "minLength", "maxLength", "$dynamicRef", "$recursiveRef",
    }
)  # fmt: skip
ADDRESSES = frozenset({"uri", "iri", "uri-reference", "iri-reference"})
DATATYPES = {"date": XSD.date, "date-time": XSD.dateTime, "time": XSD.time}
NUMBERS = {
    "integer": [XSD.integer],
    "number": [XSD.integer, XSD.decimal, XSD.double],
    "boolean": [XSD.boolean],
}
# The depth of the nested shapes made from definitions; deeper definitions are checked by class only.
DEPTH = 2


def vocabulary_context(vocabularies: list[Graph], namespaces: list[str]) -> dict[str, str]:
    """Return the classes and properties of the namespaces by local name, the first namespace first."""
    context: dict[str, str] = {}
    for namespace in namespaces:
        for vocabulary in vocabularies:
            for kind in (RDF.Property, RDFS.Class):
                for term in sorted(str(subject) for subject in vocabulary.subjects(RDF.type, kind)):
                    name = term.removeprefix(namespace)
                    if name != term and name and "/" not in name and "#" not in name:
                        context.setdefault(name, term)
    return context


def address(name: str, context: dict[str, str]) -> str:
    """Return the IRI of a JSON key or of a compact class name in a JSON-LD context."""
    term = name.rpartition(":")[2]
    if term in context:
        return context[term]
    if "@vocab" in context:
        return context["@vocab"] + term
    raise KeyError(f"No address for {name} in the context")


def _refuse(schema: dict[str, Any]) -> None:
    """Raise if a schema has a keyword of JSON Schema that is not read."""
    found = REFUSED & set(schema)
    if found:
        raise NotImplementedError(f"JSON Schema keywords not read: {', '.join(sorted(found))}")


def _leaves(
    schema: dict[str, Any], definitions: dict[str, Any]
) -> tuple[list[dict[str, Any]], bool]:
    """Return the value schemas of a schema without alternatives and arrays, and if several values fit."""
    _refuse(schema)
    if "anyOf" in schema or "oneOf" in schema:
        found: list[dict[str, Any]] = []
        several = False
        for alternative in [*schema.get("anyOf", []), *schema.get("oneOf", [])]:
            given, many = _leaves(alternative, definitions)
            found += given
            several = several or many
        return found, several
    if "$ref" in schema:
        return _leaves(definitions[schema["$ref"].rsplit("/", 1)[-1]], definitions)
    kinds = schema.get("type")
    if not kinds:
        return [], True
    kinds = kinds if isinstance(kinds, list) else [kinds]
    found = [{**schema, "type": kind} for kind in kinds if kind != "array"]
    if "array" not in kinds:
        return found, False
    items = schema.get("items", {})
    # Positions of tuple items have no counterpart in RDF: each item schema is one more alternative.
    for item in items if isinstance(items, list) else [items]:
        found += _leaves(item, definitions)[0]
    return found, True


def _value(
    leaf: dict[str, Any],
    definitions: dict[str, Any],
    context: dict[str, str],
    values: Severity | None,
    prefix: str,
    depth: int,
) -> ShaclPropertyShape:
    """Return the constraint of the values of one value schema, with prefix for nested shape IRIs."""
    kind = leaf["type"]
    if kind == "object":
        shape = ShaclPropertyShape(path="", node_kind="BlankNodeOrIRI")
        if "@type" in leaf:
            shape.class_constraint = address(str(leaf["@type"]), context)
        if depth < DEPTH:
            shape.properties = _fields(
                leaf, definitions, context, {"required": values}, values, prefix, depth + 1
            )
        return shape
    if kind == "string" and leaf.get("format") in ADDRESSES:
        return ShaclPropertyShape(path="", node_kind="IRI")
    if kind == "string" and leaf.get("format") in DATATYPES:
        return ShaclPropertyShape(path="", datatype=str(DATATYPES[leaf["format"]]))
    if kind == "string":
        return ShaclPropertyShape(path="", node_kind="Literal", pattern=leaf.get("pattern"))
    if kind in NUMBERS:
        types = [ShaclPropertyShape(path="", datatype=str(given)) for given in NUMBERS[kind]]
        return types[0] if len(types) == 1 else ShaclPropertyShape(path="", alternatives=types)
    raise NotImplementedError(f"JSON Schema type not read: {kind}")


def _fields(
    schema: dict[str, Any],
    definitions: dict[str, Any],
    context: dict[str, str],
    levels: dict[str, Severity | None],
    values: Severity | None,
    prefix: str,
    depth: int = 0,
) -> list[ShaclPropertyShape]:
    """Return a shape for each listed property of an object schema, and one for the values of each property.

    Each shape has an IRI made of prefix and the property name, so that the shapes are written in
    the same order on each run.
    """
    _refuse(schema)
    found = []
    for level, severity in levels.items():
        for name in schema.get(level, []):
            found.append(
                ShaclPropertyShape(
                    uri=f"{prefix}{name}-{level}",
                    path=address(name, context),
                    name=name,
                    min_count=1,
                    severity=severity,
                )
            )
    for name, given in schema.get("properties", {}).items():
        # A JSON-LD keyword, such as @id, is the node itself, not a property.
        if name.startswith("@"):
            continue
        given_leaves, several = _leaves(given, definitions)
        # The same value schema given as one value and as an array item is one alternative.
        unique = {json.dumps(leaf, sort_keys=True): leaf for leaf in given_leaves}
        alternatives = [
            _value(leaf, definitions, context, values, f"{prefix}{name}-{number}/", depth)
            for number, leaf in enumerate(unique.values(), start=1)
        ]
        description = " ".join(str(given.get("description", "")).split()) or None
        found.append(
            ShaclPropertyShape(
                uri=f"{prefix}{name}",
                path=address(name, context),
                name=name,
                description=description,
                max_count=None if several else 1,
                severity=values,
                alternatives=alternatives,
            )
        )
    return found


def json_schema_shape(
    schema: dict[str, Any],
    *,
    shape: str,
    target_class: str,
    context: dict[str, str],
    levels: dict[str, Severity | None] | None = None,
    values: Severity | None = "Warning",
) -> ShaclNodeShape:
    """Return the node shape of the objects of a JSON Schema.

    Args:
        schema: a JSON Schema of an object, with its definitions
        shape: the IRI of the node shape
        target_class: the class of the nodes the shape is for
        context: a JSON-LD context, with the IRI of each JSON key and class name, and @vocab
        levels: the keys of lists of property names, with the severity of a missing property;
            by default the properties in required, as violations
        values: the severity of the values that do not fit, one more value than allowed included

    Returns:
        The node shape, with one property shape for each listed property and one for the values
        of each property in properties, at IRIs under the IRI of the node shape
    """
    definitions = {**schema.get("definitions", {}), **schema.get("$defs", {})}
    return ShaclNodeShape(
        uri=shape,
        target_class=target_class,
        property_shapes=_fields(
            schema, definitions, context, levels or {"required": "Violation"}, values, f"{shape}#"
        ),
    )
