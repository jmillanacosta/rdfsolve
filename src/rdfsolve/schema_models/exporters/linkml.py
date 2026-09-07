"""Build LinkML classes and slots directly from observed schema patterns."""

from __future__ import annotations

import re
from hashlib import sha256
from typing import TYPE_CHECKING, Any, cast

from bioregistry import curie_from_iri
from linkml.generators.yamlgen import YAMLGenerator
from linkml_runtime.linkml_model import (
    ClassDefinition,
    SchemaDefinition,
    SlotDefinition,
    TypeDefinition,
)
from linkml_runtime.linkml_model.meta import AnonymousSlotExpression, Example

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern


def _clean_local_part(local: str) -> str:
    """Clean the local part of a name while preserving structure.

    Examples::

        "KeyEvent"    -> "KeyEvent"
        "data1025"    -> "data_1025"
        "edam.data1025" -> "edam_data_1025"
        "C123456"     -> "C_123456"
    """
    local = local.replace(".", "_")
    local = re.sub(r"([a-zA-Z])(\d)", r"\1_\2", local)
    local = re.sub(r"([a-z])([A-Z])", r"\1_\2", local)
    local = re.sub(r"[^a-zA-Z0-9_]", "_", local)
    return local


def _finalize_linkml_name(name: str) -> str:
    """Apply final cleanup rules to ensure valid LinkML identifier."""
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    if name and name[0].isdigit():
        name = f"item_{name}"
    elif not name or not name[0].isalpha():
        name = f"item_{name}" if name else "unknown_item"
    if not name:
        name = "unknown_item"
    return name


def make_valid_linkml_name(uri_or_curie: str) -> str:
    """Convert a URI or CURIE to a valid LinkML identifier.

    LinkML identifiers must start with a letter and contain only
    letters, digits, and underscores.

    Examples::

        "aopo:KeyEvent"           -> "aopo_KeyEvent"
        "edam.data1025"           -> "edam_data_1025"
        "http://example.org/Cls"  -> prefix_Cls  (via bioregistry)
        "40:Something"            -> "item_40_Something"
    """
    if uri_or_curie.startswith(("http://", "https://")):
        curie = curie_from_iri(uri_or_curie)
        if curie:
            uri_or_curie = curie

    if ":" in uri_or_curie:
        prefix, local = uri_or_curie.split(":", 1)
        # Clean prefix and ensure it starts with letter/underscore
        prefix = re.sub(r"[^a-zA-Z0-9_]", "_", prefix)
        prefix = _finalize_linkml_name(prefix)  # Fix invalid prefix names
        local = _clean_local_part(local)
        local = _finalize_linkml_name(local)  # Fix invalid local parts
        name = f"{prefix}_{local}"
    else:
        name = _clean_local_part(uri_or_curie)

    return _finalize_linkml_name(name)


# Core conversion


def _names(iris: list[str], labels: dict[str, str]) -> dict[str, str]:
    result = {}
    used: set[str] = set()
    for iri in sorted(iris):
        name = make_valid_linkml_name(labels.get(iri) or iri) or "term"
        if name in used:
            name += "_" + sha256(iri.encode()).hexdigest()[:8]
        used.add(name)
        result[iri] = name
    return result


def to_linkml(
    schema: MinedSchema,
    *,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> SchemaDefinition:
    """Export observed ranges. Do not infer required fields or cardinalities."""
    name = make_valid_linkml_name(schema_name or schema.about.dataset_name or "rdf_schema")
    base = schema_base_uri or schema.about.schema_uri or f"https://w3id.org/{name}/"
    labels: dict[str, str] = {}
    for pattern in schema.patterns:
        for iri, label in (
            (pattern.subject_class, pattern.subject_label),
            (pattern.property_uri, pattern.property_label),
            (pattern.object_class, pattern.object_label),
        ):
            if label:
                labels.setdefault(iri, label)
    for annotation in schema.enrichment.labels:
        labels.setdefault(annotation.term_iri, annotation.text.value)
    class_names = _names(schema.get_classes(), labels)
    slot_names = _names(schema.get_properties(), labels)
    types: dict[str, TypeDefinition] = {}
    classes = {
        iri: ClassDefinition(
            name=class_name,
            class_uri=iri,
            slots=[],
            description=schema.enrichment.description(iri),
            examples=[
                Example(value=t.value) for t in schema.enrichment.class_examples.get(iri, [])
            ],
        )
        for iri, class_name in class_names.items()
    }
    ranges: dict[str, set[str]] = {}
    class_ranges: dict[tuple[str, str], set[str]] = {}

    def value_range(pattern: SchemaPattern) -> str:
        if pattern.object_class in class_names:
            return class_names[pattern.object_class]
        if pattern.object_class == "Resource":
            return "uriorcurie"
        if pattern.object_class == "BlankNode" or not pattern.datatype:
            return "string"
        datatype = pattern.datatype
        type_name = "datatype_" + sha256(datatype.encode()).hexdigest()[:12]
        types[type_name] = TypeDefinition(name=type_name, uri=datatype, typeof="string")
        return type_name

    for pattern in schema.patterns:
        value = value_range(pattern)
        ranges.setdefault(pattern.property_uri, set()).add(value)
        class_ranges.setdefault((pattern.subject_class, pattern.property_uri), set()).add(value)

    def constraints(values: set[str]) -> dict[str, Any]:
        ordered = sorted(values)
        return (
            {"range": ordered[0]}
            if len(ordered) == 1
            else {"any_of": [AnonymousSlotExpression(range=value) for value in ordered]}
        )

    slots = {
        slot_names[iri]: SlotDefinition(
            name=slot_names[iri],
            slot_uri=iri,
            multivalued=True,
            required=False,
            **constraints(values),
            description=schema.enrichment.description(iri),
            examples=[
                Example(value=e.value.value)
                for e in schema.enrichment.examples
                if e.property_uri == iri
            ],
        )
        for iri, values in ranges.items()
    }
    for (class_iri, property_iri), values in class_ranges.items():
        slot_name = slot_names[property_iri]
        classes[class_iri].slots.append(slot_name)
        classes[class_iri].slot_usage[slot_name] = SlotDefinition(
            name=slot_name, **constraints(values)
        )

    about = schema.about
    return SchemaDefinition(
        id=base,
        name=name,
        title=about.title or about.dataset_name,
        description=schema_description or about.description,
        version=about.schema_version or None,
        source=about.endpoint,
        license=about.source_license,
        created_on=about.generated_at or None,
        default_prefix=name,
        default_range="string",
        imports=["linkml:types"],
        prefixes={name: base, "linkml": "https://w3id.org/linkml/"},
        annotations={"source_version_iri": about.source_version_iri}
        if about.source_version_iri
        else {},
        types=types,
        classes={item.name: item for item in classes.values()},
        slots=slots,
    )


def to_linkml_yaml(
    schema: MinedSchema,
    *,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Serialize a LinkML model as YAML."""
    result = to_linkml(
        schema,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )
    return cast(str, YAMLGenerator(result).serialize())
