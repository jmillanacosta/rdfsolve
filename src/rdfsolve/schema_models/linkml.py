"""LinkML schema generation from JSON-LD.

Converts a rdfsolve JSON-LD schema dict (``@context`` + ``@graph``)
into a LinkML ``SchemaDefinition`` or its YAML serialisation.
"""

from __future__ import annotations

import logging
import re
from typing import Any, cast

from bioregistry import curie_from_iri
from linkml.generators.yamlgen import YAMLGenerator
from linkml_runtime.linkml_model import (
    ClassDefinition,
    SchemaDefinition,
    SlotDefinition,
    TypeDefinition,
)

logger = logging.getLogger(__name__)

__all__ = [
    "make_valid_linkml_name",
    "to_linkml",
    "to_linkml_yaml",
]


# ── Name-cleaning helpers ────────────────────────────────────────


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
    """
    if uri_or_curie.startswith(("http://", "https://")):
        curie = curie_from_iri(uri_or_curie)
        if curie:
            uri_or_curie = curie

    if ":" in uri_or_curie:
        prefix, local = uri_or_curie.split(":", 1)
        prefix = re.sub(r"[^a-zA-Z0-9_]", "_", prefix)
        local = _clean_local_part(local)
        name = f"{prefix}_{local}"
    else:
        name = _clean_local_part(uri_or_curie)

    return _finalize_linkml_name(name)


# ── Core conversion ──────────────────────────────────────────────


def _derive_schema_meta(
    jsonld: dict[str, Any],
    schema_name: str | None,
    schema_description: str | None,
    schema_base_uri: str | None,
) -> tuple[str, str, str]:
    """Return ``(schema_name, schema_uri, description)``."""
    if not schema_name:
        about = jsonld.get("@about", {})
        schema_name = about.get("dataset_name", "rdf_schema")
        schema_name = re.sub(r"[^a-zA-Z0-9_]", "_", schema_name)

    schema_uri = (
        f"https://w3id.org/{schema_name}/"
        if not schema_base_uri
        else schema_base_uri.rstrip("/") + "/"
    )
    description = schema_description or f"LinkML schema generated from JSON-LD for {schema_name}"
    return schema_name, schema_uri, description


def _build_prefixes(
    schema_name: str,
    schema_uri: str,
    jsonld_context: dict[str, Any],
) -> dict[str, str]:
    """Merge base prefixes with JSON-LD ``@context`` entries."""
    base: dict[str, str] = {
        schema_name: schema_uri,
        "linkml": "https://w3id.org/linkml/",
        "schema": "http://schema.org/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
    }
    return {**base, **jsonld_context}


def _build_empty_schema(
    schema_name: str,
    schema_uri: str,
    description: str,
    prefixes: dict[str, str],
) -> SchemaDefinition:
    """Construct a :class:`SchemaDefinition` with types pre-populated."""
    return SchemaDefinition(
        id=schema_uri,
        name=schema_name,
        description=description,
        default_prefix=schema_name,
        prefixes=prefixes,
        types={
            "string": TypeDefinition(
                name="string",
                uri="xsd:string",
                base="str",
            ),
            "uriorcurie": TypeDefinition(
                name="uriorcurie",
                uri="xsd:anyURI",
                base="URIorCURIE",
            ),
        },
    )


def _collect_graph_items(
    jsonld: dict[str, Any],
) -> list[dict[str, Any]] | None:
    """Return non-dataset ``@graph`` items, or ``None`` if absent/empty."""
    if "@graph" not in jsonld:
        logger.warning(
            "No @graph found in JSON-LD, returning empty schema",
        )
        return None
    items = [item for item in jsonld["@graph"] if item.get("@type") != "void:Dataset"]
    if not items:
        logger.warning("No schema triples found in JSON-LD @graph")
        return None
    return items


def _scan_graph_items(
    items: list[dict[str, Any]],
    label_map: dict[str, str],
) -> tuple[
    set[str],
    set[str],
    dict[str, list[str]],
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
]:
    """Single pass over *items* collecting all schema metadata.

    Returns
    -------
    (all_class_names, all_slot_names, class_properties,
     property_ranges, property_descriptions,
     original_class_uris, original_slot_uris)
    """
    all_class_names: set[str] = set()
    all_slot_names: set[str] = set()
    class_properties: dict[str, list[str]] = {}
    property_ranges: dict[str, str] = {}
    property_descriptions: dict[str, str] = {}
    original_class_uris: dict[str, str] = {}
    original_slot_uris: dict[str, str] = {}

    for item in items:
        if "@id" not in item:
            continue
        subject = item["@id"]
        subject_clean = make_valid_linkml_name(subject)
        all_class_names.add(subject_clean)
        original_class_uris.setdefault(subject_clean, subject)
        class_properties.setdefault(subject_clean, [])

        for prop, value in item.items():
            if prop.startswith("@") or prop == "_counts":
                continue
            prop_clean = make_valid_linkml_name(prop)
            all_slot_names.add(prop_clean)
            original_slot_uris.setdefault(prop_clean, prop)

            if prop_clean not in class_properties[subject_clean]:
                class_properties[subject_clean].append(prop_clean)

            _update_property_range(
                prop_clean,
                value,
                all_class_names,
                original_class_uris,
                property_ranges,
            )

            if prop_clean not in property_descriptions:
                lbl = label_map.get(prop)
                property_descriptions[prop_clean] = lbl if lbl else f"Property {prop}"

    return (
        all_class_names,
        all_slot_names,
        class_properties,
        property_ranges,
        property_descriptions,
        original_class_uris,
        original_slot_uris,
    )


def _update_property_range(
    prop_clean: str,
    value: Any,
    all_class_names: set[str],
    original_class_uris: dict[str, str],
    property_ranges: dict[str, str],
) -> None:
    """Infer and record the range for *prop_clean* from *value*."""
    val = value[0] if isinstance(value, list) and value else value
    if isinstance(val, dict):
        if "@id" in val:
            target = make_valid_linkml_name(val["@id"])
            all_class_names.add(target)
            original_class_uris.setdefault(target, val["@id"])
            property_ranges[prop_clean] = target
        elif "@value" in val:
            property_ranges[prop_clean] = "string"
    elif isinstance(val, str):
        property_ranges[prop_clean] = "string"
    else:
        property_ranges.setdefault(prop_clean, "string")


def _build_classes(
    all_class_names: set[str],
    class_properties: dict[str, list[str]],
    original_class_uris: dict[str, str],
    slot_name_mapping: dict[str, str],
    label_map: dict[str, str],
    prefixes: dict[str, str],
) -> dict[str, ClassDefinition]:
    """Build :class:`ClassDefinition` objects for every class."""
    classes: dict[str, ClassDefinition] = {}
    for class_name in all_class_names:
        class_slots = [slot_name_mapping.get(p, p) for p in class_properties.get(class_name, [])]
        original_uri = original_class_uris.get(class_name, class_name)
        class_uri = _expand_uri(original_uri, prefixes)
        classes[class_name] = ClassDefinition(
            name=class_name,
            description=label_map.get(
                original_class_uris.get(class_name, ""),
                f"Class representing {class_name}",
            ),
            slots=class_slots,
            class_uri=class_uri,
        )
    return classes


def _build_slots(
    all_slot_names: set[str],
    slot_name_mapping: dict[str, str],
    property_ranges: dict[str, str],
    property_descriptions: dict[str, str],
    original_slot_uris: dict[str, str],
    class_properties: dict[str, list[str]],
    all_class_names: set[str],
    prefixes: dict[str, str],
) -> dict[str, SlotDefinition]:
    """Build :class:`SlotDefinition` objects for every slot."""
    slots: dict[str, SlotDefinition] = {}
    for orig_slot in all_slot_names:
        final = slot_name_mapping[orig_slot]
        rng = property_ranges.get(orig_slot, "string")
        if rng not in all_class_names and rng not in (
            "string",
            "uriorcurie",
        ):
            rng = "string"

        original_uri = original_slot_uris.get(orig_slot, orig_slot)
        slot_uri = _expand_uri(original_uri, prefixes)

        slot_def = SlotDefinition(
            name=final,
            description=property_descriptions.get(
                orig_slot,
                f"Property {orig_slot}",
            ),
            range=rng,
            slot_uri=slot_uri,
        )
        domain_classes = [c for c, props in class_properties.items() if orig_slot in props]
        if domain_classes:
            slot_def.domain_of = domain_classes
            slot_def.owner = domain_classes[0]
        slots[final] = slot_def
    return slots


def to_linkml(
    jsonld: dict[str, Any],
    *,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> SchemaDefinition:
    """Generate a LinkML ``SchemaDefinition`` from a JSON-LD dict.

    Parameters
    ----------
    jsonld:
        JSON-LD document with ``@context``, ``@graph``, and
        optionally ``_labels``.
    schema_name:
        Name for the schema (also used as default prefix).
    schema_description:
        Human-readable description.
    schema_base_uri:
        Base URI; defaults to ``https://w3id.org/{schema_name}/``.

    Returns
    -------
    SchemaDefinition
    """
    schema_name, schema_uri, description = _derive_schema_meta(
        jsonld,
        schema_name,
        schema_description,
        schema_base_uri,
    )
    prefixes = _build_prefixes(
        schema_name,
        schema_uri,
        jsonld.get("@context", {}),
    )
    schema = _build_empty_schema(
        schema_name,
        schema_uri,
        description,
        prefixes,
    )

    items = _collect_graph_items(jsonld)
    if items is None:
        return schema

    label_map: dict[str, str] = jsonld.get("_labels", {})
    (
        all_class_names,
        all_slot_names,
        class_properties,
        property_ranges,
        property_descriptions,
        original_class_uris,
        original_slot_uris,
    ) = _scan_graph_items(items, label_map)

    conflicts = all_class_names & all_slot_names
    slot_name_mapping = {s: (f"has_{s}" if s in conflicts else s) for s in all_slot_names}

    schema.classes = _build_classes(
        all_class_names,
        class_properties,
        original_class_uris,
        slot_name_mapping,
        label_map,
        prefixes,
    )
    schema.slots = _build_slots(
        all_slot_names,
        slot_name_mapping,
        property_ranges,
        property_descriptions,
        original_slot_uris,
        class_properties,
        all_class_names,
        prefixes,
    )
    return schema


def _expand_uri(
    uri_or_curie: str,
    prefixes: dict[str, str],
) -> str:
    """Expand a CURIE to a full URI using *prefixes*, or return as-is."""
    if uri_or_curie.startswith(("http://", "https://")):
        return uri_or_curie
    if ":" in uri_or_curie:
        prefix, local = uri_or_curie.split(":", 1)
        ns = prefixes.get(prefix)
        if ns:
            return ns + local
    return uri_or_curie


# ── YAML serialisation ───────────────────────────────────────────


def to_linkml_yaml(
    jsonld: dict[str, Any],
    *,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Return the LinkML schema as a YAML string.

    Parameters are the same as :func:`to_linkml`.
    """
    linkml_schema = to_linkml(
        jsonld,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )
    return cast(str, YAMLGenerator(linkml_schema).serialize())
