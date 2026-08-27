"""LinkML schema generation from JSON-LD.

Converts a rdfsolve JSON-LD schema dict (``@context`` + ``@graph``)
into a LinkML ``SchemaDefinition`` or its YAML serialization.
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
    "mined_schema_to_linkml",
    "to_linkml",
    "to_linkml_yaml",
]


# Name-cleaning helpers


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


# Core conversion


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

    # Sanitize schema_name for use in URIs (no spaces)
    sanitized_for_uri = re.sub(r"\s+", "_", schema_name)
    sanitized_for_uri = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized_for_uri)

    schema_uri = (
        f"https://w3id.org/{sanitized_for_uri}/"
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
    about: dict[str, Any] | None = None,
    original_name: str | None = None,
) -> SchemaDefinition:
    """Construct a :class:`SchemaDefinition` with types and metadata.

    Args:
        schema_name: Sanitized NCName-compatible schema name
        schema_uri: Schema URI
        description: Schema description
        prefixes: Prefix mappings
        about: AboutMetadata dict from JSON-LD
        original_name: Original schema name (before sanitization) for title
    """
    about = about or {}
    original_name = original_name or schema_name

    # Build annotations from AboutMetadata
    annotations = {}
    if about.get("pattern_count"):
        annotations["rdfsolve:pattern_count"] = str(about["pattern_count"])
    if about.get("class_count"):
        annotations["void:classes"] = str(about["class_count"])
    if about.get("property_count"):
        annotations["void:properties"] = str(about["property_count"])
    if about.get("strategy"):
        annotations["rdfsolve:strategy"] = about["strategy"]

    # Store generated_by as annotation instead of created_by (which requires URI/CURIE)
    if about.get("generated_by"):
        annotations["rdfsolve:generated_by"] = about["generated_by"]

    # Additional metadata annotations
    if about.get("homepage"):
        annotations["foaf:homepage"] = about["homepage"]
    if about.get("source_creator"):
        # Store creators as comma-separated list
        creators = about["source_creator"]
        if isinstance(creators, list):
            annotations["dcterms:creator"] = ", ".join(creators)
        else:
            annotations["dcterms:creator"] = str(creators)
    if about.get("source_publisher"):
        annotations["dcterms:publisher"] = about["source_publisher"]
    if about.get("source_version"):
        annotations["dcterms:hasVersion"] = about["source_version"]
    if about.get("source_version_iri"):
        annotations["owl:versionIRI"] = about["source_version_iri"]
    if about.get("source_issued"):
        annotations["dcterms:issued"] = about["source_issued"]
    if about.get("source_modified"):
        annotations["dcterms:modified"] = about["source_modified"]

    # Title: prefer explicit title from metadata, fallback to dataset_name
    title_value = about.get("title") or about.get("dataset_name") or original_name

    # Description: prefer explicit description from metadata
    description_value = (
        about.get("description")
        or description
        or f"Schema mined from {about.get('endpoint', 'unknown endpoint')}"
    )

    return SchemaDefinition(
        id=about.get("schema_uri") or schema_uri,
        name=schema_name,
        version=about.get("schema_version") or "0.0.0",
        title=title_value,
        description=description_value,
        default_prefix=schema_name,
        prefixes=prefixes,
        source=about.get("endpoint") or None,
        license=about.get("source_license") or None,
        created_on=about.get("generated_at") or None,
        annotations=annotations if annotations else None,
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

    # Sanitize schema_name for LinkML NCName requirements
    sanitized_name = re.sub(r"\s+", "_", schema_name)
    sanitized_name = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized_name)
    if sanitized_name and sanitized_name[0].isdigit():
        sanitized_name = f"schema_{sanitized_name}"

    prefixes = _build_prefixes(
        sanitized_name,
        schema_uri,
        jsonld.get("@context", {}),
    )
    about = jsonld.get("@about", {})
    schema = _build_empty_schema(
        sanitized_name,
        schema_uri,
        description,
        prefixes,
        about=about,
        original_name=schema_name,
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


# YAML serialization


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


def mined_schema_to_linkml(
    mined_schema: Any,  # MinedSchema, but avoiding circular import
    schema_name: str | None = None,
    schema_description: str | None = None,
) -> SchemaDefinition:
    """Generate LinkML SchemaDefinition directly from MinedSchema patterns.

    This builds the LinkML schema from the core model without going through
    JSON-LD or VoID, ensuring the schema represents the mined patterns
    directly.

    Parameters
    ----------
    mined_schema:
        MinedSchema object with patterns and metadata
    schema_name:
        Name for the schema (defaults to dataset_name from metadata)
    schema_description:
        Human-readable description

    Returns
    -------
    SchemaDefinition
        LinkML schema built from patterns
    """
    from collections import defaultdict

    # Get schema name from metadata if not provided
    schema_name = schema_name or mined_schema.about.dataset_name or "rdf_schema"

    # Sanitize for LinkML NCName requirements
    sanitized_name = re.sub(r"\s+", "_", schema_name)
    sanitized_name = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized_name)
    if sanitized_name and sanitized_name[0].isdigit():
        sanitized_name = f"schema_{sanitized_name}"

    # Build schema URI - use configurable base URI for shapes
    # This ensures SHACL shapes are in a separate namespace from
    # the original class URIs (which appear in sh:targetClass)
    from rdfsolve.config import get_base_uri

    base_uri = get_base_uri()
    schema_uri = f"{base_uri}/shapes/{sanitized_name}/"

    # Build description
    if not schema_description:
        if mined_schema.about.description:
            schema_description = mined_schema.about.description
        elif mined_schema.about.endpoint:
            schema_description = f"Schema mined from {mined_schema.about.endpoint}"
        else:
            schema_description = f"LinkML schema generated from {schema_name}"

    # Build prefixes - collect from patterns
    # Use "rdfsolve_shapes" as the default prefix for the shapes namespace
    shapes_prefix = (
        f"rdfsolve_shapes_{sanitized_name}" if sanitized_name != "rdf_schema" else "rdfsolve_shapes"
    )
    prefixes: dict[str, str] = {
        shapes_prefix: schema_uri,
        "linkml": "https://w3id.org/linkml/",
        "schema": "http://schema.org/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "owl": "http://www.w3.org/2002/07/owl#",
    }

    # Extract prefixes from URIs in patterns
    for pat in mined_schema.patterns:
        for uri in [pat.subject_class, pat.property_uri, pat.object_class]:
            if uri and not uri.startswith(("http://", "https://")):
                continue
            if uri in ("Literal", "Resource", "BlankNode"):
                continue
            # Try to get CURIE and extract prefix
            curie = curie_from_iri(uri)
            if curie and ":" in curie:
                prefix, _ = curie.split(":", 1)
                # Get namespace by removing local part
                if "#" in uri:
                    ns = uri.rsplit("#", 1)[0] + "#"
                elif "/" in uri:
                    ns = uri.rsplit("/", 1)[0] + "/"
                else:
                    continue
                if prefix and ns:
                    prefixes[prefix] = ns

    # Build empty schema with metadata
    about_dict = (
        mined_schema.about.model_dump() if hasattr(mined_schema.about, "model_dump") else {}
    )
    # Use sanitized_name as schema name (preserves dataset identity)
    # but shapes_prefix as default_prefix (ensures shapes use rdfsolve namespace)
    schema = _build_empty_schema(
        sanitized_name,  # Use dataset name as schema name
        schema_uri,
        schema_description,
        prefixes,
        about=about_dict,
        original_name=schema_name,
    )
    # Override default_prefix to use shapes namespace
    schema.default_prefix = shapes_prefix

    # Build classes and slots from patterns
    # Group patterns by subject class
    class_slots: dict[str, set[str]] = defaultdict(set)
    slot_ranges: dict[str, set[str]] = defaultdict(set)
    slot_domains: dict[str, set[str]] = defaultdict(set)
    all_classes: set[str] = set()
    slot_labels: dict[str, str] = {}
    class_labels: dict[str, str] = {}

    for pat in mined_schema.patterns:
        # Convert URIs to valid LinkML names
        subject_name = make_valid_linkml_name(pat.subject_class)
        property_name = make_valid_linkml_name(pat.property_uri)

        all_classes.add(subject_name)
        class_slots[subject_name].add(property_name)
        slot_domains[property_name].add(subject_name)

        # Store labels
        if pat.subject_label:
            class_labels[subject_name] = pat.subject_label
        if pat.property_label:
            slot_labels[property_name] = pat.property_label

        # Determine range based on object_class
        if pat.object_class == "Literal":
            # Map XSD datatype to LinkML type
            if pat.datatype:
                dt_curie = (
                    curie_from_iri(pat.datatype)
                    if pat.datatype.startswith("http")
                    else pat.datatype
                )
                if "string" in dt_curie.lower():
                    slot_ranges[property_name].add("string")
                elif "integer" in dt_curie.lower() or "int" in dt_curie.lower():
                    slot_ranges[property_name].add("integer")
                elif "boolean" in dt_curie.lower():
                    slot_ranges[property_name].add("boolean")
                elif "float" in dt_curie.lower() or "double" in dt_curie.lower():
                    slot_ranges[property_name].add("float")
                elif "date" in dt_curie.lower():
                    slot_ranges[property_name].add("date")
                else:
                    slot_ranges[property_name].add("string")
            else:
                slot_ranges[property_name].add("string")
        elif pat.object_class == "Resource":
            # Untyped URI - use uriorcurie
            slot_ranges[property_name].add("uriorcurie")
        elif pat.object_class == "BlankNode":
            # Blank node - use string for now
            slot_ranges[property_name].add("string")
        else:
            # Typed object - use the class name
            object_name = make_valid_linkml_name(pat.object_class)
            all_classes.add(object_name)
            slot_ranges[property_name].add(object_name)
            if pat.object_label:
                class_labels[object_name] = pat.object_label

    # Build ClassDefinition objects
    classes: dict[str, ClassDefinition] = {}
    for class_name in all_classes:
        slots = sorted(class_slots.get(class_name, set()))

        # Get original URI for class_uri
        original_uri = None
        for pat in mined_schema.patterns:
            if make_valid_linkml_name(pat.subject_class) == class_name:
                original_uri = pat.subject_class
                break
            if (
                pat.object_class not in ("Literal", "Resource", "BlankNode")
                and make_valid_linkml_name(pat.object_class) == class_name
            ):
                original_uri = pat.object_class
                break

        classes[class_name] = ClassDefinition(
            name=class_name,
            description=class_labels.get(class_name, f"Class {class_name}"),
            slots=slots,
            class_uri=original_uri,
        )

    # Build SlotDefinition objects
    # Collect all slot names from the dictionaries
    all_slot_names: set[str] = set()
    for slots_set in class_slots.values():
        all_slot_names.update(slots_set)
    all_slot_names.update(slot_ranges.keys())
    all_slot_names.update(slot_domains.keys())

    slots_dict: dict[str, SlotDefinition] = {}

    for slot_name in all_slot_names:
        # Determine range - if multiple ranges, pick most specific
        ranges = slot_ranges.get(slot_name, {"string"})
        if len(ranges) == 1:
            range_val = next(iter(ranges))
        else:
            # Multiple ranges - prefer class names over primitives
            class_ranges = [
                r
                for r in ranges
                if r not in ("string", "integer", "boolean", "float", "date", "uriorcurie")
            ]
            if class_ranges:
                range_val = class_ranges[0]  # Pick first class
            else:
                range_val = "string"  # Default to string

        # Get original URI for slot_uri
        original_uri = None
        for pat in mined_schema.patterns:
            if make_valid_linkml_name(pat.property_uri) == slot_name:
                original_uri = pat.property_uri
                break

        slot_def = SlotDefinition(
            name=slot_name,
            description=slot_labels.get(slot_name, f"Property {slot_name}"),
            range=range_val,
            slot_uri=original_uri,
        )

        # Add domain info
        domains = sorted(slot_domains.get(slot_name, set()))
        if domains:
            slot_def.domain_of = domains
            slot_def.owner = domains[0]

        slots_dict[slot_name] = slot_def

    schema.classes = classes
    schema.slots = slots_dict

    return schema
