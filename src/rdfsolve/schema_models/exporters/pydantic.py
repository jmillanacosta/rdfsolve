"""Generate typed views of observed RDF patterns."""

from __future__ import annotations

import keyword
import re
from collections import defaultdict
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from rdfsolve._uri import uri_to_curie
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.exporters.text import clip_description

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern


def _identifier(text: str, *, class_name: bool = False) -> str:
    words = re.findall(r"[a-zA-Z0-9]+", text)
    name = "".join(w[:1].upper() + w[1:] for w in words) if class_name else "_".join(words).lower()
    if not name or name[0].isdigit():
        name = ("Class" if class_name else "prop_") + name
    if keyword.iskeyword(name):
        name += "_"
    return name


def _unique_name(base: str, iri: str, used: set[str]) -> str:
    name = base
    if name in used:
        name = f"{base}_{sha256(iri.encode()).hexdigest()[:8]}"
    while name in used:
        name += "_"
    used.add(name)
    return name


def _value_type(pattern: SchemaPattern, names: dict[str, str]) -> str:
    if pattern.object_class == "BlankNode":
        return "RDFResource | str"
    if pattern.object_class == "Resource":
        return "str"
    if pattern.object_class != "Literal":
        return f"{names[pattern.object_class]} | str"
    datatype = (pattern.datatype or "").rsplit("#", 1)[-1]
    if datatype in {
        "integer",
        "int",
        "long",
        "short",
        "byte",
        "nonNegativeInteger",
        "positiveInteger",
        "nonPositiveInteger",
        "negativeInteger",
        "unsignedInt",
        "unsignedLong",
        "unsignedShort",
        "unsignedByte",
    }:
        return "int"
    return {
        "boolean": "bool",
        "float": "float",
        "double": "float",
        "decimal": "Decimal",
        "date": "date",
        "dateTime": "datetime",
        "time": "time",
        "base64Binary": "bytes",
    }.get(datatype, "str")


def to_pydantic(
    schema: MinedSchema, schema_name: str | None = None, *, trim_descriptions: int | None = None
) -> str:
    """Use labels for names and retain IRIs in schema metadata."""
    labels: dict[str, set[str]] = defaultdict(set)
    grouped: dict[str, dict[str, list[SchemaPattern]]] = defaultdict(lambda: defaultdict(list))
    for pattern in schema.patterns:
        grouped[pattern.subject_class][pattern.property_uri].append(pattern)
        for iri, label in (
            (pattern.subject_class, pattern.subject_label),
            (pattern.object_class, pattern.object_label),
        ):
            if iri not in _SENTINEL_OBJECTS and label:
                labels[iri].add(label)

    for annotation in schema.enrichment.labels:
        labels[annotation.term_iri].add(annotation.text.value)

    used = {
        "RDFResource",
        "BaseModel",
        "ConfigDict",
        "Field",
        "ClassVar",
        "Any",
        "RDF_NAVIGATION",
        "RDF_NAVIGATION_SUMMARY",
        "SHACL_PROFILES",
        "Decimal",
        "date",
        "datetime",
        "time",
        "str",
        "int",
        "float",
        "bool",
        "bytes",
        "list",
        "DATASET_METADATA",
    }
    names: dict[str, str] = {}
    for iri in sorted(schema.get_classes()):
        curie = uri_to_curie(iri)[0]
        meaningful = sorted(label for label in labels[iri] if label not in {iri, curie})
        base = _identifier(meaningful[0] if meaningful else curie, class_name=True)
        names[iri] = _unique_name(base, iri, used)

    profiles: dict[str, list[dict[str, Any]]] = {}
    if schema.shapes is not None:
        import logging

        for shape in schema.shapes.node_shapes:
            if shape.target_class:
                profiles.setdefault(shape.target_class, []).append(shape.model_dump(mode="json"))
        logging.getLogger(__name__).warning(
            "rdfsolve Pydantic generation retains SHACL profiles as metadata; it does not enforce SHACL "
            "paths, qualified counts, closed shapes, or RDF value-node cardinalities."
        )
    routes: dict[str, list[dict[str, Any]]] = {}
    if schema.navigation is not None:
        from rdfsolve.schema_models.exporters.navigation import navigation_metadata

        for route in schema.navigation.paths:
            item = navigation_metadata(route)
            routes.setdefault(route.steps[0].subject_class, []).append(item)

    metadata = {
        "dataset_name": schema_name or schema.about.dataset_name,
        "schema_version": schema.about.schema_version,
        "source_version": schema.about.source_version,
        "source_version_iri": schema.about.source_version_iri,
        "generated_at": schema.about.generated_at,
        "endpoint": schema.about.endpoint,
        "graph_uris": schema.about.graph_uris,
        "rdf_dataset": schema.about.model_dump(mode="json"),
    }
    summary = (
        schema.navigation.model_dump(mode="json", exclude={"paths"})
        if schema.navigation is not None
        else None
    )
    lines = [
        '"""Typed views of observed RDF patterns. Fields do not prove cardinality or completeness."""',
        "from __future__ import annotations",
        "",
        "from datetime import date, datetime, time",
        "from decimal import Decimal",
        "from typing import Any, ClassVar",
        "from pydantic import BaseModel, ConfigDict, Field",
        "",
        f"DATASET_METADATA = {metadata!r}",
        f"RDF_NAVIGATION = {routes!r}",
        f"RDF_NAVIGATION_SUMMARY = {summary!r}",
        f"SHACL_PROFILES = {profiles!r}",
        "",
        "class RDFResource(BaseModel):",
        "    model_config = ConfigDict(populate_by_name=True, extra='allow')",
        "    uri: str = Field(alias='@id', min_length=1)",
        "    rdf_type: list[str] = Field(default_factory=list, alias='@type')",
        "    rdf_terms: dict[str, list[dict[str, Any]]] = Field(default_factory=dict, repr=False)",
        "    rdf_loaded_fields: list[str] = Field(default_factory=list, repr=False)",
        "    rdf_source: dict[str, Any] = Field(default_factory=dict, repr=False)",
        "",
    ]
    for iri, name in names.items():
        class_examples = [
            {"@id": term.json_value()} for term in schema.enrichment.class_examples.get(iri, [])
        ]
        lines.extend(
            [
                f"class {name}(RDFResource):",
                f"    {clip_description(schema.enrichment.description(iri) or ('Observed type ' + iri), trim_descriptions)!r}",
                f"    rdf_class_iri: ClassVar[str] = {iri!r}",
                f"    rdf_navigation: ClassVar[list[dict[str, Any]]] = RDF_NAVIGATION.get({iri!r}, [])",
                f"    rdf_shapes: ClassVar[list[dict[str, Any]]] = SHACL_PROFILES.get({iri!r}, [])",
                f"    model_config = ConfigDict(json_schema_extra={{'rdf_class_iri': {iri!r}, 'examples': {class_examples!r}, 'rdf_navigation': RDF_NAVIGATION.get({iri!r}, []), 'rdf_shapes': SHACL_PROFILES.get({iri!r}, []), **DATASET_METADATA}})",
            ]
        )
        fields = (
            set(dir(BaseModel))
            | used
            | {
                "uri",
                "rdf_type",
                "rdf_class_iri",
                "rdf_navigation",
                "rdf_shapes",
                "rdf_terms",
                "rdf_loaded_fields",
                "rdf_source",
            }
        )
        for prop, patterns in sorted(grouped[iri].items()):
            local = re.split(r"[/#:]", prop)[-1]
            field = _identifier(local)
            if field.startswith("model_"):
                field = "prop_" + field
            field = _unique_name(field, prop, fields)
            types = sorted({part for p in patterns for part in _value_type(p, names).split(" | ")})
            value_type = " | ".join(types)
            description = schema.enrichment.description(prop) or "; ".join(
                sorted({p.property_label for p in patterns if p.property_label})
            )
            description = clip_description(description, trim_descriptions) or ""
            # Mining shows values, not a maximum count per subject.
            examples = [
                example.value.json_value()
                for example in schema.enrichment.examples
                if example.subject_class == iri and example.property_uri == prop
            ]
            rdf_metadata = {
                "rdf_property_iri": prop,
                "rdf_path": {"operator": "predicate", "iri": prop, "items": []},
                "rdf_patterns": [p.model_dump(mode="json") for p in patterns],
                "rdf_examples": [
                    e.model_dump(mode="json")
                    for e in schema.enrichment.examples
                    if e.subject_class == iri and e.property_uri == prop
                ],
            }
            lines.append(
                f"    {field}: {value_type} | list[{value_type}] | None = Field(None, "
                f"alias={prop!r}, description={description!r}, examples={examples!r}, json_schema_extra={rdf_metadata!r})"
            )
        seen_paths = set(grouped[iri])
        for profile in profiles.get(iri, []):
            for prop in profile["property_shapes"]:
                from rdfsolve.schema_models.exporters.paths import path_to_sparql
                from rdfsolve.schema_models.paths import PropertyPath

                path = (
                    PropertyPath(operator="predicate", iri=prop["path"])
                    if isinstance(prop["path"], str)
                    else PropertyPath.model_validate(prop["path"])
                )
                key = path.iri or path_to_sparql(path)
                if key in seen_paths:
                    continue
                seen_paths.add(key)
                base = prop.get("name") or (path.iri or "").rsplit("/", 1)[-1].rsplit("#", 1)[-1]
                field = _unique_name(_identifier(base or "path"), str(key), fields)
                if field.startswith("model_"):
                    field = _unique_name("prop_" + field, str(key), fields)
                description = clip_description(prop.get("description"), trim_descriptions) or ""
                path_metadata = {
                    "rdf_path": path.model_dump(mode="json"),
                    "shacl_shape": prop.get("uri"),
                }
                lines.append(
                    f"    {field}: Any = Field(None, description={description!r}, json_schema_extra={path_metadata!r})"
                )
        lines.append("")
    for name in names.values():
        lines.append(f"{name}.model_rebuild(_types_namespace=globals())")
    return "\n".join(lines) + "\n"


def build_pydantic_classes(schema: MinedSchema) -> dict[str, type[BaseModel]]:
    """Load only code produced by this exporter, never supplied Python source."""
    namespace: dict[str, Any] = {"__name__": "rdfsolve.generated"}
    exec(compile(to_pydantic(schema), "<rdfsolve generated models>", "exec"), namespace)  # noqa: S102
    return {
        name: value
        for name, value in namespace.items()
        if isinstance(value, type)
        and issubclass(value, BaseModel)
        and hasattr(value, "rdf_class_iri")
    }
