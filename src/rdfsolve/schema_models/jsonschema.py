"""JSON Schema and Pydantic model generation from mined patterns."""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, create_model


def mined_schema_to_pydantic_models(
    mined_schema: Any,
) -> dict[str, type[BaseModel]]:
    """Build Pydantic models in memory from mined patterns."""
    class_patterns = _group_by_class(mined_schema.patterns)
    models: dict[str, type[BaseModel]] = {}

    for class_uri, patterns in class_patterns.items():
        class_name = _make_class_name(class_uri)
        fields = _build_fields(patterns, models)

        class_desc = None
        for pat in patterns:
            if pat.subject_description:
                class_desc = pat.subject_description
                break

        model = create_model(
            class_name,
            __doc__=class_desc,
            **fields,
        )
        models[class_name] = model

    return models


def _build_fields(
    patterns: list,
    existing_models: dict[str, type[BaseModel]],
) -> dict[str, tuple[type, Any]]:
    """Build field definitions for create_model()."""
    fields = {}

    for pat in patterns:
        prop_name = _make_property_name(pat.property_uri)
        field_type = _pattern_to_type(pat, existing_models)
        field_default = _build_field_default(pat)
        fields[prop_name] = (field_type, field_default)

    return fields


def _pattern_to_type(
    pat,
    existing_models: dict[str, type[BaseModel]],
) -> type:
    """Map pattern to Python type."""
    if pat.object_class == "Literal":
        base_type = _datatype_to_python_type(pat.datatype)
    elif pat.object_class in ("Resource", "BlankNode"):
        base_type = str
    else:
        class_name = _make_class_name(pat.object_class)
        base_type = existing_models.get(class_name, str)

    if pat.max_count == 1:
        if pat.min_count and pat.min_count > 0:
            return base_type
        return base_type | None
    else:
        return list[base_type]


def _build_field_default(pat) -> Any:
    """Build Field() with metadata."""
    kwargs = {}

    if pat.min_count is None or pat.min_count == 0:
        kwargs["default"] = None

    if pat.property_description:
        kwargs["description"] = pat.property_description

    if pat.value_examples:
        kwargs["examples"] = pat.value_examples[:3]

    if kwargs:
        return Field(**kwargs)
    return ...


def _datatype_to_python_type(datatype: str | None) -> type:
    """Map XSD datatype to Python type."""
    mapping = {
        "http://www.w3.org/2001/XMLSchema#string": str,
        "http://www.w3.org/2001/XMLSchema#integer": int,
        "http://www.w3.org/2001/XMLSchema#int": int,
        "http://www.w3.org/2001/XMLSchema#long": int,
        "http://www.w3.org/2001/XMLSchema#float": float,
        "http://www.w3.org/2001/XMLSchema#double": float,
        "http://www.w3.org/2001/XMLSchema#decimal": float,
        "http://www.w3.org/2001/XMLSchema#boolean": bool,
        "http://www.w3.org/2001/XMLSchema#date": str,
        "http://www.w3.org/2001/XMLSchema#dateTime": str,
        "http://www.w3.org/2001/XMLSchema#anyURI": str,
    }
    return mapping.get(datatype, str)


def _group_by_class(patterns: list) -> dict[str, list]:
    """Group patterns by subject class."""
    from collections import defaultdict

    grouped: dict[str, list] = defaultdict(list)
    for pat in patterns:
        grouped[pat.subject_class].append(pat)
    return dict(grouped)


def _make_class_name(uri: str) -> str:
    """Extract class name from URI."""
    if "#" in uri:
        name = uri.split("#")[-1]
    elif "/" in uri:
        name = uri.split("/")[-1]
    else:
        name = uri

    name = name.replace("-", "_").replace(".", "_")
    if not name or not name[0].isalpha():
        name = "C_" + name
    return name


def _make_property_name(uri: str) -> str:
    """Extract property name from URI."""
    if "#" in uri:
        name = uri.split("#")[-1]
    elif "/" in uri:
        name = uri.split("/")[-1]
    else:
        name = uri

    name = name.replace("-", "_").replace(".", "_")
    if not name or not name[0].isalpha():
        name = "p_" + name
    return name


def mined_schema_to_jsonschema(
    mined_schema: Any,
    schema_name: str | None = None,
) -> dict[str, Any]:
    """Generate JSON Schema directly from mined patterns.

    Builds proper $ref references for object properties.
    """
    class_patterns = _group_by_class(mined_schema.patterns)
    definitions = {}

    for class_uri, patterns in class_patterns.items():
        class_name = _make_class_name(class_uri)

        # Get class description
        class_desc = None
        for pat in patterns:
            if pat.subject_description:
                class_desc = pat.subject_description
                break

        # Build properties
        properties = {}
        required = []

        for pat in patterns:
            prop_name = _make_property_name(pat.property_uri)
            prop_schema = _pattern_to_jsonschema_property(pat)
            properties[prop_name] = prop_schema

            # Add to required if min_count > 0
            if pat.min_count and pat.min_count > 0:
                required.append(prop_name)

        # Build class schema
        class_schema = {
            "type": "object",
            "title": class_name,
            "properties": properties,
        }

        if class_desc:
            class_schema["description"] = class_desc

        if required:
            class_schema["required"] = required

        definitions[class_name] = class_schema

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": schema_name or mined_schema.about.dataset_name,
        "description": mined_schema.about.description,
        "$defs": definitions,
    }


def _pattern_to_jsonschema_property(pat) -> dict[str, Any]:
    """Convert a pattern to a JSON Schema property definition."""
    prop_schema: dict[str, Any] = {}

    # Determine base type
    if pat.object_class == "Literal":
        # Literal value - use datatype
        json_type = _xsd_to_json_type(pat.datatype)
        base_schema = {"type": json_type}
    elif pat.object_class in ("Resource", "BlankNode"):
        # URI or blank node - use string
        base_schema = {"type": "string"}
    else:
        # Object property - reference to another class
        class_name = _make_class_name(pat.object_class)
        base_schema = {"$ref": f"#/$defs/{class_name}"}

    # Handle cardinality
    if pat.max_count == 1:
        # Single value
        prop_schema = base_schema.copy()
        if pat.min_count is None or pat.min_count == 0:
            # Optional - use anyOf with null
            prop_schema = {
                "anyOf": [base_schema, {"type": "null"}],
                "default": None,
            }
    else:
        # Array
        prop_schema = {
            "type": "array",
            "items": base_schema,
        }
        if pat.min_count is None or pat.min_count == 0:
            prop_schema["default"] = None

    # Add title
    prop_name = _make_property_name(pat.property_uri)
    prop_schema["title"] = _camel_to_title(prop_name)

    # Add description
    if pat.property_description:
        prop_schema["description"] = pat.property_description

    # Add examples
    if pat.value_examples:
        prop_schema["examples"] = pat.value_examples[:3]

    return prop_schema


def _xsd_to_json_type(datatype: str | None) -> str:
    """Map XSD datatype to JSON Schema type."""
    if not datatype:
        return "string"

    mapping = {
        "http://www.w3.org/2001/XMLSchema#string": "string",
        "http://www.w3.org/2001/XMLSchema#integer": "integer",
        "http://www.w3.org/2001/XMLSchema#int": "integer",
        "http://www.w3.org/2001/XMLSchema#long": "integer",
        "http://www.w3.org/2001/XMLSchema#float": "number",
        "http://www.w3.org/2001/XMLSchema#double": "number",
        "http://www.w3.org/2001/XMLSchema#decimal": "number",
        "http://www.w3.org/2001/XMLSchema#boolean": "boolean",
        "http://www.w3.org/2001/XMLSchema#date": "string",
        "http://www.w3.org/2001/XMLSchema#dateTime": "string",
        "http://www.w3.org/2001/XMLSchema#anyURI": "string",
    }
    return mapping.get(datatype, "string")


def _camel_to_title(name: str) -> str:
    """Convert camelCase or snake_case to Title Case."""
    # Replace underscores with spaces
    name = name.replace("_", " ")
    # Capitalize first letter of each word
    return " ".join(word.capitalize() for word in name.split())


def mined_schema_to_pydantic_file(
    mined_schema: Any,
    output_path: str | Path,
    schema_name: str | None = None,
) -> None:
    """Generate .py file with Pydantic models from mined patterns."""
    json_schema = mined_schema_to_jsonschema(mined_schema, schema_name)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
    ) as f:
        json.dump(json_schema, f)
        temp_path = f.name

    try:
        subprocess.run(
            [
                "datamodel-codegen",
                "--input",
                temp_path,
                "--output",
                str(output_path),
                "--input-file-type",
                "jsonschema",
            ],
            check=True,
        )
    finally:
        Path(temp_path).unlink()
