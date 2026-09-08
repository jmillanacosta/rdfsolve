"""Read explicit schema storage profiles without remote context loads."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from rdflib import RDF, Graph

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema


def _check_inline_contexts(value: Any) -> None:
    """Reject external JSON-LD contexts, including nested imports."""
    if isinstance(value, list):
        for item in value:
            _check_inline_contexts(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            if key == "@import":
                raise ValueError("External JSON-LD context imports are not supported")
            if key == "@context":
                contexts = item if isinstance(item, list) else [item]
                if any(
                    context is not None and not isinstance(context, dict) for context in contexts
                ):
                    raise ValueError("JSON-LD contexts must be inline objects")
            _check_inline_contexts(item)


def read_schema(raw: dict[str, Any] | list[dict[str, Any]]) -> MinedSchema:
    """Read canonical JSON or VoID JSON-LD."""
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern
    from rdfsolve.schema_models.readers.void import VOID, void_graph_to_minedschema

    if isinstance(raw, dict) and "format" in raw:
        if raw["format"] != "rdfsolve.mined-schema":
            raise ValueError("Unsupported schema format")
        if type(raw.get("version")) is not int or raw["version"] != 1:
            raise ValueError("Unsupported schema format version")
        if set(raw) != {"format", "version", "schema"}:
            raise ValueError("Expected format, version, and schema fields")
        schema = raw["schema"]
        if (
            not isinstance(schema, dict)
            or not {"patterns", "about"} <= set(schema)
            or set(schema) - {"patterns", "about", "enrichment", "shapes", "navigation", "source_metadata"}
        ):
            raise ValueError("Expected patterns and about fields in canonical schema")
        if isinstance(schema["patterns"], list):
            for pattern in schema["patterns"]:
                if isinstance(pattern, dict) and set(pattern) - set(SchemaPattern.model_fields):
                    raise ValueError("Unknown canonical pattern fields")
        return MinedSchema.model_validate(schema)

    _check_inline_contexts(raw)
    if isinstance(raw, dict) and "@about" in raw:
        raise ValueError("Legacy adjacency schemas are not supported; use canonical schema JSON")

    if not isinstance(raw, (dict, list)) or not raw:
        raise ValueError("Unsupported schema document")
    graph = Graph().parse(data=json.dumps(raw), format="json-ld")
    if not (
        (None, RDF.type, VOID.Dataset) in graph
        or (None, RDF.type, VOID.Linkset) in graph
        or (None, VOID.classPartition, None) in graph
    ):
        raise ValueError("Expected canonical JSON or VoID JSON-LD")
    return void_graph_to_minedschema(graph)
