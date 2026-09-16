"""Schema extraction utilities."""

from typing import Any

__all__ = ["extract_class_set", "extract_namespace_set", "extract_predicate_set"]


def extract_class_set(schema: Any) -> set[str]:
    """Extract all class URIs from schema."""
    return set(schema.get_classes())


def extract_predicate_set(schema: Any) -> set[str]:
    """Extract all predicate URIs from schema."""
    return {p.property_uri for p in getattr(schema, "patterns", []) if p.property_uri}


def extract_namespace_set(schema: Any) -> set[str]:
    """Extract all namespace URIs from schema."""
    namespaces = set()
    for uri in extract_class_set(schema) | extract_predicate_set(schema):
        if "#" in uri:
            namespaces.add(uri.rsplit("#", 1)[0] + "#")
        elif "/" in uri:
            namespaces.add(uri.rsplit("/", 1)[0] + "/")
    return namespaces
