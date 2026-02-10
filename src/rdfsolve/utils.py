"""
Common utility functions for RDF processing.

This module contains shared utility functions used across the RDFSolve
library to avoid code duplication between different parsers and processors.
"""

from typing import Any, Dict, Optional


def resolve_curie(curie: str, prefixes: Dict[str, str]) -> Optional[str]:
    """
    Convert CURIE to full IRI using given prefixes.

    Args:
        curie: CURIE string to resolve (e.g., "foaf:name")
        prefixes: Dictionary of prefix mappings

    Returns:
        Full IRI string wrapped in angle brackets, or None if not resolvable
    """
    if not curie or curie in ["BN", "null", "", "[]"]:
        return None

    curie = str(curie).strip()

    # Already full IRI
    if curie.startswith("<") and curie.endswith(">"):
        return curie
    if curie.startswith("http"):
        return f"<{curie}>"

    # Handle 'a' as rdf:type
    if curie == "a":
        return "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

    # Handle CURIE
    if ":" in curie:
        prefix, localname = curie.split(":", 1)
        if prefix in prefixes:
            base_uri = prefixes[prefix].strip("<>")
            return f"<{base_uri}{localname}>"

    return None


def normalize_uri(
    uri_string: str, prefixes: Dict[str, str], source: str, remove_qualifiers: bool = True
) -> Optional[str]:
    """
    Normalize URI strings to full URIs.

    Args:
        uri_string: URI string to normalize
        prefixes: Dictionary of prefix mappings
        source: Source namespace for relative URIs
        remove_qualifiers: Whether to remove qualifiers (* and ?)

    Returns:
        Normalized URI string or None if invalid
    """
    if not uri_string or uri_string in ["BN", "null", ""]:
        return None

    uri_string = str(uri_string).strip()

    # Remove qualifiers (* and ?) if flag is set
    if remove_qualifiers:
        uri_string = uri_string.rstrip("*?")

    # Already a full URI
    if uri_string.startswith("<") and uri_string.endswith(">"):
        return uri_string
    if uri_string.startswith("http"):
        return f"<{uri_string}>"

    # Handle prefix:suffix format
    if ":" in uri_string and not uri_string.startswith("http"):
        prefix, suffix = uri_string.split(":", 1)
        if prefix in prefixes:
            base_uri = prefixes[prefix].rstrip(">")
            if base_uri.startswith("<"):
                base_uri = base_uri[1:]
            return f"<{base_uri}{suffix}>"

    # Handle relative URIs - assume they belong to the source namespace
    if source in prefixes:
        base_uri = prefixes[source].rstrip(">")
        if base_uri.startswith("<"):
            base_uri = base_uri[1:]
        return f"<{base_uri}{uri_string}>"

    return f"<{uri_string}>"


def clean_predicate(predicate: str) -> str:
    """
    Remove cardinality markers from predicate.

    Args:
        predicate: Predicate string potentially with cardinality markers

    Returns:
        Cleaned predicate string
    """
    cleaned = predicate.rstrip("*+?")
    brace_pos = cleaned.rfind("{")
    if brace_pos > 0 and cleaned.endswith("}"):
        cleaned = cleaned[:brace_pos]
    return cleaned.strip()


def is_blank_node(value: Any) -> bool:
    """
    Check if value represents a blank node.

    Args:
        value: Value to check for blank node representation

    Returns:
        True if value represents a blank node
    """
    if isinstance(value, dict):
        keys = list(value.keys())
        if len(keys) == 1 and keys[0] == "[]":
            return True
        # Also check for array key (blank node in Ruby logic)
        if len(keys) == 1 and isinstance(keys[0], list):
            return True
    elif isinstance(value, list):
        if len(value) == 1 and value[0] == "[]":
            return True
        # Empty list after key like "core:range:" indicates blank node
        if len(value) == 1 and isinstance(value[0], dict):
            keys = list(value[0].keys())
            if len(keys) == 1 and keys[0] == "[]":
                return True
    elif value == "[]":
        return True
    return False


def is_example_or_metadata(key: Any, value: Any) -> bool:
    """
    Identify and filter out examples, comments, and metadata.

    Args:
        key: Key to check
        value: Value to check

    Returns:
        True if key/value pair should be filtered out
    """
    if key and isinstance(key, str) and "http" in key:
        return False
    if value and isinstance(value, str) and "http" in value:
        return False

    return False


# ---------------------------------------------------------------------------
# URI display helpers (used by compose, iri, and backend)
# ---------------------------------------------------------------------------


def get_local_name(uri: str) -> str:
    """Extract the local name from a URI.

    Examples::

        >>> get_local_name("http://example.org/foo#Bar")
        'Bar'
        >>> get_local_name("http://example.org/foo/Bar")
        'Bar'
    """
    if "#" in uri:
        return uri.split("#")[-1]
    return uri.rstrip("/").rsplit("/", 1)[-1] if "/" in uri else uri


def compact_uri(uri: str, prefixes: Dict[str, str]) -> str:
    """Compact a URI using the given prefix map.

    Returns ``prefix:localName`` if a match is found, otherwise the
    original URI.
    """
    for pfx, ns in prefixes.items():
        if uri.startswith(ns):
            return f"{pfx}:{uri[len(ns):]}"
    return uri


def expand_curie(curie: str, prefixes: Dict[str, str]) -> str:
    """Expand a CURIE (prefix:local) to a full URI."""
    if ":" not in curie or curie.startswith("http"):
        return curie
    pfx, local = curie.split(":", 1)
    ns = prefixes.get(pfx)
    return f"{ns}{local}" if ns else curie


def shorten_for_display(
    uri: str,
    prefixes: Optional[Dict[str, str]] = None,
) -> str:
    """Shorten a URI for display — try CURIE first, then local name."""
    if prefixes:
        compact = compact_uri(uri, prefixes)
        if compact != uri:
            return compact
    return get_local_name(uri)
