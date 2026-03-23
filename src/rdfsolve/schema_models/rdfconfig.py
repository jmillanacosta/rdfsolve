"""RDF-config YAML generation from JSON-LD.

Converts a rdfsolve JSON-LD schema dict to the three YAML files
expected by the `rdf-config <https://github.com/dbcls/rdf-config>`_
tool: ``model.yaml``, ``prefix.yaml``, ``endpoint.yaml``.
"""

from __future__ import annotations

import re
from typing import Any

__all__ = ["to_rdfconfig"]


# ── Public API ───────────────────────────────────────────────────


def to_rdfconfig(
    jsonld: dict[str, Any],
    *,
    endpoint_url: str | None = None,
    endpoint_name: str | None = None,
    graph_uri: str | None = None,
) -> dict[str, str]:
    """Generate RDF-config YAML files from a JSON-LD schema dict.

    Parameters
    ----------
    jsonld:
        JSON-LD document (``@context``, ``@graph``, …).
    endpoint_url:
        SPARQL endpoint URL for ``endpoint.yaml``.
    endpoint_name:
        Label for the endpoint (defaults to ``"endpoint"``).
    graph_uri:
        Optional named-graph URI for ``endpoint.yaml``.

    Returns
    -------
    dict
        Keys ``model``, ``prefix``, ``endpoint`` -> YAML strings.
    """
    prefixes: dict[str, str] = jsonld.get("@context", {})
    graph_data: list[dict[str, Any]] = jsonld.get("@graph", [])

    return {
        "model": _generate_model(graph_data, prefixes),
        "prefix": _generate_prefix(prefixes),
        "endpoint": _generate_endpoint(
            endpoint_url,
            endpoint_name,
            graph_uri,
        ),
    }


# ── prefix.yaml ─────────────────────────────────────────────────


def _generate_prefix(prefixes: dict[str, str]) -> str:
    lines = [f"{pfx}: <{uri}>" for pfx, uri in sorted(prefixes.items())]
    return "\n".join(lines) + "\n"


# ── endpoint.yaml ───────────────────────────────────────────────


def _generate_endpoint(
    endpoint_url: str | None,
    endpoint_name: str | None,
    graph_uri: str | None,
) -> str:
    if not endpoint_url:
        return ""
    name = endpoint_name or "endpoint"
    lines = [f"{name}:", f"  - {endpoint_url}"]
    if graph_uri:
        lines.append("  - graph:")
        lines.append(f"    - {graph_uri}")
    return "\n".join(lines) + "\n"


# ── model.yaml ───────────────────────────────────────────────────


def _generate_model(
    graph_data: list[dict[str, Any]],
    prefixes: dict[str, str],
) -> str:
    class_uris = {item["@id"] for item in graph_data if "@id" in item}
    class_name_map = _build_unique_class_names(
        class_uris,
        prefixes,
    )

    classes: dict[str, list[dict[str, Any]]] = {}
    for item in graph_data:
        if "@id" not in item:
            continue
        subject = item["@id"]
        classes.setdefault(subject, [])

        unique_name = class_name_map.get(
            subject,
            _class_name(subject),
        )
        class_var = _variable_name(unique_name)

        for prop, value in item.items():
            if prop.startswith("@") or prop == "_counts":
                continue
            info = _analyze_property(
                prop,
                value,
                class_var,
                class_name_map,
            )
            if info:
                classes[subject].append(info)

    return _format_yaml(classes, class_name_map)


# ── helpers ──────────────────────────────────────────────────────


def _class_name(uri_or_curie: str) -> str:
    """CamelCase class name from URI/CURIE local part."""
    if ":" in uri_or_curie:
        local = uri_or_curie.split(":", 1)[1]
    elif "/" in uri_or_curie:
        local = uri_or_curie.split("/")[-1]
    elif "#" in uri_or_curie:
        local = uri_or_curie.split("#")[-1]
    else:
        local = uri_or_curie

    local = re.sub(r"[^a-zA-Z0-9]", "", local)
    if local and local[0].isdigit():
        local = "C" + local
    if local:
        local = local[0].upper() + local[1:]
    else:
        local = "Class"
    return local


def _variable_name(uri_or_curie: str) -> str:
    """snake_case variable name from URI/CURIE local part."""
    if ":" in uri_or_curie:
        local = uri_or_curie.split(":", 1)[1]
    elif "/" in uri_or_curie:
        local = uri_or_curie.split("/")[-1]
    elif "#" in uri_or_curie:
        local = uri_or_curie.split("#")[-1]
    else:
        local = uri_or_curie

    local = re.sub(r"[^a-zA-Z0-9_]", "_", local)
    local = re.sub(r"([a-z])([A-Z])", r"\1_\2", local)
    local = local.lower()
    local = re.sub(r"_+", "_", local).strip("_")
    return local


def _analyze_property(
    prop: str,
    value: Any,
    class_var: str,
    class_name_map: dict[str, str],
) -> dict[str, Any] | None:
    """Return structured info for one property entry."""
    is_ref = False
    target = None

    if isinstance(value, dict) and "@id" in value:
        is_ref = True
        target = value["@id"]
    elif isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, dict) and "@id" in first:
            is_ref = True
            target = first["@id"]

    prop_base = _variable_name(prop)
    prop_var = f"{class_var}_{prop_base}"

    if is_ref and target:
        target_name = class_name_map.get(
            target,
            _class_name(target),
        )
        return {
            "property": prop,
            "variable": prop_var,
            "range": target_name,
        }
    return {
        "property": prop,
        "variable": prop_var,
        "range": f'"{prop_var}_value"',
    }


def _build_unique_class_names(
    class_uris: set[str],
    prefixes: dict[str, str],
) -> dict[str, str]:
    """Map each class URI to a unique CamelCase name."""
    ns_to_prefix = {ns: pfx for pfx, ns in prefixes.items()}

    name_to_uris: dict[str, list[str]] = {}
    for uri in class_uris:
        base = _class_name(uri)
        name_to_uris.setdefault(base, []).append(uri)

    result: dict[str, str] = {}
    for base, uris in name_to_uris.items():
        if len(uris) == 1:
            result[uris[0]] = base
        else:
            for uri in uris:
                pfx = None
                if ":" in uri:
                    pfx = uri.split(":", 1)[0]
                else:
                    for ns_uri, p in ns_to_prefix.items():
                        if uri.startswith(ns_uri):
                            pfx = p
                            break
                if pfx:
                    pfx_clean = re.sub(
                        r"[^a-zA-Z0-9]",
                        "",
                        pfx,
                    )
                    pfx_cap = pfx_clean[0].upper() + pfx_clean[1:] if pfx_clean else ""
                    result[uri] = f"{pfx_cap}{base}"
                else:
                    result[uri] = f"{base}{str(abs(hash(uri)))[:6]}"
    return result


def _format_yaml(
    classes: dict[str, list[dict[str, Any]]],
    class_name_map: dict[str, str],
) -> str:
    """Format classes dict as RDF-config model.yaml."""
    lines: list[str] = []
    for class_uri in sorted(classes):
        props = classes[class_uri]
        name = class_name_map.get(
            class_uri,
            _class_name(class_uri),
        )
        lines.append(f"- {name} {class_uri}:")
        for p in props:
            lines.append(f"  - {p['property']}:")
            lines.append(
                f"    - {p['variable']}: {p['range']}",
            )
    return "\n".join(lines) + "\n"
