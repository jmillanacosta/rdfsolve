"""SPARQL query composition from diagram paths — pure-library module.

Mirrors the frontend ``SPARQLComposer.generateFromPaths()`` logic:

* Each node position gets a fresh variable named after its URI local name.
* Duplicate variables get suffixed: ``protein``, ``protein_1``, ``protein_2``.
* Triple patterns are deduplicated.
* ``rdf:type`` assertions are optional.
* ``OPTIONAL`` label clauses use ``rdfs:label`` + ``dc:title``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from rdfsolve.utils import compact_uri, get_local_name


def compose_query_from_paths(
    paths: list[dict[str, Any]],
    prefixes: dict[str, str],
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate a SPARQL query from diagram paths.

    Parameters
    ----------
    paths:
        List of path dicts, each with an ``edges`` list.  Each edge has
        ``source``, ``target``, ``predicate``, and ``is_forward``.
    prefixes:
        Namespace prefix map (e.g. ``{"wp": "http://..."}``.
    options:
        Optional dict with ``include_types`` (bool), ``include_labels``
        (bool), ``limit`` (int), ``value_bindings`` (dict).

    Returns
    -------
    dict
        ``{"query": "SELECT ...", "variable_map": {...}, "jsonld": {...}}``
    """
    if options is None:
        options = {}

    include_types: bool = options.get("include_types", False)
    include_labels: bool = options.get("include_labels", True)
    limit: int = options.get("limit", 100)
    value_bindings: dict[str, list[str]] = options.get("value_bindings", {})

    # ── Variable generation ───────────────────────────────────────
    var_name_counter: dict[str, int] = {}
    select_vars: list[str] = []
    variable_map: dict[str, str] = {}

    # Track first variable assigned to each URI so that separate
    # single-edge paths sharing the same node URI reuse variables
    # (fan / star pattern).
    uri_first_var: dict[str, str] = {}

    def fresh_var(uri: str) -> str:
        ln = get_local_name(uri)
        if not ln:
            ln = "node"
        base_name = ln[0].lower() + ln[1:]
        count = var_name_counter.get(base_name, 0)
        suffix = "" if count == 0 else f"_{count}"
        var_name_counter[base_name] = count + 1
        v = f"{base_name}{suffix}"
        select_vars.append(v)
        variable_map[v] = uri
        if uri not in uri_first_var:
            uri_first_var[uri] = v
        return v

    def reuse_or_fresh(uri: str) -> str:
        """Reuse an existing variable for *uri* if one exists."""
        if uri in uri_first_var:
            return uri_first_var[uri]
        return fresh_var(uri)

    # ── Collect triple patterns ───────────────────────────────────
    all_patterns: list[str] = []
    seen: set[str] = set()
    type_assertions: list[str] = []

    for path in paths:
        edges = path.get("edges", [])
        if not edges:
            continue

        position_vars: list[str] = []

        for ei, edge in enumerate(edges):
            is_forward = edge.get("is_forward", True)
            real_subject = (
                edge["source"] if is_forward else edge["target"]
            )
            real_object = (
                edge["target"] if is_forward else edge["source"]
            )

            if ei == 0:
                # First node of the path: reuse if we've seen this
                # URI before (fan pattern); otherwise create fresh.
                position_vars.append(reuse_or_fresh(real_subject))

            # For intermediate / tail nodes inside a chain we always
            # create fresh variables so that reflexive edges
            # (e.g. Protein → Protein) get distinct vars.
            # HOWEVER, for the very last node of a single-edge path
            # we also try to reuse — this is the other half of the
            # fan pattern (both source AND target should match).
            if len(edges) == 1:
                # Single-edge path → reuse target too
                position_vars.append(reuse_or_fresh(real_object))
            else:
                position_vars.append(fresh_var(real_object))

            subj_var = position_vars[ei]
            obj_var = position_vars[ei + 1]

            pred = edge.get("predicate", "")
            pred_sparql = (
                compact_uri(pred, prefixes) if pred else "?p"
            )
            if pred_sparql == pred and pred.startswith("http"):
                pred_sparql = f"<{pred}>"

            pat = f"  ?{subj_var} {pred_sparql} ?{obj_var} ."
            if pat not in seen:
                seen.add(pat)
                all_patterns.append(pat)

        # rdf:type assertions
        if include_types:
            for pi, v in enumerate(position_vars):
                if pi == 0 and edges:
                    e = edges[0]
                    uri = (
                        e["source"]
                        if e.get("is_forward", True)
                        else e["target"]
                    )
                elif pi > 0 and pi - 1 < len(edges):
                    e = edges[pi - 1]
                    uri = (
                        e["target"]
                        if e.get("is_forward", True)
                        else e["source"]
                    )
                else:
                    continue
                tc = compact_uri(uri, prefixes)
                if tc == uri and uri.startswith("http"):
                    tc = f"<{uri}>"
                tp = f"  ?{v} a {tc} ."
                if tp not in seen:
                    seen.add(tp)
                    type_assertions.append(tp)

    # ── Build query string ────────────────────────────────────────
    parts: list[str] = []

    for pfx, ns in sorted(prefixes.items()):
        parts.append(f"PREFIX {pfx}: <{ns}>")
    parts.append("")

    label_vars = (
        [f"?{v}Label" for v in select_vars] if include_labels else []
    )
    all_select = [f"?{v}" for v in select_vars] + label_vars
    parts.append(f"SELECT DISTINCT {' '.join(all_select)}")

    parts.append("WHERE {")

    if value_bindings:
        for var_name, values in value_bindings.items():
            vals = " ".join(f"<{v}>" for v in values)
            parts.append(f"  VALUES ?{var_name} {{ {vals} }}")
        parts.append("")

    parts.extend(all_patterns)

    if type_assertions:
        parts.append("")
        parts.append("  # rdf:type assertions")
        parts.extend(type_assertions)

    if include_labels and select_vars:
        parts.append("")
        for v in select_vars:
            parts.append(
                f"  OPTIONAL {{ ?{v} rdfs:label ?{v}Label . }}"
            )
            parts.append(
                f"  OPTIONAL {{ ?{v} dc:title ?{v}Label . }}"
            )

    parts.append("}")

    if limit:
        parts.append(f"LIMIT {limit}")

    query = "\n".join(parts)

    # ── JSON-LD export ────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    jsonld: dict[str, Any] = {
        "@context": {
            **prefixes,
            "sh": "http://www.w3.org/ns/shacl#",
            "schema": "https://schema.org/",
            "sd": (
                "http://www.w3.org/ns/"
                "sparql-service-description#"
            ),
        },
        "@id": f"_:query_{int(now.timestamp() * 1000)}",
        "@type": [
            "sh:SPARQLExecutable",
            "sh:SPARQLSelectExecutable",
        ],
        "sh:select": query,
        "sh:prefixes": prefixes,
        "schema:dateCreated": now.isoformat(),
    }

    return {
        "query": query,
        "variable_map": variable_map,
        "jsonld": jsonld,
    }
