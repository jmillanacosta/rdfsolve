"""SPARQL query composition from diagram paths - pure-library module.

Canonical implementation of query composition from diagram paths:

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

# ---------------------------------------------------------------------------
# Variable registry
# ---------------------------------------------------------------------------


class _VarRegistry:
    """Mutable variable-name state shared across all paths in one query."""

    def __init__(self) -> None:
        """Initialise empty counter, variable list, map, and URI cache."""
        self._counter: dict[str, int] = {}
        self.select_vars: list[str] = []
        self.variable_map: dict[str, str] = {}
        # First variable ever assigned to each URI (fan/star de-dup).
        self._uri_first: dict[str, str] = {}

    def fresh(self, uri: str) -> str:
        """Allocate a brand-new variable for *uri*."""
        ln = get_local_name(uri) or "node"
        base = ln[0].lower() + ln[1:]
        count = self._counter.get(base, 0)
        v = base if count == 0 else f"{base}_{count}"
        self._counter[base] = count + 1
        self.select_vars.append(v)
        self.variable_map[v] = uri
        self._uri_first.setdefault(uri, v)
        return v

    def reuse_or_fresh(self, uri: str) -> str:
        """Return the existing variable for *uri*, or allocate a new one."""
        existing = self._uri_first.get(uri)
        return existing if existing is not None else self.fresh(uri)


# ---------------------------------------------------------------------------
# Per-edge helpers
# ---------------------------------------------------------------------------


def _pred_sparql(edge: dict[str, Any], prefixes: dict[str, str]) -> str:
    """Return the compacted (or bracketed) predicate token for *edge*."""
    pred = edge.get("predicate", "")
    if not pred:
        return "?p"
    token = compact_uri(pred, prefixes)
    if token == pred and pred.startswith("http"):
        return f"<{pred}>"
    return token


def _edge_pattern(
    edge: dict[str, Any],
    src_var: str,
    tgt_var: str,
    prefixes: dict[str, str],
) -> str:
    """Build ``?s pred ?o .`` for one edge, honouring ``is_forward``."""
    is_forward = edge.get("is_forward", True)
    subj, obj = (src_var, tgt_var) if is_forward else (tgt_var, src_var)
    pred = _pred_sparql(edge, prefixes)
    return f"  ?{subj} {pred} ?{obj} ."


# ---------------------------------------------------------------------------
# Per-path helpers
# ---------------------------------------------------------------------------


def _path_position_vars(
    edges: list[dict[str, Any]],
    reg: _VarRegistry,
) -> list[str]:
    """Return the ordered list of SPARQL variable names for *edges*.

    * Position 0 is the source of the first edge.
    * Each subsequent position is the target of the previous edge.
    * Single-edge paths reuse the target variable (fan/star pattern).
    * Multi-edge paths always allocate a fresh variable for every target.
    """
    pos: list[str] = []
    is_chain = len(edges) > 1
    for ei, edge in enumerate(edges):
        if ei == 0:
            pos.append(reg.reuse_or_fresh(edge["source"]))
        pos.append(reg.fresh(edge["target"]) if is_chain else reg.reuse_or_fresh(edge["target"]))
    return pos


def _path_triple_patterns(
    edges: list[dict[str, Any]],
    position_vars: list[str],
    prefixes: dict[str, str],
    seen: set[str],
) -> list[str]:
    """Yield deduplicated triple patterns for one path."""
    patterns: list[str] = []
    for ei, edge in enumerate(edges):
        pat = _edge_pattern(edge, position_vars[ei], position_vars[ei + 1], prefixes)
        if pat not in seen:
            seen.add(pat)
            patterns.append(pat)
    return patterns


def _path_type_assertions(
    edges: list[dict[str, Any]],
    position_vars: list[str],
    prefixes: dict[str, str],
    seen: set[str],
) -> list[str]:
    """Return ``?v a <Type> .`` lines for every position in one path."""
    assertions: list[str] = []
    for pi, v in enumerate(position_vars):
        if pi == 0 and edges:
            uri = edges[0]["source"]
        elif 0 < pi <= len(edges):
            uri = edges[pi - 1]["target"]
        else:
            continue
        tc = compact_uri(uri, prefixes)
        if tc == uri and uri.startswith("http"):
            tc = f"<{uri}>"
        tp = f"  ?{v} a {tc} ."
        if tp not in seen:
            seen.add(tp)
            assertions.append(tp)
    return assertions


# ---------------------------------------------------------------------------
# Full-query helpers
# ---------------------------------------------------------------------------


def _collect_patterns(
    paths: list[dict[str, Any]],
    prefixes: dict[str, str],
    include_types: bool,
    reg: _VarRegistry,
) -> tuple[list[str], list[str]]:
    """Walk all *paths* and return ``(triple_patterns, type_assertions)``."""
    all_patterns: list[str] = []
    type_assertions: list[str] = []
    seen: set[str] = set()

    for path in paths:
        edges: list[dict[str, Any]] = path.get("edges", [])
        if not edges:
            continue
        pos = _path_position_vars(edges, reg)
        all_patterns.extend(_path_triple_patterns(edges, pos, prefixes, seen))
        if include_types:
            type_assertions.extend(_path_type_assertions(edges, pos, prefixes, seen))

    return all_patterns, type_assertions


def _build_query_string(
    prefixes: dict[str, str],
    select_vars: list[str],
    all_patterns: list[str],
    type_assertions: list[str],
    value_bindings: dict[str, list[str]],
    include_labels: bool,
    limit: int,
) -> str:
    """Assemble the full SPARQL SELECT query string."""
    parts: list[str] = []

    for pfx, ns in sorted(prefixes.items()):
        parts.append(f"PREFIX {pfx}: <{ns}>")
    parts.append("")

    label_vars = [f"?{v}Label" for v in select_vars] if include_labels else []
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
            parts.append(f"  OPTIONAL {{ ?{v} rdfs:label ?{v}Label . }}")
            parts.append(f"  OPTIONAL {{ ?{v} dc:title ?{v}Label . }}")

    parts.append("}")

    if limit:
        parts.append(f"LIMIT {limit}")

    return "\n".join(parts)


def _build_jsonld(prefixes: dict[str, str], query: str) -> dict[str, Any]:
    """Wrap *query* in a minimal SHACL / Schema.org JSON-LD envelope."""
    now = datetime.now(timezone.utc)
    return {
        "@context": {
            **prefixes,
            "sh": "http://www.w3.org/ns/shacl#",
            "schema": "https://schema.org/",
            "sd": "http://www.w3.org/ns/sparql-service-description#",
        },
        "@id": f"_:query_{int(now.timestamp() * 1000)}",
        "@type": ["sh:SPARQLExecutable", "sh:SPARQLSelectExecutable"],
        "sh:select": query,
        "sh:prefixes": prefixes,
        "schema:dateCreated": now.isoformat(),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


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
    opts = options or {}
    include_types: bool = opts.get("include_types", False)
    include_labels: bool = opts.get("include_labels", True)
    limit: int = opts.get("limit", 100)
    value_bindings: dict[str, list[str]] = opts.get("value_bindings", {})

    reg = _VarRegistry()
    all_patterns, type_assertions = _collect_patterns(paths, prefixes, include_types, reg)
    query = _build_query_string(
        prefixes,
        reg.select_vars,
        all_patterns,
        type_assertions,
        value_bindings,
        include_labels,
        limit,
    )
    return {
        "query": query,
        "variable_map": reg.variable_map,
        "jsonld": _build_jsonld(prefixes, query),
    }
