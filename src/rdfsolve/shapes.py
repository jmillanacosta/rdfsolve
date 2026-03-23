"""Shapes Creator - subset schemas and convert to SHACL.

This module provides two core operations:

1. **Subset**: given a full JSON-LD schema (as produced by rdfsolve's
   miner or parser) and a list of edges (paths) to *keep*, produce a
   smaller JSON-LD containing only those triples.

2. **SHACL conversion**: take a (subset) JSON-LD schema and convert it
   to SHACL shapes via the existing VoidParser -> LinkML -> ShaclGenerator
   pipeline.

The shapes feature allows users to define *intended* schemas from
mined schemas, which can then be used for RDF data validation.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "jsonld_to_shacl",
    "subset_jsonld",
]


# ── Types ────────────────────────────────────────────────────────────

# An "edge spec" describes one edge (triple pattern) to keep.
# It mirrors what the frontend knows about a diagram edge:
#   subject   - class CURIE or URI  (e.g. "core:Protein")
#   predicate - property CURIE or URI
#   object    - class CURIE/URI or "Literal" or "Resource"
EdgeSpec = dict[str, str]  # keys: subject, predicate, object


# ── Subset ───────────────────────────────────────────────────────────


def subset_jsonld(  # noqa C901
    schema_jsonld: dict[str, Any],
    keep_edges: list[EdgeSpec],
) -> dict[str, Any]:
    """Return a copy of *schema_jsonld* containing only *keep_edges*.

    Parameters
    ----------
    schema_jsonld:
        Full JSON-LD schema with ``@context``, ``@graph``, and
        optionally ``@about``.
    keep_edges:
        List of edge specs.  Each must have ``subject``,
        ``predicate``, and ``object`` keys (CURIEs or full URIs).

    Returns
    -------
    dict
        A new JSON-LD document that keeps only the nodes and
        properties referenced by *keep_edges*.  The ``@context``
        is preserved; ``@about`` is carried over as-is.
    """
    if not keep_edges:
        return {
            "@context": schema_jsonld.get("@context", {}),
            "@graph": [],
            "@about": schema_jsonld.get("@about", {}),
        }

    context = schema_jsonld.get("@context", {})
    graph: list[dict[str, Any]] = schema_jsonld.get("@graph", [])

    # Build a lookup: subject_id -> node dict
    node_map: dict[str, dict[str, Any]] = {}
    for node in graph:
        nid = node.get("@id", "")
        if nid:
            node_map[nid] = node

    # Expand a CURIE using @context so we can compare both forms.
    def _expand(curie: str) -> str:
        if ":" not in curie or curie.startswith("http"):
            return curie
        prefix, local = curie.split(":", 1)
        ns = context.get(prefix, "")
        return f"{ns}{local}" if ns else curie

    # Build a reverse lookup: full URI -> CURIE.
    # This lets us also compact a full URI back to its CURIE form.
    def _compact(uri: str) -> str:
        if not uri.startswith("http"):
            return uri
        for prefix, ns in context.items():
            if isinstance(ns, str) and uri.startswith(ns):
                return f"{prefix}:{uri[len(ns) :]}"
        return uri

    # Build the set of (subject, predicate) to keep.
    # We store EVERY possible combination of expanded/compacted forms
    # so that matching works regardless of what format the graph or
    # the caller uses.
    keep_set: set[tuple[str, str]] = set()
    keep_subjects: set[str] = set()
    for edge in keep_edges:
        subj = edge.get("subject", "")
        pred = edge.get("predicate", "")
        if subj and pred:
            forms_s = {subj, _expand(subj), _compact(subj)}
            forms_p = {pred, _expand(pred), _compact(pred)}
            for s in forms_s:
                for p in forms_p:
                    keep_set.add((s, p))
                keep_subjects.add(s)

    # Filter: for each subject node, keep only the specified properties.
    new_graph: list[dict[str, Any]] = []
    logger.debug(
        "subset_jsonld: %d keep_edges -> %d (s,p) pairs, %d subjects, %d graph nodes",
        len(keep_edges),
        len(keep_set),
        len(keep_subjects),
        len(graph),
    )
    for node in graph:
        nid = node.get("@id", "")
        if nid not in keep_subjects:
            continue
        new_node: dict[str, Any] = {"@id": nid}
        for key, value in node.items():
            if key.startswith("@") or key == "_counts":
                if key.startswith("@"):
                    new_node[key] = value
                continue
            # Check if (subject, predicate) should be kept.
            if (nid, key) in keep_set:
                # If we also want to filter by object, we can do so here.
                # For now, we keep the entire predicate entry.
                new_node[key] = copy.deepcopy(value)
        # Only include if the node has at least one non-@ property
        has_data = any(not k.startswith("@") for k in new_node if k != "@id")
        if has_data:
            new_graph.append(new_node)

    result: dict[str, Any] = {
        "@context": copy.deepcopy(context),
        "@graph": new_graph,
    }
    if "@about" in schema_jsonld:
        result["@about"] = copy.deepcopy(schema_jsonld["@about"])

    return result


# ── SHACL conversion ─────────────────────────────────────────────────


def jsonld_to_shacl(
    schema_jsonld: dict[str, Any],
    *,
    schema_name: str | None = None,
    closed: bool = True,
    suffix: str | None = None,
) -> str:
    """Convert a JSON-LD schema to SHACL Turtle via LinkML.

    Delegates to :func:`rdfsolve.schema_models.shacl.to_shacl`, which
    runs the full LinkML -> ShaclGenerator pipeline on the given
    JSON-LD dict- no VoidParser instance required.

    Parameters
    ----------
    schema_jsonld:
        JSON-LD dict (the same format produced by ``subset_jsonld``
        or the miner).
    schema_name:
        Optional name for the generated LinkML/SHACL schema.
    closed:
        Whether to produce closed SHACL shapes (``sh:closed true``).
    suffix:
        Suffix appended to every shape name
        (e.g. ``"Shape"`` -> ``PersonShape``).

    Returns
    -------
    str
        SHACL shapes serialised as Turtle.
    """
    from rdfsolve.schema_models.shacl import to_shacl

    return to_shacl(
        schema_jsonld,
        schema_name=schema_name or "shapes",
        closed=closed,
        suffix=suffix,
    )
