"""SPARQL query builders for RDF schema mining."""

from __future__ import annotations

from typing import Any

from rdfsolve.sparql_helper import SparqlHelper

__all__ = [
    "_DECOMP_CHUNK",
    "_build_batched_blank_node_query",
    "_build_batched_literal_count_query",
    "_build_batched_literal_objects_query",
    "_build_batched_literal_query",
    "_build_batched_typed_count_query",
    "_build_batched_typed_object_query",
    "_build_batched_untyped_count_query",
    "_build_batched_untyped_uri_query",
    "_build_blank_node_query",
    "_build_blank_node_query_plain",
    "_build_cardinality_query",
    "_build_class_discovery_query",
    "_build_class_discovery_query_plain",
    "_build_declared_classes_query",
    "_build_example_query",
    "_build_label_query",
    "_build_literal_query",
    "_build_literal_query_plain",
    "_build_properties_for_class_query",
    "_build_typed_object_for_class_property_query",
    "_build_typed_object_query",
    "_build_typed_object_query_plain",
    "_build_untyped_uri_query",
    "_build_untyped_uri_query_plain",
    "_graph_clause",
    "_graph_scope",
    "_values_block",
    "pick_description",
]

_DECOMP_CHUNK = 1_000


def _graph_clause(
    graph_uris: list[str] | None,
) -> tuple[str, str]:
    """Return (open, close) strings for an optional GRAPH clause.

    If *graph_uris* is ``None`` -> empty strings (default graph).
    If a single URI -> ``GRAPH <uri> {`` / ``}``.
    If multiple -> VALUES-based pattern.
    """
    if not graph_uris:
        return "", ""
    if len(graph_uris) == 1:
        return f"GRAPH <{graph_uris[0]}> {{", "}"
    # Multiple graphs - use VALUES
    values = " ".join(f"(<{u}>)" for u in graph_uris)
    open_ = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"
    return open_, "}"


def _graph_scope(
    graph_uris: list[str] | None,
    context_graph_uris: list[str] | None = None,
) -> tuple[str, str, str]:
    """Merge data graphs and attribute edges; expose context only as named graphs."""
    if not graph_uris:
        return "", "", ""
    graphs = list(dict.fromkeys(graph_uris))
    merged = " ".join(f"FROM <{u}>" for u in graphs)
    named = " ".join(
        f"FROM NAMED <{u}>" for u in dict.fromkeys(graphs + (context_graph_uris or []))
    )
    values = " ".join(f"<{u}>" for u in graphs)
    return f"{merged} {named}", f"VALUES ?_g {{ {values} }} GRAPH ?_g {{", "}"


def _context_pattern(pattern: str, graph_uris: list[str] | None) -> str:
    """Match a pattern in the default dataset or selected companion graphs."""
    if not graph_uris:
        return pattern
    graphs = " ".join(f"<{iri}>" for iri in dict.fromkeys(graph_uris))
    return (
        f"{{ {pattern} }} UNION {{ VALUES ?_contextGraph {{ {graphs} }} "
        f"GRAPH ?_contextGraph {{ {pattern} }} }}"
    )


def _type_pattern(node: str, cls: str, context_graph_uris: list[str] | None = None) -> str:
    """Match each node/type pair once across data and companion graphs."""
    triple = f"{node} a {cls} ."
    if not context_graph_uris:
        return triple
    variables = " ".join(term for term in (node, cls) if term.startswith("?"))
    return f"{{ SELECT DISTINCT {variables} WHERE {{ {_context_pattern(triple, context_graph_uris)} }} }}"


def _values_block(class_uris: list[str]) -> str:
    """Build a ``VALUES ?class { <u1> <u2> … }`` clause."""
    entries = " ".join(f"<{u}>" for u in class_uris)
    return f"VALUES ?class {{ {entries} }}"


def _build_typed_object_query(
    graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Return the paged typed object query."""
    return SparqlHelper.prepare_paginated_query(
        _build_typed_object_query_plain(graph_uris, type_context_graph_uris)
    )


def _build_typed_object_query_plain(
    graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Query 1 - typed-object patterns, no LIMIT/OFFSET placeholders.

    Intended for one-shot execution against engines (e.g. QLever)
    that can return an unbounded result set in a single response.
    """
    dataset, g_open, g_close = _graph_scope(graph_uris, type_context_graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p ?oc
{dataset}
WHERE {{
  ?s a ?sc .
  {g_open} ?s ?p ?o . {g_close}
    {_type_pattern("?o", "?oc", type_context_graph_uris)}
}}"""


def _build_literal_query(
    graph_uris: list[str] | None,
) -> str:
    """Return the paged literal query."""
    return SparqlHelper.prepare_paginated_query(_build_literal_query_plain(graph_uris))


def _build_literal_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 2 - literal patterns, no LIMIT/OFFSET placeholders."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p ?dt
{dataset}
WHERE {{
  ?s a ?sc .
  {g_open} ?s ?p ?o . {g_close}
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
}}"""


def _build_untyped_uri_query(
    graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Return the paged untyped uri query."""
    return SparqlHelper.prepare_paginated_query(
        _build_untyped_uri_query_plain(graph_uris, type_context_graph_uris)
    )


def _build_untyped_uri_query_plain(
    graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Query 3 - untyped-URI patterns, no LIMIT/OFFSET placeholders."""
    dataset, g_open, g_close = _graph_scope(graph_uris, type_context_graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p
{dataset}
WHERE {{
  ?s a ?sc .
  {g_open} ?s ?p ?o . {g_close}
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ {_type_pattern("?o", "?any", type_context_graph_uris)} }}
}}"""


def _build_blank_node_query(
    graph_uris: list[str] | None,
) -> str:
    """Return the paged blank node query."""
    return SparqlHelper.prepare_paginated_query(_build_blank_node_query_plain(graph_uris))


def _build_blank_node_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 4 - blank-node patterns, no LIMIT/OFFSET placeholders."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p ?bnPred
{dataset}
WHERE {{
  ?s a ?sc .
  {g_open} ?s ?p ?o . {g_close}
    FILTER(isBlank(?o))
    OPTIONAL {{ ?o ?bnPred ?bnObj }}
}}"""


def _build_label_query(
    uris: list[str],
    graph_uris: list[str] | None,
) -> str:
    """Fetch labels and descriptions for a set of URIs.

    Returns bindings with ``?uri``, ``?rdfsLabel``, ``?dcTitle``,
    ``?iaoLabel``, ``?skosPrefLabel``, ``?skosAltLabel``,
    ``?rdfsComment``, ``?dcDescription``, ``?skosDefinition``,
    ``?schemaDescription``, ``?iaoDefinition``, ``?dcDesc``.
    Priority is resolved in Python via :func:`pick_label` and :func:`pick_description`.
    """
    values = " ".join(f"(<{u}>)" for u in uris)
    g_open, g_close = _graph_clause(graph_uris)
    columns = {
        "rdfsLabel": ["http://www.w3.org/2000/01/rdf-schema#label"],
        "dcTitle": ["http://purl.org/dc/elements/1.1/title", "http://purl.org/dc/terms/title"],
        "iaoLabel": ["http://purl.obolibrary.org/obo/IAO_0000118"],
        "skosPrefLabel": ["http://www.w3.org/2004/02/skos/core#prefLabel"],
        "skosAltLabel": ["http://www.w3.org/2004/02/skos/core#altLabel"],
        "rdfsComment": ["http://www.w3.org/2000/01/rdf-schema#comment"],
        "dcDescription": ["http://purl.org/dc/terms/description"],
        "skosDefinition": ["http://www.w3.org/2004/02/skos/core#definition"],
        "schemaDescription": ["http://schema.org/description", "https://schema.org/description"],
        "iaoDefinition": ["http://purl.obolibrary.org/obo/IAO_0000115"],
        "dcDesc": ["http://purl.org/dc/elements/1.1/description"],
    }
    branches = [
        f"{{ ?uri <{predicate}> ?{column} . FILTER(isLiteral(?{column})) }}"
        for column, predicates in columns.items()
        for predicate in predicates
    ]
    q = (
        "SELECT ?uri "
        + " ".join(f"?{column}" for column in columns)
        + f" WHERE {{ VALUES (?uri) {{ {values} }} {g_open} "
        + " UNION ".join(branches)
        + f" {g_close} }}"
    )
    return q


def pick_description(row: dict[str, Any]) -> str | None:
    """Pick description from query results in priority order.

    Tries description predicates in order:
    rdfs:comment, dcterms:description, skos:definition,
    schema:description, obo:IAO_0000115, dc:description.
    """
    for key in (
        "rdfsComment",
        "dcDescription",
        "skosDefinition",
        "schemaDescription",
        "iaoDefinition",
        "dcDesc",
    ):
        val = row.get(key, {}).get("value")
        if val and isinstance(val, str):
            return val
    return None


def _build_cardinality_query(
    subject_class: str,
    graph_uris: list[str] | None,
) -> str:
    """Build query to get min and max count per property for a class."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT ?p (MIN(?cnt) AS ?minCnt) (MAX(?cnt) AS ?maxCnt)
WHERE {{
  {{
    SELECT ?s ?p (COUNT(?o) AS ?cnt)
    WHERE {{
      {g_open}
        ?s a <{subject_class}> .
        ?s ?p ?o .
      {g_close}
    }}
    GROUP BY ?s ?p
  }}
}}
GROUP BY ?p"""


def _build_example_query(
    subject_class: str,
    property_uri: str,
    graph_uris: list[str] | None,
    limit: int = 5,
) -> str:
    """Build query to get example instances and values."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?s ?o
WHERE {{
  {g_open}
    ?s a <{subject_class}> .
    ?s <{property_uri}> ?o .
  {g_close}
}}
LIMIT {limit}"""


def _build_class_discovery_query(
    graph_uris: list[str] | None,
) -> str:
    """Discover all distinct rdf:type classes (paginated template)."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?class
WHERE {{
  {g_open}
    ?s a ?class .
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_class_weight_query(
    graph_uris: list[str] | None,
) -> str:
    """Count typed instances per rdf:type class (paginated template)."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?class (COUNT(?s) AS ?n)
WHERE {{
  {g_open}
    ?s a ?class .
  {g_close}
}}
GROUP BY ?class
ORDER BY ?class"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_class_discovery_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Discover all distinct rdf:type classes (single shot)."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?class
WHERE {{
  {g_open}
    ?s a ?class .
  {g_close}
}}"""


def _build_declared_classes_query(
    graph_uris: list[str] | None,
) -> str:
    """Find classes formally declared as owl:Class or rdfs:Class."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?class
WHERE {{
  {g_open}
    {{ ?class a <http://www.w3.org/2002/07/owl#Class> . }}
    UNION
    {{ ?class a <http://www.w3.org/2000/01/rdf-schema#Class> . }}
  {g_close}
}}"""


def _build_properties_for_class_query(
    class_uri: str,
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Enumerate all distinct properties used by instances of *class_uri*.

    This is a cheap single-hop query (one join) used as the first step of
    the property-first decomposition fallback for expensive classes.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` / ``{limit}``
        placeholders.
    drop_distinct:
        When ``True`` omits ``DISTINCT`` from paginated queries; the
        caller deduplicates in Python.  Only active when
        ``paginated=True``.  See
        :func:`_build_batched_typed_object_query` for caveats.
    """
    dataset, g_open, g_close = _graph_scope(graph_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?p
{dataset}
WHERE {{
  ?s a <{class_uri}> .
  {g_open} ?s ?p ?o . {g_close}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_typed_object_for_class_property_query(
    class_uri: str,
    prop_uri: str,
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Enumerate typed-object classes for a single *(class, property)* pair.

    Two-hop query but scoped to one property, so Virtuoso can use
    the property index and stays well under the cost limit.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` / ``{limit}``
        placeholders.
    drop_distinct:
        When ``True`` omits ``DISTINCT`` from paginated queries; the
        caller deduplicates in Python.  Only active when
        ``paginated=True``.  See
        :func:`_build_batched_typed_object_query` for caveats.
    """
    dataset, g_open, g_close = _graph_scope(graph_uris, type_context_graph_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?oc
{dataset}
WHERE {{
  ?s a <{class_uri}> .
  {g_open} ?s <{prop_uri}> ?o . {g_close}
    {_type_pattern("?o", "?oc", type_context_graph_uris)}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_typed_object_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Typed-object patterns for a batch of classes."""
    dataset, g_open, g_close = _graph_scope(graph_uris, type_context_graph_uris)
    values = _values_block(class_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?class ?p ?oc
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  {_type_pattern("?o", "?oc", type_context_graph_uris)}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_literal_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Literal patterns for a batch of classes."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    values = _values_block(class_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?class ?p (DATATYPE(?o) AS ?dt)
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  FILTER(isLiteral(?o))
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_untyped_uri_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Untyped-URI patterns for a batch of classes."""
    dataset, g_open, g_close = _graph_scope(graph_uris, type_context_graph_uris)
    values = _values_block(class_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?class ?p
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  FILTER(isURI(?o))
  FILTER NOT EXISTS {{ {_type_pattern("?o", "?any", type_context_graph_uris)} }}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_blank_node_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Blank-node patterns for a batch of classes."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    values = _values_block(class_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?class ?p ?bnPred
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  FILTER(isBlank(?o))
  OPTIONAL {{ ?o ?bnPred ?bnObj }}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_typed_count_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Typed-object COUNT grouped by ``(class, p, oc)`` and edge graph."""
    dataset, g_open, g_close = _graph_scope(graph_uris, type_context_graph_uris)
    values = _values_block(class_uris)
    graph_var = " ?_g" if g_open else ""
    q = f"""\
SELECT ?class ?p ?oc{graph_var} (COUNT(*) AS ?cnt)\n       (COUNT(DISTINCT ?s) AS ?subjects) (COUNT(DISTINCT ?o) AS ?objects)
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  {_type_pattern("?o", "?oc", type_context_graph_uris)}
}}
GROUP BY ?class ?p ?oc{graph_var}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_literal_count_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Literal triple and distinct-subject counts grouped by ``(class, p, dt)`` and edge graph."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    values = _values_block(class_uris)
    graph_var = " ?_g" if g_open else ""
    q = f"""\
SELECT ?class ?p ?dt{graph_var} (SUM(?k) AS ?cnt) (COUNT(*) AS ?subjects)
{dataset}
WHERE {{
  {{
    SELECT ?class ?p ?dt{graph_var} ?s (COUNT(*) AS ?k)
    WHERE {{
      {values}
      ?s a ?class .
      {g_open} ?s ?p ?o . {g_close}
      FILTER(isLiteral(?o))
      BIND(DATATYPE(?o) AS ?dt)
    }}
    GROUP BY ?class ?p ?dt{graph_var} ?s
  }}
}}
GROUP BY ?class ?p ?dt{graph_var}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_literal_objects_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Distinct literal objects grouped by ``(class, p, dt)`` and edge graph."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    values = _values_block(class_uris)
    graph_var = " ?_g" if g_open else ""
    q = f"""\
SELECT ?class ?p ?dt{graph_var} (COUNT(DISTINCT ?o) AS ?objects)
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  FILTER(isLiteral(?o))
  BIND(DATATYPE(?o) AS ?dt)
}}
GROUP BY ?class ?p ?dt{graph_var}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_untyped_count_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Untyped-URI COUNT grouped by ``(class, p)`` and edge graph."""
    dataset, g_open, g_close = _graph_scope(graph_uris, type_context_graph_uris)
    values = _values_block(class_uris)
    graph_var = " ?_g" if g_open else ""
    q = f"""\
SELECT ?class ?p{graph_var} (COUNT(*) AS ?cnt)\n       (COUNT(DISTINCT ?s) AS ?subjects) (COUNT(DISTINCT ?o) AS ?objects)
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  FILTER(isURI(?o))
  FILTER NOT EXISTS {{ {_type_pattern("?o", "?any", type_context_graph_uris)} }}
}}
GROUP BY ?class ?p{graph_var}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_blank_node_count_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Count blank-node edges per class, property and graph."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    graph_var = " ?_g" if g_open else ""
    query = f"""SELECT ?class ?p{graph_var} (COUNT(*) AS ?cnt)
       (COUNT(DISTINCT ?s) AS ?subjects) (COUNT(DISTINCT ?o) AS ?objects)
{dataset}
WHERE {{
  {_values_block(class_uris)}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  FILTER(isBlank(?o))
}}
GROUP BY ?class ?p{graph_var}"""
    return SparqlHelper.prepare_paginated_query(query) if paginated else query
