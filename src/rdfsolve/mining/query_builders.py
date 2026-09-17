"""SPARQL query builders for RDF schema mining."""

from __future__ import annotations

from typing import Any

from rdfsolve.sparql_helper import SparqlHelper

__all__ = [
    "_DECOMP_CHUNK",
    "_build_batched_blank_node_query",
    "_build_batched_literal_count_query",
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
    "_build_literal_for_class_property_query",
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
) -> tuple[str, str, str]:
    """Return (dataset, open, close) that attribute an edge to its graph.

    The dataset clause merges the selected graphs so type lookups still match
    when a shape spans graphs, while ``GRAPH ?_g`` records the graph holding
    the subject-predicate-object edge. Without *graph_uris* all three parts are
    empty and the query reads the endpoint default graph.
    """
    if not graph_uris:
        return "", "", ""
    merged = " ".join(f"FROM <{u}>" for u in graph_uris)
    named = " ".join(f"FROM NAMED <{u}>" for u in graph_uris)
    return f"{merged} {named}", "GRAPH ?_g {", "}"


def _values_block(class_uris: list[str]) -> str:
    """Build a ``VALUES ?class { <u1> <u2> … }`` clause."""
    entries = " ".join(f"<{u}>" for u in class_uris)
    return f"VALUES ?class {{ {entries} }}"


def _build_typed_object_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 1: typed-object patterns (``?o a ?oc``)."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p ?oc
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    ?o a ?oc .
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_typed_object_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 1 - typed-object patterns, no LIMIT/OFFSET placeholders.

    Intended for one-shot execution against engines (e.g. QLever)
    that can return an unbounded result set in a single response.
    """
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p ?oc
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    ?o a ?oc .
  {g_close}
}}"""


def _build_literal_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 2: literal patterns with datatype."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p ?dt
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_literal_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 2 - literal patterns, no LIMIT/OFFSET placeholders."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p ?dt
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}"""


def _build_untyped_uri_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 3: URI objects that lack an explicit ``rdf:type``."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_untyped_uri_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 3 - untyped-URI patterns, no LIMIT/OFFSET placeholders."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}"""


def _build_blank_node_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 4: blank-node objects with optional properties."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p ?bnPred
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isBlank(?o))
    OPTIONAL {{ ?o ?bnPred ?bnObj }}
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_blank_node_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 4 - blank-node patterns, no LIMIT/OFFSET placeholders."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p ?bnPred
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isBlank(?o))
    OPTIONAL {{ ?o ?bnPred ?bnObj }}
  {g_close}
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
    g_open, g_close = _graph_clause(graph_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?p
WHERE {{
  {g_open}
    ?s a <{class_uri}> .
    ?s ?p ?o .
  {g_close}
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
    g_open, g_close = _graph_clause(graph_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?oc
WHERE {{
  {g_open}
    ?s a <{class_uri}> .
    ?s <{prop_uri}> ?o .
    ?o a ?oc .
  {g_close}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_literal_for_class_property_query(
    class_uri: str,
    prop_uri: str,
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Enumerate datatypes for a single *(class, property)* pair with literal objects.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` / ``{limit}``
        placeholders.
    drop_distinct:
        When ``True`` omits ``DISTINCT`` from paginated queries; the
        caller deduplicates in Python.  Only active when
        ``paginated=True``.
    """
    g_open, g_close = _graph_clause(graph_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?dt
WHERE {{
  {g_open}
    ?s a <{class_uri}> .
    ?s <{prop_uri}> ?o .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_typed_object_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Typed-object patterns for a batch of classes."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    values = _values_block(class_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?class ?p ?oc
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  ?o a ?oc .
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_literal_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
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
) -> str:
    """Untyped-URI patterns for a batch of classes."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
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
  FILTER NOT EXISTS {{ ?o a ?any }}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_blank_node_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
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
) -> str:
    """Typed-object COUNT grouped by ``(class, p, oc)`` and edge graph."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    values = _values_block(class_uris)
    graph_var = " ?_g" if g_open else ""
    q = f"""\
SELECT ?class ?p ?oc{graph_var} (COUNT(*) AS ?cnt)
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  ?o a ?oc .
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
) -> str:
    """Literal COUNT grouped by ``(class, p, dt)`` and edge graph."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    values = _values_block(class_uris)
    graph_var = " ?_g" if g_open else ""
    q = f"""\
SELECT ?class ?p ?dt{graph_var} (COUNT(*) AS ?cnt)
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
) -> str:
    """Untyped-URI COUNT grouped by ``(class, p)`` and edge graph."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    values = _values_block(class_uris)
    graph_var = " ?_g" if g_open else ""
    q = f"""\
SELECT ?class ?p{graph_var} (COUNT(*) AS ?cnt)
{dataset}
WHERE {{
  {values}
  ?s a ?class .
  {g_open} ?s ?p ?o . {g_close}
  FILTER(isURI(?o))
  FILTER NOT EXISTS {{ ?o a ?any }}
}}
GROUP BY ?class ?p{graph_var}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q
