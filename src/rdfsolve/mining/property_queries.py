"""Execute independently bounded class-property discovery and count queries."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from weakref import WeakKeyDictionary

from rdfsolve._outcomes import QueryFailure, QueryOutcome
from rdfsolve.mining import query_builders as builders

if TYPE_CHECKING:
    from rdfsolve.mining.query_fallbacks import CollectBindings
    from rdfsolve.sparql_helper import SparqlHelper

# Object term kinds that can produce rows for each property query.
OBJECT_KINDS = {
    "_build_batched_typed_object_query": {"iri", "blank"},
    "_build_batched_typed_count_query": {"iri", "blank"},
    "_build_batched_literal_query": {"literal"},
    "_build_batched_literal_count_query": {"literal"},
    "_build_batched_literal_objects_query": {"literal"},
    "_build_batched_untyped_uri_query": {"iri"},
    "_build_batched_untyped_count_query": {"iri"},
    "_build_batched_blank_node_query": {"blank"},
    "_build_batched_blank_node_count_query": {"blank"},
    "build_property_usage_query": {"literal", "iri", "blank"},
    "build_node_kind_query": {"literal", "iri", "blank"},
    "build_literal_profile_query": {"literal"},
    "build_value_count_histogram_query": {"literal", "iri", "blank"},
}
# Evidence builders measure data edges; rdf:type is class membership.
TYPE_EXCLUDED = frozenset(name for name in OBJECT_KINDS if name.startswith("build_"))
# Count builders that can drop distinct subjects when that aggregate exceeds the budget.
SUBJECT_COUNTS = frozenset(
    [
        *(
            f"_build_batched_{kind}_count_query"
            for kind in ("typed", "literal", "untyped", "blank_node")
        ),
        "build_property_usage_query",
    ]
)
PROPERTY_BUILDERS = frozenset(getattr(builders, n) for n in OBJECT_KINDS if hasattr(builders, n))
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
logger = logging.getLogger(__name__)
_KINDS: WeakKeyDictionary[SparqlHelper, dict[tuple[str, ...], set[str] | None]] = (
    WeakKeyDictionary()
)


def object_kinds(
    prop: str, graphs: list[str] | None, context_graphs: list[str] | None, helper: SparqlHelper
) -> set[str] | None:
    """Return the object kinds of a property in scope, or None when the probe failed."""
    from rdfsolve.mining.query_fallbacks import select_outcome

    cache = _KINDS.setdefault(helper, {})
    key = (prop, *(graphs or ()), "|", *(context_graphs or ()))
    if key not in cache:
        query = builders._build_object_kinds_query(prop, graphs, context_graphs)
        outcome = select_outcome(query, f"object-kinds/{prop}", helper, [], graphs)
        cache[key] = (
            {row["kind"]["value"] for row in outcome.rows} if outcome.state == "complete" else None
        )
    return cache[key]


def decomposes(helper: SparqlHelper, classes: list[str], builder: Callable[..., str]) -> bool:
    """Query one QLever class property by property, with constant bindings."""
    name = getattr(builder, "__name__", "")
    return len(classes) == 1 and helper.sparql_engine == "qlever" and name in OBJECT_KINDS


def _subjects_from_total(
    class_uri: str,
    prop: str,
    graphs: list[str] | None,
    context_graphs: list[str] | None,
    builder: Callable[..., str],
    purpose: str,
    helper: SparqlHelper,
) -> QueryOutcome | None:
    """Count edges per object first, and give each group the distinct subjects of the property.

    A group whose edges are all the edges of (class, property) in its graph has an object of the
    group on every edge, so its distinct subjects are those of (class, property): exact. The
    edges and distinct subjects of (class, property) are one merge join on QLever (Bgee: 13 s for
    813,735,712 edges), where the distinct subjects of each group reached the time limit. A group
    with part of the edges is counted alone (Bgee ExpressionCondition: 709,482,280 edges, 93 s).
    None when a query is refused or a group cannot be counted alone; the count of each group
    then runs.
    """
    from rdfsolve.mining.query_fallbacks import select_outcome

    lighter = builder(
        [class_uri],
        graphs,
        property_uri=prop,
        type_context_graph_uris=context_graphs,
        subjects=False,
    )
    groups = select_outcome(lighter, f"{purpose}/per-object", helper, [class_uri], graphs)
    if groups.state != "complete":
        return None
    if not groups.rows:
        return groups
    total = builders._build_class_property_total_query([class_uri], prop, graphs, context_graphs)
    totals = select_outcome(total, f"{purpose}/total", helper, [class_uri], graphs)
    if totals.state != "complete":
        return None
    by_graph = {row.get("_g", {}).get("value"): row for row in totals.rows}
    subjects: dict[int, dict[str, Any]] = {}
    alone: list[int] = []
    for index, row in enumerate(groups.rows):
        whole = by_graph.get(row.get("_g", {}).get("value"))
        if whole is None or _edges(row) is None:
            return None
        if _edges(row) == whole["cnt"]["value"]:
            subjects[index] = whole["subjects"]
        elif _group(builder, row) is None:
            return None
        else:
            alone.append(index)
    failures: list[QueryFailure] = []
    if alone:
        logger.info("%s: distinct subjects of %d object groups, in batches", purpose, len(alone))
        found = _count_groups(
            alone,
            groups.rows,
            class_uri,
            prop,
            graphs,
            context_graphs,
            builder,
            f"{purpose}/groups",
            helper,
            failures,
        )
        if found is None:
            return None
        subjects.update(found)
    rows = [
        {**row, "subjects": subjects[i]} if i in subjects else row
        for i, row in enumerate(groups.rows)
    ]
    return QueryOutcome(rows, "complete", gaps=failures)


def _group(builder: Callable[..., str], row: dict[str, Any]) -> tuple[str, str] | None:
    """Return the term that names the group of a row and the test that keeps its edges, with
    the group as ?_group; None when the builder's groups cannot be told apart.
    """
    name = getattr(builder, "__name__", "")
    if name == "_build_batched_typed_count_query" and row.get("oc", {}).get("type") == "uri":
        return row["oc"]["value"], "?o a ?_group ."
    if name == "_build_batched_literal_count_query" and row.get("dt", {}).get("type") == "uri":
        return row["dt"]["value"], "FILTER(isLiteral(?o) && DATATYPE(?o) = ?_group)"
    return None


def _count_groups(
    indexes: list[int],
    rows: list[dict[str, Any]],
    class_uri: str,
    prop: str,
    graphs: list[str] | None,
    context_graphs: list[str] | None,
    builder: Callable[..., str],
    purpose: str,
    helper: SparqlHelper,
    failures: list[QueryFailure],
) -> dict[int, dict[str, Any]] | None:
    """Count the distinct subjects of these groups in one query; a refused batch is split in
    two, and a group refused alone is recorded in *failures*. None on a result that does not
    match the edges counted before.
    """
    from rdfsolve.mining.query_fallbacks import select_outcome

    found = {i: g for i in indexes if (g := _group(builder, rows[i])) is not None}
    terms = sorted({term for term, _ in found.values()})
    test = found[indexes[0]][1]
    if test.startswith("?o a "):
        test = builders._type_pattern("?o", "?_group", context_graphs)
    values = "VALUES ?_group { " + " ".join(f"<{t}>" for t in terms) + " } "
    query = builders._build_class_property_total_query(
        [class_uri], prop, graphs, context_graphs, values + test, group="?_group"
    )
    outcome = select_outcome(query, purpose, helper, [class_uri], graphs)
    if outcome.state != "complete":
        if len(indexes) == 1:
            for failure in outcome.failures:
                failures.append(
                    QueryFailure(
                        failure.category,
                        f"distinct subjects of the {_edges(rows[indexes[0]])} edges to {terms[0]} "
                        f"not counted: {failure.message}",
                        purpose,
                        [class_uri],
                        graphs,
                    )
                )
            return {}
        half = len(indexes) // 2
        left = _count_groups(
            indexes[:half],
            rows,
            class_uri,
            prop,
            graphs,
            context_graphs,
            builder,
            purpose,
            helper,
            failures,
        )
        right = _count_groups(
            indexes[half:],
            rows,
            class_uri,
            prop,
            graphs,
            context_graphs,
            builder,
            purpose,
            helper,
            failures,
        )
        return None if left is None or right is None else {**left, **right}
    answered = {
        (r.get("_g", {}).get("value"), r.get("_group", {}).get("value")): r for r in outcome.rows
    }
    counted: dict[int, dict[str, Any]] = {}
    for i in indexes:
        match = answered.get((rows[i].get("_g", {}).get("value"), found[i][0]))
        if match is None or match["cnt"]["value"] != _edges(rows[i]):
            return None
        counted[i] = match["subjects"]
    return counted


def _edges(row: dict[str, Any]) -> str | None:
    """Return the edge count of a row: ?cnt in the count builders, ?triples in property usage."""
    value: str | None = (row.get("cnt") or row.get("triples") or {}).get("value")
    return value


def query_by_property(
    class_uri: str,
    graphs: list[str] | None,
    builder: Callable[..., str],
    purpose: str,
    helper: SparqlHelper,
    collect: CollectBindings,
    chunk_size: int,
    context_graphs: list[str] | None,
    properties: list[str] | None = None,
) -> QueryOutcome:
    """Query the given or enumerated properties whose objects can match the builder."""
    from rdfsolve.mining.query_fallbacks import enumerate_properties_for_class, select_outcome

    result = QueryOutcome()
    if properties is None:
        found = enumerate_properties_for_class(
            class_uri, graphs, purpose, helper, type_context_graph_uris=context_graphs
        )
        result = QueryOutcome(state=found.state, failures=found.failures)
        properties = [r["p"]["value"] for r in found.rows if r.get("p", {}).get("type") == "uri"]
    for prop in dict.fromkeys(properties):
        if prop in builders.MEMBERSHIP.get() and builder.__name__ in TYPE_EXCLUDED:
            continue
        kinds = object_kinds(prop, graphs, context_graphs, helper)
        if kinds is not None and not kinds & OBJECT_KINDS[builder.__name__]:
            continue
        if builder.__name__ in SUBJECT_COUNTS and helper.sparql_engine == "qlever":
            counted = _subjects_from_total(
                class_uri,
                prop,
                graphs,
                context_graphs,
                builder,
                f"{purpose}/property/{prop}",
                helper,
            )
            if counted is not None:
                result = result.merge(counted)
                continue
        query = builder(
            [class_uri], graphs, property_uri=prop, type_context_graph_uris=context_graphs
        )
        found = select_outcome(query, f"{purpose}/property/{prop}", helper, [class_uri], graphs)
        if builder.__name__ in SUBJECT_COUNTS and any(
            f.category == "timeout" for f in found.failures
        ):
            # Distinct subjects are the costly part; keep triple and object counts without them.
            lighter = builder(
                [class_uri],
                graphs,
                property_uri=prop,
                type_context_graph_uris=context_graphs,
                subjects=False,
            )
            retry = select_outcome(
                lighter, f"{purpose}/property/{prop}/without-subjects", helper, [class_uri], graphs
            )
            if retry.rows:
                found = QueryOutcome(retry.rows, "partial", found.failures + retry.failures)
        result = result.merge(found)
    return result
