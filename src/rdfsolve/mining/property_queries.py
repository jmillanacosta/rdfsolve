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
# Object groups of one (class, property) whose distinct subjects are counted by a query each,
# from the largest. A property whose objects are typed by a large ontology has one group for
# each term in use; the others keep their edges and objects, and their subjects are not counted.
GROUP_QUERIES = 10
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
    alone: list[tuple[int, str, str]] = []
    for index, row in enumerate(groups.rows):
        whole = by_graph.get(row.get("_g", {}).get("value"))
        edges = _edges(row)
        if whole is None or edges is None:
            return None
        test = _group_test(builder, row, context_graphs)
        if edges == whole["cnt"]["value"]:
            subjects[index] = whole["subjects"]
        elif test is None:
            return None
        else:
            alone.append((index, edges, test))
    alone.sort(key=lambda item: -int(item[1]))
    for index, edges, test in alone[:GROUP_QUERIES]:
        graph = groups.rows[index].get("_g", {}).get("value")
        one = builders._build_class_property_total_query(
            [class_uri], prop, graphs, context_graphs, test
        )
        counted = select_outcome(one, f"{purpose}/group", helper, [class_uri], graphs)
        if counted.state != "complete":
            return None
        match = [r for r in counted.rows if r.get("_g", {}).get("value") == graph]
        if len(match) != 1 or match[0]["cnt"]["value"] != edges:
            return None
        subjects[index] = match[0]["subjects"]
    rows = [
        {**row, "subjects": subjects[i]} if i in subjects else row
        for i, row in enumerate(groups.rows)
    ]
    rest = len(alone) - GROUP_QUERIES
    if rest <= 0:
        return QueryOutcome(rows, "complete", [])
    message = (
        f"distinct subjects of {rest} of {len(alone)} object groups not counted: each needs its "
        f"own query, beyond the budget of {GROUP_QUERIES}"
    )
    logger.info("%s: %s", purpose, message)
    return QueryOutcome(
        rows, "partial", [QueryFailure("budget", message, purpose, [class_uri], graphs)]
    )


def _edges(row: dict[str, Any]) -> str | None:
    """Return the edge count of a row: ?cnt in the count builders, ?triples in property usage."""
    value: str | None = (row.get("cnt") or row.get("triples") or {}).get("value")
    return value


def _group_test(
    builder: Callable[..., str], row: dict[str, Any], context_graphs: list[str] | None
) -> str | None:
    """Return the test of the object that keeps the edges of one group, or None."""
    name = getattr(builder, "__name__", "")
    if name == "_build_batched_typed_count_query" and row.get("oc", {}).get("type") == "uri":
        return builders._type_pattern("?o", f"<{row['oc']['value']}>", context_graphs)
    if name == "_build_batched_literal_count_query" and row.get("dt", {}).get("type") == "uri":
        return f"FILTER(isLiteral(?o) && DATATYPE(?o) = <{row['dt']['value']}>)"
    return None


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
