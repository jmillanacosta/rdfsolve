"""Execute independently bounded class-property discovery and count queries."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

from rdfsolve._outcomes import QueryOutcome
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
        if prop == RDF_TYPE and builder.__name__ in TYPE_EXCLUDED:
            continue
        kinds = object_kinds(prop, graphs, context_graphs, helper)
        if kinds is not None and not kinds & OBJECT_KINDS[builder.__name__]:
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
