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
}
PROPERTY_BUILDERS = frozenset(getattr(builders, name) for name in OBJECT_KINDS)
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


def query_by_property(
    class_uri: str,
    graphs: list[str] | None,
    builder: Callable[..., str],
    purpose: str,
    helper: SparqlHelper,
    collect: CollectBindings,
    chunk_size: int,
    context_graphs: list[str] | None,
) -> QueryOutcome:
    """Enumerate all properties, then query those whose objects can match the builder."""
    from rdfsolve.mining.query_fallbacks import enumerate_properties_for_class, select_outcome

    properties = enumerate_properties_for_class(
        class_uri,
        graphs,
        purpose,
        helper,
        collect,
        chunk_size=chunk_size,
        type_context_graph_uris=context_graphs,
    )
    result = QueryOutcome(state=properties.state, failures=properties.failures)
    for prop in dict.fromkeys(
        row["p"]["value"] for row in properties.rows if row.get("p", {}).get("type") == "uri"
    ):
        kinds = object_kinds(prop, graphs, context_graphs, helper)
        if kinds is not None and not kinds & OBJECT_KINDS[builder.__name__]:
            continue
        query = builder(
            [class_uri], graphs, property_uri=prop, type_context_graph_uris=context_graphs
        )
        result = result.merge(
            select_outcome(query, f"{purpose}/property/{prop}", helper, [class_uri], graphs)
        )
    return result
