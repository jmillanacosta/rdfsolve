"""Execute independently bounded class-property discovery and count queries."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from rdfsolve._outcomes import QueryOutcome
from rdfsolve.mining import query_builders as builders

if TYPE_CHECKING:
    from rdfsolve.mining.query_fallbacks import CollectBindings
    from rdfsolve.sparql_helper import SparqlHelper

PROPERTY_BUILDERS = frozenset(
    getattr(builders, name)
    for name in (
        "_build_batched_typed_object_query",
        "_build_batched_typed_count_query",
        "_build_batched_literal_query",
        "_build_batched_literal_count_query",
        "_build_batched_literal_objects_query",
        "_build_batched_untyped_uri_query",
        "_build_batched_untyped_count_query",
        "_build_batched_blank_node_query",
        "_build_batched_blank_node_count_query",
    )
)


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
    """Enumerate all properties, then retain each query's rows and failures."""
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
        query = builder(
            [class_uri], graphs, property_uri=prop, type_context_graph_uris=context_graphs
        )
        result = result.merge(
            select_outcome(query, f"{purpose}/property/{prop}", helper, [class_uri], graphs)
        )
    return result
